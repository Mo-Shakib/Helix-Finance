"""End-to-end ETL orchestration with audit logging and validation gates."""

from __future__ import annotations

import argparse
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from sqlalchemy.engine import Engine

from finance_dwh.config import get_settings
from finance_dwh.schema import create_db_engine, initialize_database
from finance_dwh.transforms import (
    SOURCE_FILES,
    RejectedRow,
    TransformResult,
    build_warehouse_frames,
    read_source,
    transform_customers,
    transform_loans,
    transform_products,
    transform_transactions,
)
from finance_dwh.validation import ValidationResult, WarehouseValidationError
from finance_dwh.warehouse import (
    load_warehouse,
    persist_rejections,
    persist_validations,
    start_pipeline_run,
    update_pipeline_run,
)

ProgressCallback = Callable[[str, int], None]


@dataclass
class PipelineResult:
    run_id: int | None
    status: str
    extracted_rows: int = 0
    transformed_rows: int = 0
    rejected_rows: int = 0
    loaded_rows: int = 0
    validation_failures: int = 0
    validation_warnings: int = 0
    duration_seconds: float = 0.0
    error: str | None = None
    details: dict = field(default_factory=dict)
    checks: list[ValidationResult] = field(default_factory=list)
    rejections: list[RejectedRow] = field(default_factory=list)


def _notify(callback: ProgressCallback | None, message: str, percent: int) -> None:
    if callback is not None:
        callback(message, percent)


def _source_counts(results: dict[str, TransformResult]) -> dict[str, dict[str, int]]:
    return {
        name: {
            "extracted": result.extracted,
            "accepted": result.accepted,
            "rejected": result.rejected_count,
        }
        for name, result in results.items()
    }


def _failure_check(error: str) -> ValidationResult:
    return ValidationResult(
        check_name="pipeline_execution",
        category="Execution",
        status="FAIL",
        expected_value="SUCCESS",
        actual_value="FAILED",
        details=error,
    )


def run_pipeline(
    *,
    engine: Engine | None = None,
    data_dir: Path | None = None,
    database_url: str | None = None,
    progress: ProgressCallback | None = None,
) -> PipelineResult:
    """Run extract -> transform -> validate -> load -> audit.

    Pipeline-level data problems are returned as a ``FAILED`` result and
    recorded in the audit tables. A database connection failure can still
    raise because there is nowhere to write the audit trail.
    """
    settings = get_settings()
    data_path = (data_dir or settings.data_dir).resolve()
    engine = engine or create_db_engine(database_url or settings.database_url)

    started_at = datetime.now(timezone.utc)
    started_perf = time.perf_counter()
    initialize_database(engine)
    run_id = start_pipeline_run(engine, started_at)

    rejections: list[RejectedRow] = []
    checks: list[ValidationResult] = []
    source_counts: dict[str, dict[str, int]] = {}
    extracted_rows = transformed_rows = 0
    loaded_rows = validation_failures = validation_warnings = 0
    stage_timings: dict[str, float] = {}
    details: dict = {}
    result: PipelineResult

    try:
        _notify(progress, "Extracting operational source feeds…", 5)
        stage_start = time.perf_counter()
        missing = [
            filename
            for filename in SOURCE_FILES.values()
            if not (data_path / filename).is_file()
        ]
        if missing:
            raise FileNotFoundError(
                "Missing source file(s): " + ", ".join(sorted(missing))
            )

        raw = {
            name: read_source(data_path / filename)
            for name, filename in SOURCE_FILES.items()
        }
        stage_timings["extract_seconds"] = round(time.perf_counter() - stage_start, 3)

        _notify(progress, "Cleaning and transforming dimensions…", 25)
        stage_start = time.perf_counter()
        customer_result = transform_customers(raw["crm_customers"])
        product_result = transform_products(raw["product_catalog"])
        stage_timings["dimension_transform_seconds"] = round(
            time.perf_counter() - stage_start, 3
        )

        _notify(progress, "Cleaning loans and transactions…", 45)
        stage_start = time.perf_counter()
        loan_result = transform_loans(
            raw["loan_core"],
            customer_result.frame,
            product_result.frame,
        )
        transaction_result = transform_transactions(
            raw["payment_ledger"],
            loan_result.frame,
        )
        stage_timings["fact_transform_seconds"] = round(
            time.perf_counter() - stage_start, 3
        )

        results: dict[str, TransformResult] = {
            "crm_customers": customer_result,
            "product_catalog": product_result,
            "loan_core": loan_result,
            "payment_ledger": transaction_result,
        }
        source_counts = _source_counts(results)
        extracted_rows = sum(result.extracted for result in results.values())
        transformed_rows = sum(result.accepted for result in results.values())
        rejections = [
            rejection for result in results.values() for rejection in result.rejected
        ]

        _notify(progress, "Persisting rejected rows to the audit log…", 60)
        persist_rejections(
            engine,
            run_id,
            rejections,
            created_at=datetime.now(timezone.utc),
        )

        frames = build_warehouse_frames(
            customers=customer_result.frame,
            products=product_result.frame,
            loans=loan_result.frame,
            transactions=transaction_result.frame,
        )
        warehouse_counts = {
            name: len(frame) for name, frame in frames.as_dict().items()
        }
        details = {
            "source_counts": source_counts,
            "warehouse_counts": warehouse_counts,
            "rejected_by_reason": dict(
                Counter(rejection.reason_code for rejection in rejections)
            ),
            "rejected_by_source": dict(
                Counter(rejection.source for rejection in rejections)
            ),
            "stage_timings": stage_timings,
            "data_dir": str(data_path),
        }

        update_pipeline_run(
            engine,
            run_id,
            extracted_rows=extracted_rows,
            transformed_rows=transformed_rows,
            rejected_rows=len(rejections),
            run_details=details,
        )

        _notify(progress, "Loading star schema atomically…", 70)
        stage_start = time.perf_counter()
        checks = load_warehouse(
            engine,
            frames,
            source_file_count=len(raw),
        )
        stage_timings["load_and_validate_seconds"] = round(
            time.perf_counter() - stage_start, 3
        )
        loaded_rows = sum(warehouse_counts.values())
        validation_failures = sum(check.status == "FAIL" for check in checks)
        validation_warnings = sum(check.status == "WARN" for check in checks)
        details["stage_timings"] = stage_timings

        _notify(progress, "Recording validation results…", 90)
        persist_validations(
            engine,
            run_id,
            checks,
            checked_at=datetime.now(timezone.utc),
        )

        duration = round(time.perf_counter() - started_perf, 3)
        update_pipeline_run(
            engine,
            run_id,
            status="SUCCESS",
            completed_at=datetime.now(timezone.utc),
            loaded_rows=loaded_rows,
            validation_failures=validation_failures,
            validation_warnings=validation_warnings,
            duration_seconds=duration,
            run_details=details,
        )
        result = PipelineResult(
            run_id=run_id,
            status="SUCCESS",
            extracted_rows=extracted_rows,
            transformed_rows=transformed_rows,
            rejected_rows=len(rejections),
            loaded_rows=loaded_rows,
            validation_failures=validation_failures,
            validation_warnings=validation_warnings,
            duration_seconds=duration,
            details=details,
            checks=checks,
            rejections=rejections,
        )
        _notify(progress, "Pipeline completed successfully.", 100)
        return result

    except WarehouseValidationError as exc:
        checks = exc.checks
        validation_failures = sum(check.status == "FAIL" for check in checks)
        validation_warnings = sum(check.status == "WARN" for check in checks)
        duration = round(time.perf_counter() - started_perf, 3)
        error = str(exc)
        details["stage_timings"] = stage_timings
        try:
            persist_validations(
                engine,
                run_id,
                checks,
                checked_at=datetime.now(timezone.utc),
            )
            update_pipeline_run(
                engine,
                run_id,
                status="FAILED",
                completed_at=datetime.now(timezone.utc),
                extracted_rows=extracted_rows,
                transformed_rows=transformed_rows,
                rejected_rows=len(rejections),
                loaded_rows=0,
                validation_failures=validation_failures,
                validation_warnings=validation_warnings,
                duration_seconds=duration,
                error_message=error,
                run_details=details,
            )
        except Exception:  # pragma: no cover - audit DB failure is terminal
            pass
        result = PipelineResult(
            run_id=run_id,
            status="FAILED",
            extracted_rows=extracted_rows,
            transformed_rows=transformed_rows,
            rejected_rows=len(rejections),
            validation_failures=validation_failures,
            validation_warnings=validation_warnings,
            duration_seconds=duration,
            error=error,
            details=details,
            checks=checks,
            rejections=rejections,
        )
        _notify(progress, f"Pipeline failed validation: {error}", 100)
        return result

    except Exception as exc:
        duration = round(time.perf_counter() - started_perf, 3)
        error = f"{type(exc).__name__}: {exc}"
        if not checks:
            checks = [_failure_check(error)]
        validation_failures = max(1, sum(check.status == "FAIL" for check in checks))
        validation_warnings = sum(check.status == "WARN" for check in checks)
        details["stage_timings"] = stage_timings
        try:
            persist_validations(
                engine,
                run_id,
                checks,
                checked_at=datetime.now(timezone.utc),
            )
            update_pipeline_run(
                engine,
                run_id,
                status="FAILED",
                completed_at=datetime.now(timezone.utc),
                extracted_rows=extracted_rows,
                transformed_rows=transformed_rows,
                rejected_rows=len(rejections),
                loaded_rows=0,
                validation_failures=validation_failures,
                validation_warnings=validation_warnings,
                duration_seconds=duration,
                error_message=error,
                run_details=details,
            )
        except Exception:  # pragma: no cover - audit DB failure is terminal
            pass
        result = PipelineResult(
            run_id=run_id,
            status="FAILED",
            extracted_rows=extracted_rows,
            transformed_rows=transformed_rows,
            rejected_rows=len(rejections),
            validation_failures=validation_failures,
            validation_warnings=validation_warnings,
            duration_seconds=duration,
            error=error,
            details=details,
            checks=checks,
            rejections=rejections,
        )
        _notify(progress, f"Pipeline failed: {error}", 100)
        return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the finance warehouse ETL pipeline"
    )
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--database-url", default=None)
    args = parser.parse_args()

    def progress(message: str, percent: int) -> None:
        print(f"[{percent:>3}%] {message}")

    result = run_pipeline(
        data_dir=args.data_dir,
        database_url=args.database_url,
        progress=progress,
    )
    print("\nPipeline result")
    print("---------------")
    print(f"Run ID:              {result.run_id}")
    print(f"Status:              {result.status}")
    print(f"Rows extracted:      {result.extracted_rows:,}")
    print(f"Rows transformed:    {result.transformed_rows:,}")
    print(f"Rows rejected:       {result.rejected_rows:,}")
    print(f"Rows loaded:         {result.loaded_rows:,}")
    print(f"Validation failures: {result.validation_failures}")
    print(f"Validation warnings: {result.validation_warnings}")
    print(f"Duration:            {result.duration_seconds:.2f}s")
    if result.error:
        print(f"Error:               {result.error}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
