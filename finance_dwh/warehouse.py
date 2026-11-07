"""Warehouse loading and ETL audit persistence."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import delete, insert, text, update
from sqlalchemy.engine import Engine

from finance_dwh.schema import (
    ALL_WAREHOUSE_TABLES,
    dim_customer,
    dim_date,
    dim_loan,
    dim_product,
    etl_pipeline_runs,
    etl_rejected_rows,
    etl_validation_results,
    fact_loan_originations,
    fact_transactions,
)
from finance_dwh.transforms import RejectedRow, WarehouseFrames
from finance_dwh.validation import (
    ValidationResult,
    WarehouseValidationError,
    run_warehouse_validations,
)

_LOAD_ORDER = (
    (dim_date, "dim_date"),
    (dim_customer, "dim_customer"),
    (dim_product, "dim_product"),
    (dim_loan, "dim_loan"),
    (fact_loan_originations, "fact_loan_originations"),
    (fact_transactions, "fact_transactions"),
)


def load_warehouse(
    engine: Engine,
    frames: WarehouseFrames,
    *,
    source_file_count: int,
) -> list[ValidationResult]:
    """Replace the star schema atomically and run post-load checks.

    A critical validation failure raises ``WarehouseValidationError`` inside the
    database transaction, rolling back the replacement and leaving the previous
    warehouse snapshot intact.
    """
    checks: list[ValidationResult] = []
    with engine.begin() as conn:
        # Children first so this is safe with SQLite/PostgreSQL FK enforcement.
        for table, _name in reversed(_LOAD_ORDER):
            conn.execute(delete(table))

        for table, name in _LOAD_ORDER:
            frame = frames.as_dict()[name]
            if frame.empty:
                continue
            frame.to_sql(
                name,
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500,
            )

        checks = run_warehouse_validations(
            conn,
            frames,
            source_file_count=source_file_count,
        )
        if any(check.status == "FAIL" for check in checks):
            raise WarehouseValidationError(checks)
    return checks


def start_pipeline_run(engine: Engine, started_at: datetime) -> int:
    with engine.begin() as conn:
        result = conn.execute(
            insert(etl_pipeline_runs).values(
                started_at=started_at,
                status="RUNNING",
                extracted_rows=0,
                transformed_rows=0,
                rejected_rows=0,
                loaded_rows=0,
                validation_failures=0,
                validation_warnings=0,
                run_details={},
            )
        )
        return int(result.inserted_primary_key[0])


def update_pipeline_run(engine: Engine, run_id: int, **values: Any) -> None:
    if not values:
        return
    with engine.begin() as conn:
        conn.execute(
            update(etl_pipeline_runs)
            .where(etl_pipeline_runs.c.run_id == run_id)
            .values(**values)
        )


def persist_rejections(
    engine: Engine,
    run_id: int,
    rejections: list[RejectedRow],
    *,
    created_at: datetime,
) -> None:
    if not rejections:
        return
    payload = [
        {
            "run_id": run_id,
            "source": rejection.source,
            "source_row": rejection.source_row,
            "reason_code": rejection.reason_code,
            "reason": rejection.reason,
            "raw_record": rejection.raw_record,
            "created_at": created_at,
        }
        for rejection in rejections
    ]
    with engine.begin() as conn:
        for start in range(0, len(payload), 500):
            conn.execute(insert(etl_rejected_rows), payload[start : start + 500])


def persist_validations(
    engine: Engine,
    run_id: int,
    checks: list[ValidationResult],
    *,
    checked_at: datetime,
) -> None:
    if not checks:
        return
    payload = [
        {
            "run_id": run_id,
            "check_name": check.check_name,
            "category": check.category,
            "status": check.status,
            "expected_value": check.expected_value,
            "actual_value": check.actual_value,
            "details": check.details,
            "checked_at": checked_at,
        }
        for check in checks
    ]
    with engine.begin() as conn:
        for start in range(0, len(payload), 500):
            conn.execute(insert(etl_validation_results), payload[start : start + 500])


def table_row_counts(engine: Engine) -> dict[str, int]:
    counts: dict[str, int] = {}
    with engine.connect() as conn:
        for table_name in ALL_WAREHOUSE_TABLES:
            counts[table_name] = int(
                conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar() or 0
            )
    return counts
