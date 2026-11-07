from pathlib import Path

import pytest
from sqlalchemy import text

from finance_dwh.analytics import load_analytics_queries, run_analytics_query
from finance_dwh.config import PROJECT_ROOT
from finance_dwh.generate_data import generate_datasets
from finance_dwh.pipeline import run_pipeline
from finance_dwh.schema import create_db_engine
from finance_dwh.validation import ValidationResult


@pytest.fixture()
def sqlite_engine(tmp_path: Path):
    engine = create_db_engine(f"sqlite:///{tmp_path / 'warehouse.db'}")
    yield engine
    engine.dispose()


@pytest.fixture()
def raw_data(tmp_path: Path) -> Path:
    data_dir = tmp_path / "raw"
    generate_datasets(data_dir)
    return data_dir


def _scalar(engine, sql: str) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text(sql)).scalar() or 0)


def test_pipeline_is_repeatable_and_audited(sqlite_engine, raw_data):
    first = run_pipeline(engine=sqlite_engine, data_dir=raw_data)
    assert first.status == "SUCCESS", first.error
    assert first.extracted_rows == 1713
    assert first.transformed_rows == 1659
    assert first.rejected_rows == 54
    assert first.loaded_rows == 3704
    assert first.validation_failures == 0
    assert first.validation_warnings == 3
    assert first.run_id == 1

    assert _scalar(sqlite_engine, "SELECT COUNT(*) FROM dim_customer") == 220
    assert _scalar(sqlite_engine, "SELECT COUNT(*) FROM dim_product") == 8
    assert _scalar(sqlite_engine, "SELECT COUNT(*) FROM dim_loan") == 280
    assert _scalar(sqlite_engine, "SELECT COUNT(*) FROM fact_loan_originations") == 280
    assert _scalar(sqlite_engine, "SELECT COUNT(*) FROM fact_transactions") == 1151
    assert _scalar(sqlite_engine, "SELECT COUNT(*) FROM etl_rejected_rows") == 54
    assert _scalar(sqlite_engine, "SELECT COUNT(*) FROM etl_validation_results") == 24

    duplicate_ids = _scalar(
        sqlite_engine,
        """
        SELECT COUNT(*) FROM (
            SELECT customer_id FROM dim_customer
            GROUP BY customer_id HAVING COUNT(*) > 1
        ) duplicates
        """,
    )
    assert duplicate_ids == 0

    second = run_pipeline(engine=sqlite_engine, data_dir=raw_data)
    assert second.status == "SUCCESS", second.error
    assert second.run_id == 2
    assert second.loaded_rows == first.loaded_rows
    assert _scalar(sqlite_engine, "SELECT COUNT(*) FROM fact_transactions") == 1151
    assert _scalar(sqlite_engine, "SELECT COUNT(*) FROM etl_pipeline_runs") == 2
    assert _scalar(sqlite_engine, "SELECT COUNT(*) FROM etl_rejected_rows") == 108

    with sqlite_engine.connect() as conn:
        statuses = [
            row[0]
            for row in conn.execute(
                text("SELECT status FROM etl_pipeline_runs ORDER BY run_id")
            )
        ]
    assert statuses == ["SUCCESS", "SUCCESS"]


def test_blocking_validation_rolls_back_warehouse(sqlite_engine, raw_data, monkeypatch):
    first = run_pipeline(engine=sqlite_engine, data_dir=raw_data)
    assert first.status == "SUCCESS"

    def fail_validation(conn, frames, *, source_file_count):
        return [
            ValidationResult(
                check_name="forced_failure",
                category="Test",
                status="FAIL",
                expected_value="0",
                actual_value="1",
                details="Simulated blocking failure.",
            )
        ]

    monkeypatch.setattr(
        "finance_dwh.warehouse.run_warehouse_validations",
        fail_validation,
    )
    failed = run_pipeline(engine=sqlite_engine, data_dir=raw_data)
    assert failed.status == "FAILED"
    assert failed.validation_failures == 1
    assert "forced_failure" in (failed.error or "")

    # The previous committed snapshot must still be intact.
    assert _scalar(sqlite_engine, "SELECT COUNT(*) FROM fact_loan_originations") == 280
    assert _scalar(sqlite_engine, "SELECT COUNT(*) FROM fact_transactions") == 1151

    with sqlite_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT status, loaded_rows FROM etl_pipeline_runs "
                "WHERE run_id = :run_id"
            ),
            {"run_id": failed.run_id},
        ).one()
    assert row[0] == "FAILED"
    assert row[1] == 0


def test_analytics_library_runs(sqlite_engine, raw_data):
    result = run_pipeline(engine=sqlite_engine, data_dir=raw_data)
    assert result.status == "SUCCESS"

    queries = load_analytics_queries(PROJECT_ROOT / "sql" / "analytics_queries.sql")
    assert {query.name for query in queries} == {
        "portfolio_by_product",
        "monthly_originations",
        "transaction_activity",
        "top_customers",
        "loan_status_summary",
        "segment_performance",
    }
    for query in queries:
        frame = run_analytics_query(sqlite_engine, query)
        assert not frame.empty, query.name
