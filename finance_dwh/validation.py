"""Post-load data validation for the star schema."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from finance_dwh.transforms import WarehouseFrames


@dataclass(frozen=True)
class ValidationResult:
    check_name: str
    category: str
    status: str
    expected_value: str
    actual_value: str
    details: str

    @property
    def passed(self) -> bool:
        return self.status == "PASS"


class WarehouseValidationError(Exception):
    """Raised when one or more critical validation checks fail."""

    def __init__(self, checks: list[ValidationResult]):
        self.checks = checks
        failed = [check.check_name for check in checks if check.status == "FAIL"]
        super().__init__(f"Warehouse validation failed: {', '.join(failed)}")


def _scalar(conn: Connection, sql: str) -> Any:
    return conn.execute(text(sql)).scalar()


def _compare(
    checks: list[ValidationResult],
    *,
    name: str,
    category: str,
    expected: Any,
    actual: Any,
    details: str,
    warn: bool = False,
) -> None:
    if actual == expected:
        status = "PASS"
    else:
        status = "WARN" if warn else "FAIL"
    checks.append(
        ValidationResult(
            check_name=name,
            category=category,
            status=status,
            expected_value=str(expected),
            actual_value=str(actual),
            details=details,
        )
    )


def _warn_if_any(
    checks: list[ValidationResult],
    *,
    name: str,
    category: str,
    issue_count: int,
    details: str,
) -> None:
    checks.append(
        ValidationResult(
            check_name=name,
            category=category,
            status="WARN" if issue_count else "PASS",
            expected_value="0",
            actual_value=str(issue_count),
            details=details,
        )
    )


def run_warehouse_validations(
    conn: Connection,
    frames: WarehouseFrames,
    *,
    source_file_count: int,
) -> list[ValidationResult]:
    """Run referential, uniqueness, reconciliation and quality checks."""
    checks: list[ValidationResult] = []

    _compare(
        checks,
        name="source_file_inventory",
        category="Completeness",
        expected=4,
        actual=source_file_count,
        details="All four operational CSV feeds must be present.",
    )

    expected_counts = {
        "dim_date": len(frames.dim_date),
        "dim_customer": len(frames.dim_customer),
        "dim_product": len(frames.dim_product),
        "dim_loan": len(frames.dim_loan),
        "fact_loan_originations": len(frames.fact_loan_originations),
        "fact_transactions": len(frames.fact_transactions),
    }
    for table_name, expected in expected_counts.items():
        actual = _scalar(conn, f"SELECT COUNT(*) FROM {table_name}")
        _compare(
            checks,
            name=f"row_count_{table_name}",
            category="Row Count Reconciliation",
            expected=expected,
            actual=int(actual or 0),
            details=f"Loaded {table_name} count must equal the transformed frame count.",
        )

    uniqueness_checks = (
        ("dim_customer_business_key_unique", "dim_customer", "customer_id"),
        ("dim_product_business_key_unique", "dim_product", "product_code"),
        ("dim_loan_business_key_unique", "dim_loan", "loan_id"),
        (
            "fact_transaction_business_key_unique",
            "fact_transactions",
            "transaction_id",
        ),
        ("dim_date_key_unique", "dim_date", "full_date"),
    )
    for name, table_name, key_column in uniqueness_checks:
        duplicate_groups = _scalar(
            conn,
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {key_column}
                FROM {table_name}
                GROUP BY {key_column}
                HAVING COUNT(*) > 1
            ) AS duplicate_keys
            """,
        )
        _compare(
            checks,
            name=name,
            category="Uniqueness",
            expected=0,
            actual=int(duplicate_groups or 0),
            details=f"No duplicate {table_name}.{key_column} values are allowed.",
        )

    referential_checks = (
        (
            "fact_loan_customer_referential_integrity",
            """
            SELECT COUNT(*) FROM fact_loan_originations f
            LEFT JOIN dim_customer d ON d.customer_key = f.customer_key
            WHERE d.customer_key IS NULL
            """,
            "Every loan fact customer_key must exist in dim_customer.",
        ),
        (
            "fact_loan_product_referential_integrity",
            """
            SELECT COUNT(*) FROM fact_loan_originations f
            LEFT JOIN dim_product d ON d.product_key = f.product_key
            WHERE d.product_key IS NULL
            """,
            "Every loan fact product_key must exist in dim_product.",
        ),
        (
            "fact_loan_date_referential_integrity",
            """
            SELECT COUNT(*) FROM fact_loan_originations f
            LEFT JOIN dim_date d ON d.date_key = f.origination_date_key
            WHERE d.date_key IS NULL
            """,
            "Every loan origination date must exist in dim_date.",
        ),
        (
            "fact_transaction_loan_referential_integrity",
            """
            SELECT COUNT(*) FROM fact_transactions f
            LEFT JOIN dim_loan d ON d.loan_key = f.loan_key
            WHERE d.loan_key IS NULL
            """,
            "Every transaction loan_key must exist in dim_loan.",
        ),
        (
            "fact_transaction_customer_referential_integrity",
            """
            SELECT COUNT(*) FROM fact_transactions f
            LEFT JOIN dim_customer d ON d.customer_key = f.customer_key
            WHERE d.customer_key IS NULL
            """,
            "Every transaction customer_key must exist in dim_customer.",
        ),
        (
            "fact_transaction_date_referential_integrity",
            """
            SELECT COUNT(*) FROM fact_transactions f
            LEFT JOIN dim_date d ON d.date_key = f.transaction_date_key
            WHERE d.date_key IS NULL
            """,
            "Every transaction date must exist in dim_date.",
        ),
    )
    for name, sql, details in referential_checks:
        orphan_count = _scalar(conn, sql)
        _compare(
            checks,
            name=name,
            category="Referential Integrity",
            expected=0,
            actual=int(orphan_count or 0),
            details=details,
        )

    expected_loan_total = round(
        float(frames.fact_loan_originations["principal_amount"].sum()), 2
    )
    actual_loan_total = round(
        float(
            _scalar(
                conn,
                "SELECT COALESCE(SUM(principal_amount), 0) FROM fact_loan_originations",
            )
        ),
        2,
    )
    loan_reconciled = abs(expected_loan_total - actual_loan_total) <= 0.01
    checks.append(
        ValidationResult(
            check_name="loan_principal_reconciliation",
            category="Reconciliation",
            status="PASS" if loan_reconciled else "FAIL",
            expected_value=f"{expected_loan_total:,.2f}",
            actual_value=f"{actual_loan_total:,.2f}",
            details="Warehouse principal total must reconcile to the transformed source total.",
        )
    )

    expected_txn_total = round(float(frames.fact_transactions["amount"].sum()), 2)
    actual_txn_total = round(
        float(_scalar(conn, "SELECT COALESCE(SUM(amount), 0) FROM fact_transactions")),
        2,
    )
    txn_reconciled = abs(expected_txn_total - actual_txn_total) <= 0.01
    checks.append(
        ValidationResult(
            check_name="transaction_amount_reconciliation",
            category="Reconciliation",
            status="PASS" if txn_reconciled else "FAIL",
            expected_value=f"{expected_txn_total:,.2f}",
            actual_value=f"{actual_txn_total:,.2f}",
            details="Warehouse transaction total must reconcile to the transformed source total.",
        )
    )

    imputed_emails = int(frames.dim_customer["email_imputed"].sum())
    _warn_if_any(
        checks,
        name="customer_email_quality",
        category="Data Quality",
        issue_count=imputed_emails,
        details="Missing or malformed emails are replaced with deterministic placeholders.",
    )

    unknown_loan_statuses = int((frames.dim_loan["loan_status"] == "unknown").sum())
    _warn_if_any(
        checks,
        name="loan_status_coverage",
        category="Data Quality",
        issue_count=unknown_loan_statuses,
        details="Unrecognized or missing loan statuses are retained as 'unknown'.",
    )

    unknown_txn_statuses = int((frames.fact_transactions["status"] == "unknown").sum())
    _warn_if_any(
        checks,
        name="transaction_status_coverage",
        category="Data Quality",
        issue_count=unknown_txn_statuses,
        details="Unrecognized or missing transaction statuses are retained as 'unknown'.",
    )

    missing_dob = int(frames.dim_customer["date_of_birth"].isna().sum())
    _warn_if_any(
        checks,
        name="customer_date_of_birth_quality",
        category="Data Quality",
        issue_count=missing_dob,
        details="Invalid or missing dates of birth are stored as NULL; customer rows remain valid.",
    )

    return checks
