"""SQLAlchemy definitions for the warehouse star schema and ETL audit tables."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    event,
)
from sqlalchemy.engine import Engine

metadata = MetaData()

# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------

dim_date = Table(
    "dim_date",
    metadata,
    Column("date_key", Integer, primary_key=True, comment="YYYYMMDD integer key"),
    Column("full_date", Date, nullable=False, unique=True),
    Column("year", Integer, nullable=False),
    Column("quarter", Integer, nullable=False),
    Column("month", Integer, nullable=False),
    Column("month_name", String(20), nullable=False),
    Column("day_of_month", Integer, nullable=False),
    Column("day_of_week", Integer, nullable=False, comment="ISO weekday, Monday=1"),
    Column("day_name", String(20), nullable=False),
    Column("is_weekend", Boolean, nullable=False),
    Column("year_month", String(7), nullable=False, comment="YYYY-MM reporting period"),
    Index("ix_dim_date_year_month", "year_month"),
)

dim_customer = Table(
    "dim_customer",
    metadata,
    Column("customer_key", Integer, primary_key=True),
    Column("customer_id", String(32), nullable=False, unique=True),
    Column("full_name", String(160), nullable=False),
    Column("email", String(255), nullable=False),
    Column("email_imputed", Boolean, nullable=False, default=False),
    Column("country", String(80), nullable=False),
    Column("segment", String(40), nullable=False),
    Column("date_of_birth", Date, nullable=True),
    Column("signup_date", Date, nullable=False),
)

dim_product = Table(
    "dim_product",
    metadata,
    Column("product_key", Integer, primary_key=True),
    Column("product_code", String(32), nullable=False, unique=True),
    Column("product_name", String(120), nullable=False),
    Column("product_family", String(80), nullable=False),
    Column("base_rate", Numeric(6, 3), nullable=False),
    Column("is_secured", Boolean, nullable=False),
)

dim_loan = Table(
    "dim_loan",
    metadata,
    Column("loan_key", Integer, primary_key=True),
    Column("loan_id", String(32), nullable=False, unique=True),
    Column(
        "customer_key", Integer, ForeignKey("dim_customer.customer_key"), nullable=False
    ),
    Column(
        "product_key", Integer, ForeignKey("dim_product.product_key"), nullable=False
    ),
    Column("origination_date", Date, nullable=False),
    Column("term_months", Integer, nullable=False),
    Column("loan_status", String(20), nullable=False),
    Index("ix_dim_loan_customer_key", "customer_key"),
    Index("ix_dim_loan_product_key", "product_key"),
)

# ---------------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------------

fact_loan_originations = Table(
    "fact_loan_originations",
    metadata,
    Column(
        "loan_key",
        Integer,
        ForeignKey("dim_loan.loan_key"),
        primary_key=True,
        comment="One row per loan; grain is loan origination",
    ),
    Column(
        "customer_key", Integer, ForeignKey("dim_customer.customer_key"), nullable=False
    ),
    Column(
        "product_key", Integer, ForeignKey("dim_product.product_key"), nullable=False
    ),
    Column(
        "origination_date_key",
        Integer,
        ForeignKey("dim_date.date_key"),
        nullable=False,
    ),
    Column("principal_amount", Numeric(16, 2), nullable=False),
    Column("interest_rate", Numeric(6, 3), nullable=False),
    Column("term_months", Integer, nullable=False),
    Column("loan_status", String(20), nullable=False),
    Column("loan_count", Integer, nullable=False, default=1),
    Index("ix_fact_loan_customer_key", "customer_key"),
    Index("ix_fact_loan_product_key", "product_key"),
    Index("ix_fact_loan_date_key", "origination_date_key"),
)

fact_transactions = Table(
    "fact_transactions",
    metadata,
    Column("transaction_key", BigInteger, primary_key=True),
    Column("transaction_id", String(40), nullable=False, unique=True),
    Column("loan_key", Integer, ForeignKey("dim_loan.loan_key"), nullable=False),
    Column(
        "customer_key", Integer, ForeignKey("dim_customer.customer_key"), nullable=False
    ),
    Column(
        "transaction_date_key",
        Integer,
        ForeignKey("dim_date.date_key"),
        nullable=False,
    ),
    Column("amount", Numeric(16, 2), nullable=False),
    Column("transaction_type", String(24), nullable=False),
    Column("channel", String(24), nullable=False),
    Column("status", String(20), nullable=False),
    Column("is_successful", Boolean, nullable=False),
    Index("ix_fact_txn_loan_key", "loan_key"),
    Index("ix_fact_txn_customer_key", "customer_key"),
    Index("ix_fact_txn_date_key", "transaction_date_key"),
)

# ---------------------------------------------------------------------------
# ETL observability / audit
# ---------------------------------------------------------------------------

etl_pipeline_runs = Table(
    "etl_pipeline_runs",
    metadata,
    Column("run_id", Integer, primary_key=True, autoincrement=True),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    Column("status", String(20), nullable=False),
    Column("extracted_rows", Integer, nullable=False, default=0),
    Column("transformed_rows", Integer, nullable=False, default=0),
    Column("rejected_rows", Integer, nullable=False, default=0),
    Column("loaded_rows", Integer, nullable=False, default=0),
    Column("validation_failures", Integer, nullable=False, default=0),
    Column("validation_warnings", Integer, nullable=False, default=0),
    Column("duration_seconds", Float, nullable=True),
    Column("error_message", Text, nullable=True),
    Column("run_details", JSON, nullable=True),
)

etl_rejected_rows = Table(
    "etl_rejected_rows",
    metadata,
    Column("rejection_id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, ForeignKey("etl_pipeline_runs.run_id"), nullable=False),
    Column("source", String(60), nullable=False),
    Column("source_row", Integer, nullable=True),
    Column("reason_code", String(60), nullable=False),
    Column("reason", Text, nullable=False),
    Column("raw_record", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Index("ix_etl_rejections_run_id", "run_id"),
    Index("ix_etl_rejections_reason_code", "reason_code"),
)

etl_validation_results = Table(
    "etl_validation_results",
    metadata,
    Column("validation_id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, ForeignKey("etl_pipeline_runs.run_id"), nullable=False),
    Column("check_name", String(80), nullable=False),
    Column("category", String(40), nullable=False),
    Column("status", String(10), nullable=False, comment="PASS, WARN or FAIL"),
    Column("expected_value", String(160), nullable=True),
    Column("actual_value", String(160), nullable=True),
    Column("details", Text, nullable=True),
    Column("checked_at", DateTime(timezone=True), nullable=False),
    Index("ix_etl_validations_run_id", "run_id"),
    UniqueConstraint("run_id", "check_name", name="uq_validation_run_check"),
)

ALL_WAREHOUSE_TABLES = (
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_loan",
    "fact_loan_originations",
    "fact_transactions",
)


def create_db_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine with sensible demo/test defaults."""
    connect_args: dict[str, object] = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        future=True,
        connect_args=connect_args,
    )
    if engine.dialect.name == "sqlite":

        @event.listens_for(engine, "connect")
        def _enable_sqlite_foreign_keys(dbapi_connection, _connection_record):  # type: ignore[no-untyped-def]
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    return engine


def initialize_database(engine: Engine) -> None:
    """Create all warehouse and audit tables if they do not exist."""
    metadata.create_all(engine, checkfirst=True)
