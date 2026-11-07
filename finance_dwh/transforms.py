"""Extract and transform raw operational CSV sources into star-schema frames."""

from __future__ import annotations

import calendar
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from finance_dwh.quality import (
    as_json_safe,
    clean_text,
    date_to_key,
    is_valid_email,
    normalize_email,
    normalize_id,
    parse_bool,
    parse_decimal,
    parse_mixed_date,
    parse_positive_int,
)

SOURCE_FILES = {
    "crm_customers": "crm_customers.csv",
    "product_catalog": "product_catalog.csv",
    "loan_core": "loan_core.csv",
    "payment_ledger": "payment_ledger.csv",
}

LOAN_STATUS_MAP = {
    "active": "active",
    "open": "active",
    "current": "active",
    "closed": "closed",
    "paid": "closed",
    "paid off": "closed",
    "settled": "closed",
    "defaulted": "defaulted",
    "default": "defaulted",
    "charged off": "defaulted",
    "delinquent": "defaulted",
    "pending": "pending",
    "approved": "pending",
}

TRANSACTION_TYPE_MAP = {
    "payment": "payment",
    "repayment": "payment",
    "disbursement": "disbursement",
    "drawdown": "disbursement",
    "fee": "fee",
    "late fee": "fee",
    "refund": "refund",
    "reversal": "refund",
}

CHANNEL_MAP = {
    "mobile": "mobile",
    "mobile app": "mobile",
    "online": "online",
    "web": "online",
    "branch": "branch",
    "atm": "atm",
    "direct debit": "direct_debit",
    "direct_debit": "direct_debit",
}

TRANSACTION_STATUS_MAP = {
    "completed": "completed",
    "complete": "completed",
    "success": "completed",
    "succeeded": "completed",
    "settled": "completed",
    "pending": "pending",
    "processing": "pending",
    "failed": "failed",
    "failure": "failed",
    "declined": "failed",
}

SEGMENT_MAP = {
    "retail": "Retail",
    "premium": "Premium",
    "private": "Private Banking",
    "private banking": "Private Banking",
    "wealth": "Private Banking",
}


@dataclass(frozen=True)
class RejectedRow:
    source: str
    source_row: int
    reason_code: str
    reason: str
    raw_record: dict[str, Any]


@dataclass
class TransformResult:
    frame: pd.DataFrame
    rejected: list[RejectedRow]
    extracted: int
    source: str

    @property
    def accepted(self) -> int:
        return len(self.frame)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected)


@dataclass
class WarehouseFrames:
    dim_date: pd.DataFrame = field(default_factory=pd.DataFrame)
    dim_customer: pd.DataFrame = field(default_factory=pd.DataFrame)
    dim_product: pd.DataFrame = field(default_factory=pd.DataFrame)
    dim_loan: pd.DataFrame = field(default_factory=pd.DataFrame)
    fact_loan_originations: pd.DataFrame = field(default_factory=pd.DataFrame)
    fact_transactions: pd.DataFrame = field(default_factory=pd.DataFrame)

    def as_dict(self) -> dict[str, pd.DataFrame]:
        return {
            "dim_date": self.dim_date,
            "dim_customer": self.dim_customer,
            "dim_product": self.dim_product,
            "dim_loan": self.dim_loan,
            "fact_loan_originations": self.fact_loan_originations,
            "fact_transactions": self.fact_transactions,
        }


def read_source(path: Path) -> pd.DataFrame:
    """Read a CSV as strings so source identifiers are never type-coerced."""
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    if frame.empty or len(frame.columns) == 0:
        raise ValueError(f"Source file is empty: {path}")
    frame["__source_row"] = frame.index + 2
    return frame


def _raw_record(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: as_json_safe(value)
        for key, value in row.items()
        if not str(key).startswith("__")
    }


def _reject(
    rejects: list[RejectedRow],
    source: str,
    row: dict[str, Any],
    code: str,
    reason: str,
) -> None:
    rejects.append(
        RejectedRow(
            source=source,
            source_row=int(row["__source_row"]),
            reason_code=code,
            reason=reason,
            raw_record=_raw_record(row),
        )
    )


def _dedupe_latest(
    records: list[dict[str, Any]],
    *,
    source: str,
    key_field: str,
    order_field: str,
    rejects: list[RejectedRow],
    reason_code: str,
) -> list[dict[str, Any]]:
    """Keep the highest-ordered row per natural key and reject superseded rows."""
    winners: dict[str, dict[str, Any]] = {}
    for record in records:
        key = record[key_field]
        existing = winners.get(key)
        if existing is None:
            winners[key] = record
            continue

        incoming_order = (record[order_field], record["__source_row"])
        existing_order = (existing[order_field], existing["__source_row"])
        if incoming_order >= existing_order:
            _reject(
                rejects,
                source,
                existing,
                reason_code,
                f"Duplicate {key_field} {key!r}; superseded by a later source record.",
            )
            winners[key] = record
        else:
            _reject(
                rejects,
                source,
                record,
                reason_code,
                f"Duplicate {key_field} {key!r}; a later source record was retained.",
            )
    return sorted(winners.values(), key=lambda record: record[key_field])


def _canonical(value: Any, mapping: dict[str, str], default: str) -> str:
    cleaned = clean_text(value).lower().replace("-", "_")
    return mapping.get(cleaned, default) if cleaned else default


def transform_customers(source: pd.DataFrame) -> TransformResult:
    source_name = "crm_customers"
    rejects: list[RejectedRow] = []
    records: list[dict[str, Any]] = []

    for row in source.to_dict("records"):
        customer_id = normalize_id(row.get("customer_id"))
        first_name = clean_text(row.get("first_name"))
        last_name = clean_text(row.get("last_name"))
        signup_date = parse_mixed_date(row.get("created_at"))

        if not customer_id:
            _reject(
                rejects,
                source_name,
                row,
                "CUSTOMER_MISSING_ID",
                "customer_id is required and must not be blank.",
            )
            continue
        if not first_name and not last_name:
            _reject(
                rejects,
                source_name,
                row,
                "CUSTOMER_MISSING_NAME",
                "At least one of first_name or last_name is required.",
            )
            continue
        if signup_date is None:
            _reject(
                rejects,
                source_name,
                row,
                "CUSTOMER_INVALID_SIGNUP_DATE",
                f"created_at could not be parsed: {row.get('created_at')!r}.",
            )
            continue

        email = normalize_email(row.get("email"))
        email_imputed = not is_valid_email(email)
        if email_imputed:
            email = f"unknown+{customer_id.lower()}@example.com"

        country = clean_text(row.get("country"), "Unknown")
        segment = _canonical(row.get("customer_segment"), SEGMENT_MAP, "Retail")
        dob = parse_mixed_date(row.get("date_of_birth"))

        records.append(
            {
                "customer_id": customer_id,
                "full_name": f"{first_name} {last_name}".strip(),
                "email": email,
                "email_imputed": email_imputed,
                "country": country,
                "segment": segment,
                "date_of_birth": dob,
                "signup_date": signup_date,
                "__source_row": int(row["__source_row"]),
            }
        )

    records = _dedupe_latest(
        records,
        source=source_name,
        key_field="customer_id",
        order_field="signup_date",
        rejects=rejects,
        reason_code="CUSTOMER_DUPLICATE",
    )
    for index, record in enumerate(records, start=1):
        record["customer_key"] = index

    columns = [
        "customer_key",
        "customer_id",
        "full_name",
        "email",
        "email_imputed",
        "country",
        "segment",
        "date_of_birth",
        "signup_date",
    ]
    frame = pd.DataFrame(records)
    if not frame.empty:
        frame = frame[columns]
    return TransformResult(
        frame=frame,
        rejected=rejects,
        extracted=len(source),
        source=source_name,
    )


def transform_products(source: pd.DataFrame) -> TransformResult:
    source_name = "product_catalog"
    rejects: list[RejectedRow] = []
    records: list[dict[str, Any]] = []

    for row in source.to_dict("records"):
        product_code = normalize_id(row.get("product_code"))
        product_name = clean_text(row.get("product_name"))
        if not product_code:
            _reject(
                rejects,
                source_name,
                row,
                "PRODUCT_MISSING_CODE",
                "product_code is required.",
            )
            continue
        if not product_name:
            _reject(
                rejects,
                source_name,
                row,
                "PRODUCT_MISSING_NAME",
                "product_name is required.",
            )
            continue
        try:
            base_rate = parse_decimal(row.get("base_rate"), percent=True)
        except ValueError:
            base_rate = None
        if base_rate is None or base_rate < 0 or base_rate > 100:
            _reject(
                rejects,
                source_name,
                row,
                "PRODUCT_INVALID_RATE",
                f"base_rate is invalid: {row.get('base_rate')!r}.",
            )
            continue

        records.append(
            {
                "product_code": product_code,
                "product_name": product_name,
                "product_family": clean_text(row.get("product_family"), "Unassigned"),
                "base_rate": base_rate,
                "is_secured": parse_bool(row.get("is_secured")),
                "__source_row": int(row["__source_row"]),
            }
        )

    records = _dedupe_latest(
        records,
        source=source_name,
        key_field="product_code",
        order_field="__source_row",
        rejects=rejects,
        reason_code="PRODUCT_DUPLICATE",
    )
    for index, record in enumerate(records, start=1):
        record["product_key"] = index

    columns = [
        "product_key",
        "product_code",
        "product_name",
        "product_family",
        "base_rate",
        "is_secured",
    ]
    frame = pd.DataFrame(records)
    if not frame.empty:
        frame = frame[columns]
    return TransformResult(
        frame=frame,
        rejected=rejects,
        extracted=len(source),
        source=source_name,
    )


def transform_loans(
    source: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
) -> TransformResult:
    source_name = "loan_core"
    rejects: list[RejectedRow] = []
    records: list[dict[str, Any]] = []
    customer_lookup = {
        row["customer_id"]: int(row["customer_key"])
        for row in customers.to_dict("records")
    }
    product_lookup = {
        row["product_code"]: int(row["product_key"])
        for row in products.to_dict("records")
    }

    for row in source.to_dict("records"):
        loan_id = normalize_id(row.get("loan_ref"))
        customer_id = normalize_id(row.get("borrower_id"))
        product_code = normalize_id(row.get("product_code"))

        if not loan_id:
            _reject(
                rejects,
                source_name,
                row,
                "LOAN_MISSING_ID",
                "loan_ref is required.",
            )
            continue
        if not customer_id:
            _reject(
                rejects,
                source_name,
                row,
                "LOAN_MISSING_BORROWER",
                "borrower_id is required.",
            )
            continue
        if not product_code:
            _reject(
                rejects,
                source_name,
                row,
                "LOAN_MISSING_PRODUCT",
                "product_code is required.",
            )
            continue

        try:
            principal = parse_decimal(row.get("principal"))
        except ValueError:
            principal = None
        if principal is None or principal <= 0:
            _reject(
                rejects,
                source_name,
                row,
                "LOAN_INVALID_PRINCIPAL",
                f"principal must be a positive number, got {row.get('principal')!r}.",
            )
            continue

        try:
            interest_rate = parse_decimal(row.get("annual_rate"), percent=True)
        except ValueError:
            interest_rate = None
        if interest_rate is None or interest_rate < 0 or interest_rate > 100:
            _reject(
                rejects,
                source_name,
                row,
                "LOAN_INVALID_RATE",
                f"annual_rate is invalid: {row.get('annual_rate')!r}.",
            )
            continue

        try:
            term_months = parse_positive_int(row.get("term_months"))
        except ValueError:
            term_months = None
        if term_months is None or term_months > 600:
            _reject(
                rejects,
                source_name,
                row,
                "LOAN_INVALID_TERM",
                f"term_months must be a positive integer, got {row.get('term_months')!r}.",
            )
            continue

        origination_date = parse_mixed_date(row.get("origination_dt"))
        if origination_date is None:
            _reject(
                rejects,
                source_name,
                row,
                "LOAN_INVALID_ORIGINATION_DATE",
                f"origination_dt could not be parsed: {row.get('origination_dt')!r}.",
            )
            continue

        if customer_id not in customer_lookup:
            _reject(
                rejects,
                source_name,
                row,
                "LOAN_UNKNOWN_CUSTOMER",
                f"borrower_id {customer_id!r} does not exist in the customer dimension.",
            )
            continue
        if product_code not in product_lookup:
            _reject(
                rejects,
                source_name,
                row,
                "LOAN_UNKNOWN_PRODUCT",
                f"product_code {product_code!r} does not exist in the product catalog.",
            )
            continue

        records.append(
            {
                "loan_id": loan_id,
                "customer_id": customer_id,
                "product_code": product_code,
                "customer_key": customer_lookup[customer_id],
                "product_key": product_lookup[product_code],
                "principal_amount": round(principal, 2),
                "interest_rate": round(interest_rate, 3),
                "term_months": term_months,
                "origination_date": origination_date,
                "loan_status": _canonical(
                    row.get("loan_status"), LOAN_STATUS_MAP, "unknown"
                ),
                "__source_row": int(row["__source_row"]),
            }
        )

    records = _dedupe_latest(
        records,
        source=source_name,
        key_field="loan_id",
        order_field="origination_date",
        rejects=rejects,
        reason_code="LOAN_DUPLICATE",
    )
    for index, record in enumerate(records, start=1):
        record["loan_key"] = index

    columns = [
        "loan_key",
        "loan_id",
        "customer_key",
        "product_key",
        "principal_amount",
        "interest_rate",
        "term_months",
        "origination_date",
        "loan_status",
    ]
    frame = pd.DataFrame(records)
    if not frame.empty:
        frame = frame[columns]
    return TransformResult(
        frame=frame,
        rejected=rejects,
        extracted=len(source),
        source=source_name,
    )


def transform_transactions(
    source: pd.DataFrame,
    loans: pd.DataFrame,
) -> TransformResult:
    source_name = "payment_ledger"
    rejects: list[RejectedRow] = []
    records: list[dict[str, Any]] = []
    loan_lookup = {
        row["loan_id"]: {
            "loan_key": int(row["loan_key"]),
            "customer_key": int(row["customer_key"]),
        }
        for row in loans.to_dict("records")
    }

    for row in source.to_dict("records"):
        transaction_id = normalize_id(row.get("txn_id"))
        loan_id = normalize_id(row.get("loan_ref"))

        if not transaction_id:
            _reject(
                rejects,
                source_name,
                row,
                "TRANSACTION_MISSING_ID",
                "txn_id is required.",
            )
            continue
        if not loan_id:
            _reject(
                rejects,
                source_name,
                row,
                "TRANSACTION_MISSING_LOAN",
                "loan_ref is required.",
            )
            continue

        try:
            amount = parse_decimal(row.get("amount"))
        except ValueError:
            amount = None
        if amount is None or amount <= 0:
            _reject(
                rejects,
                source_name,
                row,
                "TRANSACTION_INVALID_AMOUNT",
                f"amount must be positive, got {row.get('amount')!r}.",
            )
            continue

        transaction_date = parse_mixed_date(row.get("txn_date"))
        if transaction_date is None:
            _reject(
                rejects,
                source_name,
                row,
                "TRANSACTION_INVALID_DATE",
                f"txn_date could not be parsed: {row.get('txn_date')!r}.",
            )
            continue

        loan = loan_lookup.get(loan_id)
        if loan is None:
            _reject(
                rejects,
                source_name,
                row,
                "TRANSACTION_UNKNOWN_LOAN",
                f"loan_ref {loan_id!r} does not exist in the loan dimension.",
            )
            continue

        status = _canonical(row.get("status"), TRANSACTION_STATUS_MAP, "unknown")
        records.append(
            {
                "transaction_id": transaction_id,
                "loan_id": loan_id,
                "loan_key": loan["loan_key"],
                "customer_key": loan["customer_key"],
                "amount": round(amount, 2),
                "transaction_date": transaction_date,
                "transaction_type": _canonical(
                    row.get("transaction_type"), TRANSACTION_TYPE_MAP, "other"
                ),
                "channel": _canonical(row.get("channel"), CHANNEL_MAP, "other"),
                "status": status,
                "is_successful": status == "completed",
                "__source_row": int(row["__source_row"]),
            }
        )

    records = _dedupe_latest(
        records,
        source=source_name,
        key_field="transaction_id",
        order_field="transaction_date",
        rejects=rejects,
        reason_code="TRANSACTION_DUPLICATE",
    )
    for index, record in enumerate(records, start=1):
        record["transaction_key"] = index

    columns = [
        "transaction_key",
        "transaction_id",
        "loan_key",
        "customer_key",
        "amount",
        "transaction_date",
        "transaction_type",
        "channel",
        "status",
        "is_successful",
    ]
    frame = pd.DataFrame(records)
    if not frame.empty:
        frame = frame[columns]
    return TransformResult(
        frame=frame,
        rejected=rejects,
        extracted=len(source),
        source=source_name,
    )


def build_date_dimension(dates: list[date]) -> pd.DataFrame:
    """Build a complete, month-aligned daily date dimension."""
    valid_dates = [value for value in dates if value is not None]
    if not valid_dates:
        raise ValueError("Cannot build dim_date without at least one date.")

    first = min(valid_dates)
    last = max(valid_dates)
    start = date(first.year, first.month, 1)
    end = date(last.year, last.month, calendar.monthrange(last.year, last.month)[1])
    day_count = (end - start).days + 1

    rows: list[dict[str, Any]] = []
    for offset in range(day_count):
        current = start + timedelta(days=offset)
        rows.append(
            {
                "date_key": date_to_key(current),
                "full_date": current,
                "year": current.year,
                "quarter": (current.month - 1) // 3 + 1,
                "month": current.month,
                "month_name": current.strftime("%B"),
                "day_of_month": current.day,
                "day_of_week": current.isoweekday(),
                "day_name": current.strftime("%A"),
                "is_weekend": current.isoweekday() >= 6,
                "year_month": current.strftime("%Y-%m"),
            }
        )
    return pd.DataFrame(rows)


def build_warehouse_frames(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    loans: pd.DataFrame,
    transactions: pd.DataFrame,
) -> WarehouseFrames:
    """Assemble dimension and fact frames from cleaned source data."""
    loan_dates = loans["origination_date"].tolist() if not loans.empty else []
    transaction_dates = (
        transactions["transaction_date"].tolist() if not transactions.empty else []
    )
    signup_dates = customers["signup_date"].tolist() if not customers.empty else []
    dim_date = build_date_dimension([*loan_dates, *transaction_dates, *signup_dates])

    dim_customer = customers.copy()
    dim_product = products.copy()

    dim_loan = loans[
        [
            "loan_key",
            "loan_id",
            "customer_key",
            "product_key",
            "origination_date",
            "term_months",
            "loan_status",
        ]
    ].copy()

    fact_loans = loans[
        [
            "loan_key",
            "customer_key",
            "product_key",
            "origination_date",
            "principal_amount",
            "interest_rate",
            "term_months",
            "loan_status",
        ]
    ].copy()
    fact_loans["origination_date_key"] = fact_loans["origination_date"].map(date_to_key)
    fact_loans["loan_count"] = 1
    fact_loans = fact_loans[
        [
            "loan_key",
            "customer_key",
            "product_key",
            "origination_date_key",
            "principal_amount",
            "interest_rate",
            "term_months",
            "loan_status",
            "loan_count",
        ]
    ]

    fact_transactions = transactions[
        [
            "transaction_key",
            "transaction_id",
            "loan_key",
            "customer_key",
            "transaction_date",
            "amount",
            "transaction_type",
            "channel",
            "status",
            "is_successful",
        ]
    ].copy()
    fact_transactions["transaction_date_key"] = fact_transactions[
        "transaction_date"
    ].map(date_to_key)
    fact_transactions = fact_transactions[
        [
            "transaction_key",
            "transaction_id",
            "loan_key",
            "customer_key",
            "transaction_date_key",
            "amount",
            "transaction_type",
            "channel",
            "status",
            "is_successful",
        ]
    ]

    return WarehouseFrames(
        dim_date=dim_date,
        dim_customer=dim_customer,
        dim_product=dim_product,
        dim_loan=dim_loan,
        fact_loan_originations=fact_loans,
        fact_transactions=fact_transactions,
    )
