"""Generate deterministic, intentionally messy synthetic finance datasets.

The raw CSVs model three operational systems plus a product reference feed:

* ``crm_customers.csv``          CRM / customer master
* ``loan_core.csv``              Core lending platform
* ``payment_ledger.csv``         Payments and servicing ledger
* ``product_catalog.csv``        Product reference data

Each source deliberately contains realistic data-quality defects so the ETL
pipeline has something meaningful to clean, reject, and audit.
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from finance_dwh.config import PROJECT_ROOT

SEED = 20261108
REFERENCE_DATE = date(2026, 10, 31)
CUSTOMER_COUNT = 220
LOAN_COUNT = 280

FIRST_NAMES = [
    "Aarav",
    "Amelia",
    "Arjun",
    "Ava",
    "Benjamin",
    "Chloe",
    "Daniel",
    "Diya",
    "Elena",
    "Ethan",
    "Fatima",
    "Gabriel",
    "Grace",
    "Hassan",
    "Isabella",
    "Ishaan",
    "Jacob",
    "Jasmine",
    "Kai",
    "Layla",
    "Leo",
    "Maya",
    "Mia",
    "Noah",
    "Olivia",
    "Omar",
    "Priya",
    "Ravi",
    "Sara",
    "Sofia",
    "Thomas",
    "Zara",
    "Lucas",
    "Nora",
    "Yusuf",
    "Anika",
    "Mateo",
    "Leila",
    "Henry",
    "Aisha",
    "Diego",
    "Freya",
    "Kabir",
    "Lina",
    "Marcus",
    "Nadia",
    "Owen",
    "Rina",
    "Samir",
    "Tara",
]
LAST_NAMES = [
    "Ahmed",
    "Anderson",
    "Bennett",
    "Chen",
    "Costa",
    "Davis",
    "Fernandez",
    "Garcia",
    "Gupta",
    "Hassan",
    "Ivanova",
    "Johnson",
    "Kapoor",
    "Khan",
    "Kim",
    "Kumar",
    "Lee",
    "Lopez",
    "Martin",
    "Mehta",
    "Miller",
    "Nguyen",
    "Patel",
    "Rahman",
    "Reed",
    "Rossi",
    "Sato",
    "Shah",
    "Silva",
    "Singh",
    "Smith",
    "Tan",
    "Taylor",
    "Thompson",
    "Walsh",
    "Wang",
    "Williams",
    "Wilson",
    "Yamamoto",
    "Zhang",
]
COUNTRIES = [
    "United States",
    "United States",
    "United Kingdom",
    "Germany",
    "France",
    "Netherlands",
    "Singapore",
    "United Arab Emirates",
    "Canada",
    "Australia",
    "India",
    "Japan",
    "Brazil",
    "South Africa",
]
SEGMENTS = ["Retail", "Retail", "Retail", "Premium", "Private Banking"]
CHANNELS = ["mobile", "online", "branch", "atm", "direct debit"]

PRODUCTS = [
    {
        "product_code": "PL_PERSONAL",
        "product_name": "Personal Loan",
        "product_family": "Personal Lending",
        "base_rate": 11.25,
        "is_secured": "No",
        "amount_range": (4_000, 45_000),
        "term_choices": (24, 36, 48, 60),
    },
    {
        "product_code": "PL_AUTO",
        "product_name": "Auto Loan",
        "product_family": "Asset Finance",
        "base_rate": 7.90,
        "is_secured": "Yes",
        "amount_range": (8_000, 65_000),
        "term_choices": (36, 48, 60, 72),
    },
    {
        "product_code": "PL_HOME",
        "product_name": "Home Loan",
        "product_family": "Mortgage",
        "base_rate": 5.40,
        "is_secured": "Yes",
        "amount_range": (120_000, 650_000),
        "term_choices": (180, 240, 300, 360),
    },
    {
        "product_code": "PL_STUDENT",
        "product_name": "Student Loan",
        "product_family": "Education Finance",
        "base_rate": 6.75,
        "is_secured": "No",
        "amount_range": (5_000, 35_000),
        "term_choices": (36, 60, 84),
    },
    {
        "product_code": "PL_BUSINESS",
        "product_name": "Business Loan",
        "product_family": "Commercial Lending",
        "base_rate": 10.10,
        "is_secured": "No",
        "amount_range": (20_000, 250_000),
        "term_choices": (12, 24, 36, 60),
    },
    {
        "product_code": "CC_REVOLVING",
        "product_name": "Revolving Credit Card",
        "product_family": "Cards",
        "base_rate": 19.99,
        "is_secured": "No",
        "amount_range": (1_500, 20_000),
        "term_choices": (12, 24, 36),
    },
    {
        "product_code": "BNPL",
        "product_name": "Buy Now Pay Later",
        "product_family": "Consumer Credit",
        "base_rate": 15.50,
        "is_secured": "No",
        "amount_range": (250, 4_000),
        "term_choices": (3, 6, 12),
    },
    {
        "product_code": "LOC",
        "product_name": "Line of Credit",
        "product_family": "Commercial Lending",
        "base_rate": 9.35,
        "is_secured": "Yes",
        "amount_range": (10_000, 100_000),
        "term_choices": (12, 24, 36),
    },
]


def _date_text(value: date, rng: random.Random) -> str:
    """Render a date in one of several source-system styles."""
    style = rng.choice(("%Y-%m-%d", "%d/%m/%Y", "%b %d, %Y", "%d-%m-%Y", "%Y/%m/%d"))
    return value.strftime(style)


def _random_date(rng: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rng.randint(0, (end - start).days))


def _money(value: float, rng: random.Random) -> str:
    style = rng.randrange(5)
    if style == 0:
        return f"${value:,.2f}"
    if style == 1:
        return f"{value:,.2f}"
    if style == 2:
        return f"{value:.2f}"
    if style == 3:
        return f"{int(value)}"
    return f" {value:,.2f} "


def _rate(value: float, rng: random.Random) -> str:
    style = rng.randrange(3)
    if style == 0:
        return f"{value:.2f}%"
    if style == 1:
        return f"{value:.2f}"
    return f" {value:.3f}% "


def _source_customer_id(customer_id: str, rng: random.Random) -> str:
    if rng.random() < 0.12:
        return f" {customer_id.lower()} "
    if rng.random() < 0.05:
        return customer_id.lower()
    return customer_id


def _source_loan_id(loan_id: str, rng: random.Random) -> str:
    if rng.random() < 0.10:
        return f" {loan_id.lower()} "
    return loan_id


def _loan_status(origination: date, rng: random.Random) -> str:
    age_days = (REFERENCE_DATE - origination).days
    if age_days > 900:
        status = rng.choices(
            ["Active", "Closed", "Defaulted"], weights=[45, 45, 10], k=1
        )[0]
    elif age_days > 365:
        status = rng.choices(
            ["Active", "Closed", "Defaulted"], weights=[62, 32, 6], k=1
        )[0]
    else:
        status = rng.choices(
            ["Active", "Pending", "Defaulted"], weights=[86, 10, 4], k=1
        )[0]
    if rng.random() < 0.20:
        return status.lower()
    if rng.random() < 0.10:
        return status.upper()
    if rng.random() < 0.03:
        return ""
    return status


def _transaction_status(rng: random.Random) -> str:
    status = rng.choices(["Completed", "Pending", "Failed"], weights=[86, 7, 7], k=1)[0]
    variant = rng.randrange(5)
    if variant == 0:
        return status.lower()
    if variant == 1:
        return status.upper()
    if variant == 2 and status == "Completed":
        return "success"
    if variant == 3 and rng.random() < 0.18:
        return ""
    return status


def _transaction_type(rng: random.Random) -> str:
    txn_type = rng.choices(["Payment", "Fee", "Refund"], weights=[78, 14, 8], k=1)[0]
    if rng.random() < 0.25:
        return txn_type.lower()
    if rng.random() < 0.10:
        return txn_type.upper()
    return txn_type


def _channel(rng: random.Random) -> str:
    channel = rng.choice(CHANNELS)
    if rng.random() < 0.35:
        return channel.title()
    if rng.random() < 0.10:
        return channel.upper()
    return channel


def _customer_records(
    rng: random.Random,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    reference: list[dict[str, Any]] = []

    for index in range(1, CUSTOMER_COUNT + 1):
        customer_id = f"CUST-{index:04d}"
        first_name = rng.choice(FIRST_NAMES)
        last_name = rng.choice(LAST_NAMES)
        email = f"{first_name.lower()}.{last_name.lower()}{index}@example.com"
        dob = _random_date(rng, date(1960, 1, 1), date(2003, 12, 31))
        signup = _random_date(rng, date(2022, 1, 1), date(2026, 9, 30))
        country = rng.choice(COUNTRIES)
        segment = rng.choice(SEGMENTS)

        if rng.random() < 0.10:
            email = ""
        elif rng.random() < 0.05:
            email = rng.choice(
                ("not-an-email", "missing-at.example.com", "@example.com")
            )
        if rng.random() < 0.05:
            country = ""
        if rng.random() < 0.06:
            segment = ""

        raw = {
            "customer_id": _source_customer_id(customer_id, rng),
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "date_of_birth": _date_text(dob, rng),
            "country": country,
            "customer_segment": segment,
            "created_at": _date_text(signup, rng),
        }
        rows.append(raw)
        reference.append(
            {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "date_of_birth": dob,
                "country": country,
                "segment": segment,
                "signup": signup,
            }
        )

    # Seven source updates with the same natural key: the ETL keeps the most
    # recent signup record and rejects the superseded versions.
    for customer in rng.sample(reference, 7):
        duplicate = {
            "customer_id": _source_customer_id(customer["customer_id"], rng),
            "first_name": customer["first_name"],
            "last_name": customer["last_name"],
            "email": f"updated.{customer['email'] or 'customer'}",
            "date_of_birth": _date_text(customer["date_of_birth"], rng),
            "country": customer["country"] or "Unknown",
            "customer_segment": customer["segment"] or "Retail",
            "created_at": _date_text(
                customer["signup"] + timedelta(days=rng.randint(1, 45)), rng
            ),
        }
        rows.append(duplicate)

    # Invalid records exercise required-field and malformed-date handling.
    rows.extend(
        [
            {
                "customer_id": "",
                "first_name": "Missing",
                "last_name": "Identifier",
                "email": "missing.id@example.com",
                "date_of_birth": "1990-01-01",
                "country": "Singapore",
                "customer_segment": "Retail",
                "created_at": "2025-01-10",
            },
            {
                "customer_id": "   ",
                "first_name": "Blank",
                "last_name": "Identifier",
                "email": "blank.id@example.com",
                "date_of_birth": "1985-06-12",
                "country": "Canada",
                "customer_segment": "Premium",
                "created_at": "31/12/2024",
            },
            {
                "customer_id": "CUST-9001",
                "first_name": "",
                "last_name": "",
                "email": "no.name@example.com",
                "date_of_birth": "1991-05-05",
                "country": "India",
                "customer_segment": "Retail",
                "created_at": "2024-08-20",
            },
            {
                "customer_id": "CUST-9002",
                "first_name": "",
                "last_name": "",
                "email": "no.name.two@example.com",
                "date_of_birth": "1993-07-05",
                "country": "Germany",
                "customer_segment": "Retail",
                "created_at": "2024-09-20",
            },
            {
                "customer_id": "CUST-9003",
                "first_name": "Invalid",
                "last_name": "Signup",
                "email": "invalid.signup@example.com",
                "date_of_birth": "1988-02-03",
                "country": "France",
                "customer_segment": "Premium",
                "created_at": "not-a-date",
            },
            {
                "customer_id": "CUST-9004",
                "first_name": "Impossible",
                "last_name": "Signup",
                "email": "impossible.signup@example.com",
                "date_of_birth": "1988-02-03",
                "country": "France",
                "customer_segment": "Premium",
                "created_at": "31/02/2025",
            },
            {
                "customer_id": "CUST-9005",
                "first_name": "Missing",
                "last_name": "Signup",
                "email": "missing.signup@example.com",
                "date_of_birth": "1988-02-03",
                "country": "France",
                "customer_segment": "Premium",
                "created_at": "",
            },
        ]
    )
    return rows, reference


def _product_records() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for product in PRODUCTS:
        rows.append(
            {
                "product_code": product["product_code"],
                "product_name": product["product_name"],
                "product_family": product["product_family"],
                "base_rate": f"{product['base_rate']:.2f}%",
                "is_secured": product["is_secured"],
            }
        )
    # Duplicate reference row plus an incomplete record.
    duplicate = dict(rows[2])
    rows.append(duplicate)
    rows.append(
        {
            "product_code": "PL_UNKNOWN",
            "product_name": "",
            "product_family": "Unassigned",
            "base_rate": "n/a",
            "is_secured": "No",
        }
    )
    return rows


def _loan_records(
    rng: random.Random, customers: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    base_loans: list[dict[str, Any]] = []
    for index in range(1, LOAN_COUNT + 1):
        loan_id = f"LN-{index:06d}"
        customer = rng.choice(customers)
        product = rng.choice(PRODUCTS)
        amount = round(rng.uniform(*product["amount_range"]), 2)
        rate = round(product["base_rate"] + rng.uniform(-1.75, 2.75), 2)
        term = rng.choice(product["term_choices"])
        origination = _random_date(rng, date(2023, 1, 1), date(2026, 10, 15))
        status = _loan_status(origination, rng)

        raw = {
            "loan_ref": _source_loan_id(loan_id, rng),
            "borrower_id": _source_customer_id(customer["customer_id"], rng),
            "product_code": (
                product["product_code"].lower()
                if rng.random() < 0.08
                else product["product_code"]
            ),
            "principal": _money(amount, rng),
            "annual_rate": _rate(rate, rng),
            "term_months": str(term),
            "origination_dt": _date_text(origination, rng),
            "loan_status": status,
        }
        rows.append(raw)
        base_loans.append(
            {
                "loan_ref": loan_id,
                "borrower_id": customer["customer_id"],
                "product_code": product["product_code"],
                "amount": amount,
                "rate": rate,
                "term": term,
                "origination": origination,
                "status": status,
            }
        )

    # Duplicate loan feeds (late-arriving updates). The pipeline keeps one row
    # per loan_ref and records the superseded row as rejected.
    for loan in rng.sample(base_loans, 6):
        rows.append(
            {
                "loan_ref": _source_loan_id(loan["loan_ref"], rng),
                "borrower_id": loan["borrower_id"],
                "product_code": loan["product_code"],
                "principal": _money(loan["amount"], rng),
                "annual_rate": _rate(loan["rate"] + 0.15, rng),
                "term_months": str(loan["term"]),
                "origination_dt": _date_text(loan["origination"], rng),
                "loan_status": rng.choice(["Active", "Closed", "active", "CLOSED"]),
            }
        )

    template = {
        "loan_ref": "LN-990001",
        "borrower_id": customers[0]["customer_id"],
        "product_code": "PL_PERSONAL",
        "principal": "25000.00",
        "annual_rate": "11.00%",
        "term_months": "36",
        "origination_dt": "2025-01-15",
        "loan_status": "Active",
    }
    invalid_rows = []
    for borrower in ("CUST-9991", "CUST-9992"):
        row = dict(template)
        row["loan_ref"] = f"LN-99{borrower[-4:]}"
        row["borrower_id"] = borrower
        invalid_rows.append(row)
    row = dict(template)
    row["loan_ref"] = "LN-990003"
    row["product_code"] = "PL_CRYPTO"
    invalid_rows.append(row)
    row = dict(template)
    row["loan_ref"] = "LN-990004"
    row["principal"] = ""
    invalid_rows.append(row)
    row = dict(template)
    row["loan_ref"] = "LN-990005"
    row["principal"] = "-5000"
    invalid_rows.append(row)
    row = dict(template)
    row["loan_ref"] = "LN-990006"
    row["origination_dt"] = "13/13/2025"
    invalid_rows.append(row)
    row = dict(template)
    row["loan_ref"] = "LN-990007"
    row["origination_dt"] = "not-a-date"
    invalid_rows.append(row)
    row = dict(template)
    row["loan_ref"] = "LN-990008"
    row["term_months"] = ""
    invalid_rows.append(row)
    row = dict(template)
    row["loan_ref"] = "LN-990009"
    row["principal"] = "0"
    invalid_rows.append(row)
    row = dict(template)
    row["loan_ref"] = ""
    invalid_rows.append(row)
    rows.extend(invalid_rows)
    return rows, base_loans


def _transaction_records(
    rng: random.Random,
    loans: list[dict[str, Any]],
    customers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    customer_ids = {customer["customer_id"] for customer in customers}
    transaction_number = 0

    def add_transaction(
        loan: dict[str, Any],
        txn_date: date,
        amount: float,
        txn_type: str,
        status: str,
        channel: str,
    ) -> None:
        nonlocal transaction_number
        transaction_number += 1
        rows.append(
            {
                "txn_id": f"TXN-{transaction_number:06d}",
                "loan_ref": _source_loan_id(loan["loan_ref"], rng),
                "borrower_id": _source_customer_id(loan["borrower_id"], rng),
                "txn_date": _date_text(txn_date, rng),
                "amount": _money(amount, rng),
                "transaction_type": txn_type,
                "channel": channel,
                "status": status,
            }
        )

    for loan in loans:
        assert loan["borrower_id"] in customer_ids
        disbursement_date = loan["origination"]
        add_transaction(
            loan,
            disbursement_date,
            loan["amount"],
            "Disbursement",
            "Completed",
            _channel(rng),
        )

        extra_count = rng.randint(1, 5)
        max_days = max(1, (REFERENCE_DATE - disbursement_date).days)
        for _ in range(extra_count):
            txn_date = disbursement_date + timedelta(
                days=rng.randint(1, min(max_days, 900))
            )
            txn_type = _transaction_type(rng)
            if txn_type.lower() == "payment":
                base_payment = loan["amount"] / max(loan["term"], 12)
                amount = round(base_payment * rng.uniform(0.75, 1.35), 2)
            elif txn_type.lower() == "fee":
                amount = round(rng.uniform(5, 120), 2)
            else:
                base_payment = loan["amount"] / max(loan["term"], 12)
                amount = round(base_payment * rng.uniform(0.35, 1.0), 2)
            add_transaction(
                loan,
                txn_date,
                max(amount, 1.0),
                txn_type,
                _transaction_status(rng),
                _channel(rng),
            )

    # Duplicate transaction events. The latest source record wins.
    for transaction in rng.sample(rows, 12):
        duplicate_date = parse_or_today(transaction["txn_date"], REFERENCE_DATE)
        duplicate = dict(transaction)
        duplicate["txn_date"] = _date_text(
            min(REFERENCE_DATE, duplicate_date + timedelta(days=1)), rng
        )
        duplicate["status"] = rng.choice(["Completed", "completed", "Failed"])
        rows.append(duplicate)

    template = {
        "txn_id": "TXN-990001",
        "loan_ref": loans[0]["loan_ref"],
        "borrower_id": loans[0]["borrower_id"],
        "txn_date": "2025-02-01",
        "amount": "120.00",
        "transaction_type": "Payment",
        "channel": "Mobile",
        "status": "Completed",
    }
    invalid_rows = []
    for _ in range(2):
        row = dict(template)
        row["txn_id"] = ""
        row["loan_ref"] = loans[0]["loan_ref"]
        invalid_rows.append(row)
    for ref in ("LN-999999", "LN-888888"):
        row = dict(template)
        row["txn_id"] = f"TXN-99{ref[-4:]}"
        row["loan_ref"] = ref
        invalid_rows.append(row)
    for amount in ("-30.00", "-150.00"):
        row = dict(template)
        row["txn_id"] = f"TXN-99{len(invalid_rows) + 1:04d}"
        row["loan_ref"] = loans[1]["loan_ref"]
        row["amount"] = amount
        invalid_rows.append(row)
    row = dict(template)
    row["txn_id"] = "TXN-990011"
    row["loan_ref"] = loans[2]["loan_ref"]
    row["amount"] = "0"
    invalid_rows.append(row)
    for invalid_date in ("31/02/2025", "not-a-date"):
        row = dict(template)
        row["txn_id"] = f"TXN-99{len(invalid_rows) + 1:04d}"
        row["loan_ref"] = loans[3]["loan_ref"]
        row["txn_date"] = invalid_date
        invalid_rows.append(row)
    row = dict(template)
    row["txn_id"] = "TXN-990015"
    row["loan_ref"] = ""
    invalid_rows.append(row)
    rows.extend(invalid_rows)
    return rows


def parse_or_today(text: str, fallback: date) -> date:
    """Small generator-only date helper for duplicate timestamps."""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text.strip(), fmt).date()
        except ValueError:
            continue
    return fallback


def generate_datasets(output_dir: Path | None = None) -> dict[str, Any]:
    """Generate all raw CSVs and a manifest; return generation metadata."""
    output_dir = (output_dir or PROJECT_ROOT / "data" / "raw").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)

    customers, _ = _customer_records(rng)
    products = _product_records()
    # Reconstruct clean loan references from the customer rows, ignoring the
    # invalid source-only customers. This keeps the generator self-contained.
    clean_customers = [
        {
            "customer_id": f"CUST-{index:04d}",
            "first_name": "",
            "last_name": "",
            "email": "",
            "date_of_birth": date(1990, 1, 1),
            "country": "",
            "segment": "",
            "signup": date(2023, 1, 1),
        }
        for index in range(1, CUSTOMER_COUNT + 1)
    ]
    loans, base_loans = _loan_records(rng, clean_customers)
    transactions = _transaction_records(rng, base_loans, clean_customers)

    datasets = {
        "crm_customers.csv": pd.DataFrame(customers),
        "product_catalog.csv": pd.DataFrame(products),
        "loan_core.csv": pd.DataFrame(loans),
        "payment_ledger.csv": pd.DataFrame(transactions),
    }
    for filename, frame in datasets.items():
        frame.to_csv(output_dir / filename, index=False, encoding="utf-8")

    issue_notes = {
        "crm_customers.csv": {
            "duplicate_customer_updates": 7,
            "missing_identifiers": 2,
            "missing_names": 2,
            "invalid_or_missing_signup_dates": 3,
            "missing_or_malformed_emails": "randomly embedded",
            "inconsistent_date_formats": True,
        },
        "product_catalog.csv": {
            "duplicate_product_code": 1,
            "invalid_reference_row": 1,
        },
        "loan_core.csv": {
            "duplicate_loan_updates": 6,
            "orphan_customers": 2,
            "unknown_product": 1,
            "invalid_principal": 3,
            "invalid_dates": 2,
            "missing_term": 1,
            "missing_loan_id": 1,
            "inconsistent_number_formats": True,
        },
        "payment_ledger.csv": {
            "duplicate_transaction_events": 12,
            "missing_transaction_ids": 2,
            "unknown_loan_references": 2,
            "invalid_amounts": 3,
            "invalid_dates": 2,
            "missing_loan_reference": 1,
            "mixed_case_categories": True,
        },
    }
    manifest = {
        "dataset": "Helix Finance synthetic operational sources",
        "seed": SEED,
        "reference_date": REFERENCE_DATE.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "sources": {
            filename: {
                "rows": int(len(frame)),
                "columns": list(frame.columns),
                "intentional_issues": issue_notes[filename],
            }
            for filename, frame in datasets.items()
        },
        "totals": {
            "rows": int(sum(len(frame) for frame in datasets.values())),
            "sources": len(datasets),
        },
    }
    (output_dir / "_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic financial source data"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw",
        help="Directory for generated CSV files",
    )
    args = parser.parse_args()
    manifest = generate_datasets(args.output_dir)
    print("Generated raw datasets:")
    for filename, details in manifest["sources"].items():
        print(f"  {filename:<24} {details['rows']:>5} rows")
    print(
        f"\nTotal: {manifest['totals']['rows']} rows from {manifest['totals']['sources']} sources"
    )
    print(f"Output: {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
