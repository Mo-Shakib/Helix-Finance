"""Reusable data-quality and normalization helpers for the ETL layer."""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%d-%b-%Y",
    "%d %b %Y",
    "%b %d, %Y",
    "%B %d, %Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%m/%d/%Y",
)

_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_NUMBER_PATTERN = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")


def clean_text(value: Any, default: str = "") -> str:
    """Trim and collapse whitespace; turn null-like values into a default."""
    if value is None:
        return default
    if isinstance(value, float) and math.isnan(value):
        return default
    text = str(value).replace("\u00a0", " ").strip()
    if not text or text.lower() in {"nan", "none", "null", "n/a", "-"}:
        return default
    return re.sub(r"\s+", " ", text)


def is_blank(value: Any) -> bool:
    return clean_text(value) == ""


def normalize_id(value: Any) -> str:
    """Canonicalize natural keys: remove whitespace and uppercase."""
    return re.sub(r"\s+", "", clean_text(value)).upper()


def normalize_email(value: Any) -> str:
    return clean_text(value).lower()


def is_valid_email(value: Any) -> bool:
    email = normalize_email(value)
    return bool(_EMAIL_PATTERN.match(email))


def parse_mixed_date(value: Any) -> date | None:
    """Parse common source-system date formats.

    Day-first is the default for ambiguous slash dates because the synthetic
    operational sources are produced primarily by non-US systems. Month-first
    is still accepted as a fallback where a value cannot be day-first.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, float) and math.isnan(value):
        return None

    text = clean_text(value)
    if not text:
        return None

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_decimal(value: Any, *, percent: bool = False) -> float | None:
    """Parse numbers containing currency symbols, thousands separators or %.

    Returns ``None`` for blank values and raises ``ValueError`` for genuinely
    malformed values. That distinction lets the pipeline handle optional and
    critical fields differently.
    """
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (int, float, Decimal)):
        return float(value)

    text = clean_text(value)
    if not text:
        return None

    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1]
    if percent:
        text = text.rstrip("%")
    for token in ("$", "€", "£", ",", " "):
        text = text.replace(token, "")
    text = text.strip()

    if not _NUMBER_PATTERN.match(text):
        raise ValueError(f"Not a number: {value!r}")
    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:  # pragma: no cover - guarded by regex
        raise ValueError(f"Not a number: {value!r}") from exc
    return float(-parsed if negative else parsed)


def parse_positive_int(value: Any) -> int | None:
    number = parse_decimal(value)
    if number is None:
        return None
    if number <= 0 or number != int(number):
        raise ValueError(f"Not a positive integer: {value!r}")
    return int(number)


def parse_bool(value: Any, default: bool = False) -> bool:
    text = clean_text(value).lower()
    if not text:
        return default
    return text in {"1", "true", "t", "yes", "y", "secured"}


def date_to_key(value: date) -> int:
    return value.year * 10_000 + value.month * 100 + value.day


def as_json_safe(value: Any) -> Any:
    """Make arbitrary source values safe for a JSON audit payload."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)
