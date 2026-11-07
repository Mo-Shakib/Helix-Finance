from datetime import date

import pytest

from finance_dwh.quality import (
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


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2024-03-05", date(2024, 3, 5)),
        ("05/03/2024", date(2024, 3, 5)),
        ("31/12/2024", date(2024, 12, 31)),
        ("12/31/2024", date(2024, 12, 31)),
        ("05-Mar-2024", date(2024, 3, 5)),
        ("Mar 05, 2024", date(2024, 3, 5)),
        ("2024/03/05", date(2024, 3, 5)),
        ("2024-03-05 14:30:00", date(2024, 3, 5)),
        ("not-a-date", None),
        ("31/02/2025", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_mixed_date(raw, expected):
    assert parse_mixed_date(raw) == expected


def test_normalization_helpers():
    assert clean_text("  Maya   Chen  ") == "Maya Chen"
    assert clean_text("N/A", default="Unknown") == "Unknown"
    assert normalize_id(" cust-0012 ") == "CUST-0012"
    assert normalize_email("  Maya.Chen@Example.COM ") == "maya.chen@example.com"
    assert is_valid_email("maya@example.com")
    assert not is_valid_email("not-an-email")
    assert not is_valid_email("@example.com")


@pytest.mark.parametrize(
    ("raw", "percent", "expected"),
    [
        ("$1,234.50", False, 1234.5),
        (" 12,500 ", False, 12500.0),
        ("7.25%", True, 7.25),
        ("(30.00)", False, -30.0),
        ("0", False, 0.0),
        ("", False, None),
    ],
)
def test_parse_decimal(raw, percent, expected):
    assert parse_decimal(raw, percent=percent) == expected


def test_parse_decimal_rejects_garbage():
    with pytest.raises(ValueError):
        parse_decimal("twelve dollars")
    with pytest.raises(ValueError):
        parse_decimal("12.5.6")


def test_integer_and_boolean_parsing():
    assert parse_positive_int("36") == 36
    assert parse_positive_int("36.0") == 36
    assert parse_positive_int("") is None
    with pytest.raises(ValueError):
        parse_positive_int("0")
    assert parse_bool("Yes") is True
    assert parse_bool("false") is False
    assert parse_bool(None, default=True) is True


def test_date_key():
    assert date_to_key(date(2026, 10, 31)) == 20261031
