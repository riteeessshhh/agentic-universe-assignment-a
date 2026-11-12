"""Unit tests for header, phone, date, decimal, enum, and timestamp normalization."""
from __future__ import annotations

import pytest

from solution.normalizers import (
    mask_phone,
    normalize_date,
    normalize_decimal,
    normalize_enum,
    normalize_header,
    normalize_phone,
    parse_timestamp_ist,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("customer_name", "customer name"),
        ("  Customer___Name  ", "customer name"),
        ("Mobile  No", "mobile no"),
        ("POLICY_NO", "policy no"),
        ("  order__id ", "order id"),
    ],
)
def test_normalize_header(raw: str, expected: str) -> None:
    assert normalize_header(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("9876543210", "+919876543210"),
        ("6000000000", "+916000000000"),
        ("7123456789", "+917123456789"),
        ("8123456789", "+918123456789"),
        ("+91 98765 43210", "+919876543210"),
        ("09876543210", "+919876543210"),
        ("91-77187-50036", "+917718750036"),
        ("(+91) 9876543210", "+919876543210"),
        ("+919876543210", "+919876543210"),
    ],
)
def test_normalize_phone_valid(raw: str, expected: str) -> None:
    assert normalize_phone(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "N/A",
        "987654321",          # 9 digits
        "5876543210",         # starts with 5
        "022-2345-6789",      # landline -> 10 digits starting with 2
        "+1 415 555 0134",    # 11 digits starting with 1
        "910987654321",       # 12 digits starting 91, next digit 0
        "00919876543210",     # 14 digits
    ],
)
def test_normalize_phone_invalid(raw: str) -> None:
    assert normalize_phone(raw) is None


def test_mask_phone() -> None:
    assert mask_phone("+919876543210") == "+91******3210"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("2026-10-15", "2026-10-15"),
        ("01/12/2026", "2026-12-01"),   # day-first!
        ("15-03-2026", "2026-03-15"),
        ("01-Dec-2026", "2026-12-01"),
        ("01-dec-2026", "2026-12-01"),
        ("29/02/2024", "2024-02-29"),   # leap year
    ],
)
def test_normalize_date_valid(raw: str, expected: str) -> None:
    assert normalize_date(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "31/02/2026",
        "29/02/2025",
        "2026-13-01",
        "12/25/2026",   # month 25 in day-first
        "soon",
        "TBD",
        "",
    ],
)
def test_normalize_date_invalid(raw: str) -> None:
    assert normalize_date(raw) is None


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("0", "0.00"),
        ("12345", "12345.00"),
        ("12345.678", "12345.68"),
        ("₹12,345", "12345.00"),
        ("₹ 1,23,456.7", "123456.70"),
        ("Rs. 4500", "4500.00"),
        ("rs 4500.125", "4500.13"),
        ("INR 999.9", "999.90"),
        ("inr100", "100.00"),
    ],
)
def test_normalize_decimal_valid(raw: str, expected: str) -> None:
    assert normalize_decimal(raw) == expected


@pytest.mark.parametrize("raw", ["N/A", "-", "twelve thousand", "-100", "₹ -5", "", "12.34.56"])
def test_normalize_decimal_invalid(raw: str) -> None:
    assert normalize_decimal(raw) is None


def test_normalize_enum() -> None:
    vals = ["en", "hi", "ta", "kn", "te", "mr"]
    assert normalize_enum(" HI ", vals) == "hi"
    assert normalize_enum("En", vals) == "en"
    assert normalize_enum("Hindi", vals) is None
    assert normalize_enum("english", vals) is None


def test_parse_timestamp_ist() -> None:
    # UTC crossing midnight into next IST date
    iso, us, d = parse_timestamp_ist("2026-10-01T20:00:00Z")
    assert iso == "2026-10-02T01:30:00+05:30"
    assert d == "2026-10-02"

    # Explicit IST offset
    iso2, us2, d2 = parse_timestamp_ist("2026-10-02T01:30:00+05:30")
    assert iso2 == "2026-10-02T01:30:00+05:30"
    assert us == us2
    assert d2 == "2026-10-02"

    # Naive -> IST
    iso3, us3, d3 = parse_timestamp_ist("2026-10-02 01:30:00")
    assert iso3 == "2026-10-02T01:30:00+05:30"
    assert us3 == us2
    assert d3 == "2026-10-02"

    assert parse_timestamp_ist("not-a-timestamp") is None

