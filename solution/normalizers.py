"""Field and timestamp normalization utilities for lead and event ingestion."""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

IST = timezone(timedelta(hours=5, minutes=30))
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)

_HEADER_SEP_RE = re.compile(r"[_\s]+")
_NON_DIGIT_RE = re.compile(r"\D+")

_DATE_ISO_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_DATE_SLASH_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
_DATE_DASH_RE = re.compile(r"^(\d{1,2})-(\d{1,2})-(\d{4})$")
_DATE_MON_RE = re.compile(r"^(\d{1,2})-([A-Za-z]{3})-(\d{4})$")

_MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

_DECIMAL_RE = re.compile(
    r"^(?:₹|rs\.?|inr)?\s*([+-]?(?:\d+(?:,\d+)*(?:\.\d+)?|\.\d+))$",
    re.IGNORECASE,
)


def normalize_header(header: str) -> str:
    """Lower-case, trim, and collapse runs of '_' or whitespace to a single space."""
    return _HEADER_SEP_RE.sub(" ", header.strip().lower()).strip()


def normalize_phone(raw: str) -> str | None:
    """Normalize an Indian mobile number to '+91XXXXXXXXXX', or return None if invalid."""
    digits = _NON_DIGIT_RE.sub("", raw)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) == 10 and digits[0] in ("6", "7", "8", "9"):
        return f"+91{digits}"
    return None


def mask_phone(canonical_phone: str) -> str:
    """Mask canonical phone '+91XXXXXXXXXX' as '+91******XXXX'."""
    return f"+91******{canonical_phone[-4:]}"


def normalize_date(raw: str) -> str | None:
    """Parse YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, or DD-Mon-YYYY (day-first) to YYYY-MM-DD."""
    s = raw.strip()
    if m := _DATE_ISO_RE.match(s):
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    elif m := _DATE_SLASH_RE.match(s):
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    elif m := _DATE_DASH_RE.match(s):
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    elif m := _DATE_MON_RE.match(s):
        mo = _MONTHS.get(m.group(2).lower())
        if mo is None:
            return None
        d, y = int(m.group(1)), int(m.group(3))
    else:
        return None
    try:
        return date(y, mo, d).isoformat()
    except ValueError:
        return None


def normalize_decimal(raw: str) -> str | None:
    """Parse optional currency prefix and thousands separators; require >= 0; round to 2 places."""
    s = raw.strip()
    m = _DECIMAL_RE.match(s)
    if not m:
        return None
    num_str = m.group(1).replace(",", "")
    try:
        val = Decimal(num_str)
    except InvalidOperation:
        return None
    if not val.is_finite() or val < 0:
        return None
    return str(val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def normalize_enum(raw: str, allowed_values: list[str]) -> str | None:
    """Compare case-insensitively against allowed_values and return lower-case value or None."""
    low = raw.strip().lower()
    for v in allowed_values:
        if v.lower() == low:
            return v.lower()
    return None


def parse_timestamp_ist(raw: str) -> tuple[str, int, str] | None:
    """
    Parse UTC ('...Z'), offset ('...+05:30'), or naive (assumed IST) timestamp.
    Returns (ist_iso_8601, epoch_microseconds, ist_date_yyyy_mm_dd) or None if invalid.
    """
    s = raw.strip()
    if not s:
        return None
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    else:
        dt = dt.astimezone(IST)
    delta = dt.astimezone(timezone.utc) - _EPOCH_UTC
    epoch_us = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    return dt.isoformat(), epoch_us, dt.date().isoformat()

