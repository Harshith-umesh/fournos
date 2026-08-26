"""Parse Go-style duration strings (e.g. "12h", "30m", "7d", "1h30m") into timedelta."""

from __future__ import annotations

import re
from datetime import timedelta

_DURATION_RE = re.compile(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$")


def parse_duration(value: str) -> timedelta | None:
    """Parse a Go-style duration string into a timedelta.

    Supports days (d), hours (h), minutes (m), and seconds (s).
    Returns None if the string is empty or does not match.
    """
    value = value.strip()
    if not value:
        return None

    m = _DURATION_RE.match(value)
    if not m or not any(m.groups()):
        return None

    days = int(m.group(1) or 0)
    hours = int(m.group(2) or 0)
    minutes = int(m.group(3) or 0)
    seconds = int(m.group(4) or 0)

    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
