"""Deterministic synthetic hourly ammonia-nitrogen (NH3-N) observations.

A real county station would serve measurements from its own database. For the
local mock we generate 24 hourly rows per section/day deterministically, so
demonstrations are reproducible.
"""
from __future__ import annotations

import math
import os

DEFAULT_SECTIONS = ["SECTION-A", "SECTION-B", "SECTION-C"]


def sections() -> list[str]:
    raw = os.environ.get("STATION_SECTIONS", "")
    if raw.strip():
        return [s.strip() for s in raw.split(",") if s.strip()]
    return list(DEFAULT_SECTIONS)


def generate_rows(date: str) -> list[dict]:
    """Return 24 UTC hourly NH3-N rows (mg/L) for the given date."""
    # Touch `date` so generation is formally per-day while staying deterministic.
    day_ordinal = sum(int(p) for p in date.split("-"))
    rows = []
    for hour in range(24):
        value = 0.15 + 0.035 * hour + 0.08 * math.sin((hour + day_ordinal) / 3.0)
        rows.append({"time": f"{hour:02d}:00", "nh3n": round(value, 3)})
    return rows
