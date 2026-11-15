"""Local mock of the county station paginated observation API.

Behaviour modelled here (see README):

* ``GET /metadata``          -> current revision + total rows for a day
* ``GET /observations``      -> one page + opaque next cursor
* ``POST /admin/correct``    -> station corrects a record: revision increases,
                                old cursors become invalid (409 on reuse)
* ``POST /admin/reset``      -> restore the day to revision 1
* ``POST /admin/fail-next``  -> next page read returns HTTP 500 once
* ``POST /admin/latency``    -> tune per-page simulated latency

Every page carries the current ``revision`` and ``total_rows``. A cursor
embeds the revision it was minted for; after a correction the server rejects
stale cursors so a collector cannot silently mix pages from two revisions.
"""
from __future__ import annotations

import base64
import json
import os
import threading
import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import data

PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "8"))
DEFAULT_LATENCY_MS = int(os.environ.get("PAGE_DELAY_MS", "350"))

app = FastAPI(title="County station mock API", version="1.0")
_lock = threading.Lock()

# day_key -> {"revision": int, "rows": [...], "fail_once": bool}
_days: dict[str, dict] = {}


def _day_key(section: str, date: str) -> str:
    return f"{section}|{date}"


def _get_day(section: str, date: str) -> dict:
    """Lazily create a day's dataset at revision 1."""
    key = _day_key(section, date)
    day = _days.get(key)
    if day is None:
        day = {"revision": 1, "rows": data.generate_rows(date), "fail_once": False}
        _days[key] = day
    return day


def _encode_cursor(section: str, date: str, revision: int, next_page: int) -> str:
    payload = {"s": section, "d": date, "r": revision, "p": next_page}
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode()


def _decode_cursor(cursor: str) -> dict:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        payload = json.loads(raw)
        if not all(k in payload for k in ("s", "d", "r", "p")):
            raise ValueError
        return payload
    except Exception:
        raise HTTPException(status_code=400, detail="invalid cursor")


class CorrectBody(BaseModel):
    section: str = Field(..., examples=["SECTION-A"])
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$", examples=["2026-11-14"])
    time: str = Field(..., pattern=r"^\d{2}:\d{2}$", examples=["08:00"])
    nh3n: float = Field(..., gt=0)


class FailNextBody(BaseModel):
    section: str
    date: str


class LatencyBody(BaseModel):
    delay_ms: int = Field(..., ge=0, le=10000)


class ResetBody(BaseModel):
    section: str
    date: str


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/sections")
def list_sections() -> dict:
    return {"sections": data.sections()}


@app.get("/metadata")
def metadata(section: str, date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$")) -> dict:
    with _lock:
        day = _get_day(section, date)
        return {
            "section": section,
            "date": date,
            "revision": day["revision"],
            "total_rows": len(day["rows"]),
            "page_size": PAGE_SIZE,
        }


@app.get("/observations")
def observations(
    section: str,
    date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    cursor: Optional[str] = Query(None),
) -> dict:
    # Resolve the requested page before sleeping so invalid requests fail fast.
    with _lock:
        day = _get_day(section, date)
        if cursor is None:
            page = 1
            cursor_revision = day["revision"]
        else:
            token = _decode_cursor(cursor)
            if token["s"] != section or token["d"] != date:
                raise HTTPException(status_code=400, detail="cursor does not match section/date")
            if token["r"] != day["revision"]:
                # Correction happened after this cursor was handed out.
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "stale_cursor",
                        "cursor_revision": token["r"],
                        "current_revision": day["revision"],
                    },
                )
            page = token["p"]
            cursor_revision = token["r"]
        total_pages = (len(day["rows"]) + PAGE_SIZE - 1) // PAGE_SIZE
        if not (1 <= page <= total_pages):
            raise HTTPException(status_code=400, detail="page out of range")
        should_fail = day["fail_once"]
        day["fail_once"] = False
        snapshot_revision = day["revision"]
        total_rows = len(day["rows"])
        start = (page - 1) * PAGE_SIZE
        rows = [dict(r) for r in day["rows"][start:start + PAGE_SIZE]]

    # Simulated network/instrument latency outside the lock.
    time.sleep(DEFAULT_LATENCY_MS / 1000.0)

    if should_fail:
        raise HTTPException(status_code=500, detail="simulated transient read failure")

    next_cursor = None
    if page < total_pages:
        next_cursor = _encode_cursor(section, date, snapshot_revision, page + 1)
    return {
        "section": section,
        "date": date,
        "revision": snapshot_revision,
        "total_rows": total_rows,
        "page": page,
        "page_size": PAGE_SIZE,
        "next_cursor": next_cursor,
        "rows": [
            {
                "time": r["time"],
                "nh3n": r["nh3n"],
                "source_page": page,
            }
            for r in rows
        ],
    }


@app.post("/admin/correct")
def correct(body: CorrectBody) -> dict:
    """Station corrects a record: bump revision, old cursors die."""
    with _lock:
        day = _get_day(body.section, body.date)
        target = next((r for r in day["rows"] if r["time"] == body.time), None)
        if target is None:
            raise HTTPException(status_code=404, detail=f"no row at {body.time}")
        old_value = target["nh3n"]
        target["nh3n"] = round(body.nh3n, 3)
        day["revision"] += 1
        day["fail_once"] = False
        return {
            "section": body.section,
            "date": body.date,
            "revision": day["revision"],
            "corrected": {"time": body.time, "old_nh3n": old_value, "new_nh3n": target["nh3n"]},
        }


@app.post("/admin/reset")
def reset(body: ResetBody) -> dict:
    with _lock:
        _days.pop(_day_key(body.section, body.date), None)
        day = _get_day(body.section, body.date)
        return {
            "section": body.section,
            "date": body.date,
            "revision": day["revision"],
            "total_rows": len(day["rows"]),
        }


@app.post("/admin/fail-next")
def fail_next(body: FailNextBody) -> dict:
    with _lock:
        day = _get_day(body.section, body.date)
        day["fail_once"] = True
        return {"section": body.section, "date": body.date, "fail_next_read": True}


@app.post("/admin/latency")
def latency(body: LatencyBody) -> dict:
    global DEFAULT_LATENCY_MS
    DEFAULT_LATENCY_MS = body.delay_ms
    return {"delay_ms": DEFAULT_LATENCY_MS}
