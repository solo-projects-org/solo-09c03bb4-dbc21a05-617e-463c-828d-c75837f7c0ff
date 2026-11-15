"""Page-by-page collection with revision pinning and all-or-nothing publish.

Guarantees implemented here:

1. The revision seen on the first page is pinned. Every subsequent page must
   carry the same revision; the station's stale-cursor 409 or a mismatched
   revision aborts the run (the previously published version stays intact).
2. Each row must belong to the requested UTC date and have a unique hour;
   duplicates abort the run.
3. The number of collected rows must equal the station's ``total_rows``.
4. After the last page, metadata is polled again to confirm the source
   revision never changed; only then is the full day published atomically.
5. Timeouts / 5xx / network errors retry the *current* page only (the cursor
   itself is not advanced until the page is accepted).
"""
from __future__ import annotations

import logging
import re
import time
from datetime import date as date_cls

from . import db
from .station_client import (
    StationClient,
    StationError,
    StationReadError,
    StaleCursorError,
)

log = logging.getLogger("collector")

MAX_PAGE_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 0.5
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME_RE = re.compile(r"^\d{2}:\d{2}$")


class CollectionError(Exception):
    """Permanent failure of a collection run."""


def _validate_date(date_str: str) -> None:
    if not DATE_RE.match(date_str):
        raise CollectionError("date must be YYYY-MM-DD")
    try:
        date_cls.fromisoformat(date_str)
    except ValueError as exc:
        raise CollectionError(f"invalid calendar date: {date_str}") from exc


def _check_rows(date_str: str, seen_times: set[str], page_rows: list[dict]) -> None:
    for row in page_rows:
        t = row.get("time")
        if not isinstance(t, str) or not TIME_RE.match(t):
            raise CollectionError(f"row has invalid time: {row!r}")
        if t not in {f"{h:02d}:00" for h in range(24)}:
            raise CollectionError(f"row time {t} is not an on-the-hour UTC time for {date_str}")
        if t in seen_times:
            raise CollectionError(f"duplicate hour {t} across pages for {date_str}")
        if not isinstance(row.get("nh3n"), (int, float)):
            raise CollectionError(f"row has non-numeric nh3n: {row!r}")
        if not isinstance(row.get("source_page"), int):
            raise CollectionError(f"row is missing source_page: {row!r}")
        seen_times.add(t)


def collect_day(
    db_path: str,
    run_id: int,
    section: str,
    date_str: str,
    client: StationClient,
    worker: str,
) -> dict:
    """Run one full collection. Raises on permanent failure."""
    _validate_date(date_str)

    meta = client.metadata(section, date_str)
    expected_total = int(meta["total_rows"])
    pinned_revision = int(meta["revision"])
    log.info(
        "run %s starting: section=%s date=%s revision=%s total=%s",
        run_id, section, date_str, pinned_revision, expected_total,
    )

    cursor = None
    collected: list[dict] = []
    seen_times: set[str] = set()
    attempts = 0
    expected_page = 1

    while True:
        # Retry only the current page; cursor advances only on accepted pages.
        for attempt in range(1, MAX_PAGE_ATTEMPTS + 1):
            attempts += 1
            try:
                body, next_cursor = client.fetch_page(section, date_str, cursor)
                break
            except StaleCursorError as exc:
                raise CollectionError(
                    f"station correction detected via stale cursor: {exc}; "
                    "keeping previously published version"
                ) from exc
            except StationReadError as exc:
                log.warning("run %s page read attempt %s failed: %s", run_id, attempt, exc)
                if attempt == MAX_PAGE_ATTEMPTS:
                    raise CollectionError(
                        f"page read failed after {MAX_PAGE_ATTEMPTS} attempts: {exc}; "
                        "keeping previously published version"
                    ) from exc
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)

        page_revision = int(body["revision"])
        if page_revision != pinned_revision:
            raise CollectionError(
                f"revision changed mid-collection: pinned {pinned_revision}, "
                f"page reports {page_revision}; keeping previously published version"
            )
        if int(body["total_rows"]) != expected_total:
            raise CollectionError(
                f"total_rows changed: {expected_total} -> {body['total_rows']}"
            )
        if int(body["page"]) != expected_page:
            raise CollectionError(
                f"page sequence broken: expected {expected_page}, got {body['page']}"
            )

        page_rows = body.get("rows", [])
        _check_rows(date_str, seen_times, page_rows)
        for r in page_rows:
            collected.append(
                {
                    "time": r["time"],
                    "nh3n": float(r["nh3n"]),
                    "source_page": int(r["source_page"]),
                }
            )

        db.update_progress(db_path, run_id, pinned_revision, len(collected), attempts)

        if next_cursor is None:
            break
        cursor = next_cursor
        expected_page += 1

    # All pages read: verify completeness.
    if len(collected) != expected_total:
        raise CollectionError(
            f"row count mismatch: collected {len(collected)}, total_rows {expected_total}"
        )

    # Final confirmation that the source revision never changed.
    final_meta = client.metadata(section, date_str)
    if int(final_meta["revision"]) != pinned_revision:
        raise CollectionError(
            f"station revision changed before publish: pinned {pinned_revision}, "
            f"now {final_meta['revision']}; keeping previously published version"
        )
    if int(final_meta["total_rows"]) != expected_total:
        raise CollectionError("station total_rows changed before publish")

    try:
        result = db.publish_version(
            db_path, section, date_str, pinned_revision, collected, worker, run_id
        )
    except db.StaleRevisionError as exc:
        # Re-mark the run failed: a concurrently published newer revision wins.
        db.fail_run(db_path, run_id, str(exc))
        raise CollectionError(str(exc)) from exc

    log.info("run %s published: %s", run_id, result)
    return result
