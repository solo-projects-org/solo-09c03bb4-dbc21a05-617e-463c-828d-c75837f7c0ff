"""SQLite persistence for the collection web service.

The database is shared by the two replicated web processes through a mounted
volume, so SQLite (WAL mode) plus ``BEGIN IMMEDIATE`` is the cross-process
concurrency boundary:

* only one RUNNING collection per (section, date) may exist;
* a late collection may never overwrite a published day whose source revision
  is newer (``StaleRevisionError``);
* publication replaces the whole day atomically -- partial pages never become
  visible.

The module deliberately depends only on the standard library so the demo
harness can exercise the stale-revision guard with ``python -I`` in-process.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS published (
    section      TEXT NOT NULL,
    date         TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    time         TEXT NOT NULL,
    nh3n         REAL NOT NULL,
    source_page  INTEGER NOT NULL,
    published_at TEXT NOT NULL,
    published_by TEXT NOT NULL,
    PRIMARY KEY (section, date, time)
);

CREATE TABLE IF NOT EXISTS runs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    section        TEXT NOT NULL,
    date           TEXT NOT NULL,
    worker         TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('running','success','failed','interrupted')),
    revision       INTEGER,
    rows_collected INTEGER NOT NULL DEFAULT 0,
    page_attempts  INTEGER NOT NULL DEFAULT 0,
    error          TEXT,
    started_at     TEXT NOT NULL,
    finished_at    TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_runs_active
    ON runs (section, date) WHERE status = 'running';
"""


class ActiveRunError(Exception):
    """Another collection for this section/day is still in progress."""


class StaleRevisionError(Exception):
    """A newer revision of the day is already published locally."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_conn(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path, isolation_level=None, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str) -> None:
    conn = get_conn(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


def recover_interrupted(db_path: str) -> int:
    """Mark runs left 'running' by a crashed/restarted worker."""
    conn = get_conn(db_path)
    try:
        cur = conn.execute(
            "UPDATE runs SET status='interrupted', "
            "error='worker restarted before collection finished', finished_at=? "
            "WHERE status='running'",
            (utc_now(),),
        )
        return cur.rowcount
    finally:
        conn.close()


def start_run(db_path: str, section: str, date: str, worker: str) -> int:
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "INSERT INTO runs (section, date, worker, status, started_at) "
                "VALUES (?, ?, ?, 'running', ?)",
                (section, date, worker, utc_now()),
            )
            conn.execute("COMMIT")
            return int(cur.lastrowid)
        except sqlite3.IntegrityError:
            conn.execute("ROLLBACK")
            raise ActiveRunError(
                f"a collection for {section} {date} is already running"
            )
    finally:
        conn.close()


def update_progress(
    db_path: str,
    run_id: int,
    revision: Optional[int],
    rows_collected: int,
    page_attempts: int,
) -> None:
    conn = get_conn(db_path)
    try:
        conn.execute(
            "UPDATE runs SET revision=?, rows_collected=?, page_attempts=? WHERE id=?",
            (revision, rows_collected, page_attempts, run_id),
        )
    finally:
        conn.close()


def fail_run(db_path: str, run_id: int, error: str) -> None:
    conn = get_conn(db_path)
    try:
        conn.execute(
            "UPDATE runs SET status='failed', error=?, finished_at=? WHERE id=?",
            (error[:500], utc_now(), run_id),
        )
    finally:
        conn.close()


def publish_version(
    db_path: str,
    section: str,
    date: str,
    revision: int,
    rows: list[dict],
    worker: str,
    run_id: int,
) -> dict:
    """Atomically replace the whole day's published version.

    Raises ``StaleRevisionError`` if a strictly newer source revision is
    already published. Re-publishing the same revision is idempotent.
    """
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT revision FROM published WHERE section=? AND date=? LIMIT 1",
            (section, date),
        ).fetchone()
        current = None if row is None else int(row["revision"])
        if current is not None and current > revision:
            conn.execute("ROLLBACK")
            raise StaleRevisionError(
                f"refusing to publish revision {revision}: revision {current} "
                f"is already published for {section} {date}"
            )

        now = utc_now()
        conn.execute(
            "DELETE FROM published WHERE section=? AND date=?", (section, date)
        )
        conn.executemany(
            "INSERT INTO published (section, date, revision, time, nh3n, "
            "source_page, published_at, published_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    section,
                    date,
                    revision,
                    r["time"],
                    float(r["nh3n"]),
                    int(r["source_page"]),
                    now,
                    worker,
                )
                for r in rows
            ],
        )
        conn.execute(
            "UPDATE runs SET status='success', revision=?, rows_collected=?, "
            "finished_at=?, error=NULL WHERE id=?",
            (revision, len(rows), now, run_id),
        )
        conn.execute("COMMIT")
        return {
            "action": "created" if current is None else
                      ("replaced" if current < revision else "unchanged_same_revision"),
            "previous_revision": current,
            "published_revision": revision,
            "rows": len(rows),
        }
    finally:
        conn.close()


def get_run(db_path: str, run_id: int) -> Optional[dict]:
    conn = get_conn(db_path)
    try:
        row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def list_runs(db_path: str, section: str, date: str) -> list[dict]:
    conn = get_conn(db_path)
    try:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM runs WHERE section=? AND date=? ORDER BY id DESC",
                (section, date),
            ).fetchall()
        ]
    finally:
        conn.close()


def get_published(db_path: str, section: str, date: str) -> Optional[dict]:
    conn = get_conn(db_path)
    try:
        rows = conn.execute(
            "SELECT time, nh3n, source_page, revision, published_at, published_by "
            "FROM published WHERE section=? AND date=? ORDER BY time",
            (section, date),
        ).fetchall()
        if not rows:
            return None
        first = rows[0]
        return {
            "section": section,
            "date": date,
            "revision": first["revision"],
            "published_at": first["published_at"],
            "published_by": first["published_by"],
            "rows": [
                {
                    "time": r["time"],
                    "nh3n": r["nh3n"],
                    "source_page": r["source_page"],
                    "revision": r["revision"],
                }
                for r in rows
            ],
        }
    finally:
        conn.close()
