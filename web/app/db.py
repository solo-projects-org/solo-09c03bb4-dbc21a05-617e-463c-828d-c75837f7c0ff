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

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

HOUR_TIMES = tuple(f"{h:02d}:00" for h in range(24))

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

-- One row per published day.  ``sequence`` is a monotonic publication id:
-- even republishing the *same* revision with identical values creates a new
-- sequence.  Readers compare sequence ids to prove whether a day changed
-- underneath an analysis snapshot.
CREATE TABLE IF NOT EXISTS published_versions (
    sequence     INTEGER PRIMARY KEY AUTOINCREMENT,
    section      TEXT NOT NULL,
    date         TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    published_at TEXT NOT NULL,
    published_by TEXT NOT NULL,
    row_count    INTEGER NOT NULL,
    UNIQUE (section, date)
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

-- Propagation checks.  ``status`` is computed at request time; the raw
-- evidence (archive sequence ids, parameters, match result) is stored so the
-- page can be reopened without recomputing against newer publications.
CREATE TABLE IF NOT EXISTS analyses (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    upstream_section    TEXT NOT NULL,
    downstream_section  TEXT NOT NULL,
    date                TEXT NOT NULL,
    threshold           REAL NOT NULL,
    max_lag_hours       INTEGER NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('conclusive','indeterminate','stale')),
    snapshot_json       TEXT NOT NULL,
    snapshot_token      TEXT NOT NULL,
    archived_json       TEXT,
    result_json         TEXT,
    message             TEXT NOT NULL,
    created_at          TEXT NOT NULL
);
"""


class ActiveRunError(Exception):
    """Another collection for this section/day is still in progress."""


class StaleRevisionError(Exception):
    """A newer revision of the day is already published locally."""


class SnapshotConflictError(Exception):
    """The three archives do not form one committed state (or changed)."""


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
    already published. Re-publishing the same revision is allowed but still
    produces a new publication ``sequence`` so concurrent readers can detect
    that the archived day was rewritten.
    """
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        version = conn.execute(
            "SELECT sequence, revision FROM published_versions "
            "WHERE section=? AND date=?",
            (section, date),
        ).fetchone()
        current = None if version is None else int(version["revision"])
        if current is None:
            # Backfill databases published before published_versions existed.
            legacy = conn.execute(
                "SELECT revision, published_at, published_by, COUNT(*) AS n "
                "FROM published WHERE section=? AND date=?",
                (section, date),
            ).fetchone()
            if legacy is not None and int(legacy["n"]) > 0:
                current = int(legacy["revision"])
                conn.execute(
                    "INSERT INTO published_versions (section, date, revision, "
                    "published_at, published_by, row_count) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        section,
                        date,
                        current,
                        legacy["published_at"],
                        legacy["published_by"],
                        int(legacy["n"]),
                    ),
                )
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
            "DELETE FROM published_versions WHERE section=? AND date=?",
            (section, date),
        )
        conn.execute(
            "INSERT INTO published_versions (section, date, revision, "
            "published_at, published_by, row_count) VALUES (?, ?, ?, ?, ?, ?)",
            (section, date, revision, now, worker, len(rows)),
        )
        conn.execute(
            "UPDATE runs SET status='success', revision=?, rows_collected=?, "
            "finished_at=?, error=NULL WHERE id=?",
            (revision, len(rows), now, run_id),
        )
        conn.execute("COMMIT")
        new_version = conn.execute(
            "SELECT sequence FROM published_versions WHERE section=? AND date=?",
            (section, date),
        ).fetchone()
        return {
            "action": "created" if current is None else
                      ("replaced" if current <= revision else "refused"),
            "previous_revision": current,
            "published_revision": revision,
            "sequence": int(new_version["sequence"]) if new_version else None,
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
        return _published_from_conn(conn, section, date)
    finally:
        conn.close()


def _published_from_conn(
    conn: sqlite3.Connection, section: str, date: str
) -> Optional[dict]:
    rows = conn.execute(
        "SELECT time, nh3n, source_page, revision, published_at, published_by "
        "FROM published WHERE section=? AND date=? ORDER BY time",
        (section, date),
    ).fetchall()
    if not rows:
        return None
    first = rows[0]
    version = conn.execute(
        "SELECT sequence, revision, row_count, published_at, published_by "
        "FROM published_versions WHERE section=? AND date=?",
        (section, date),
    ).fetchone()
    if version is None:
        # Legacy database written before published_versions existed: expose a
        # synthetic, stable publication identity.  The first publish through
        # ``publish_version`` records a real (backfilled) sequence, which is
        # then detectable as a version change.
        version_info = {
            "sequence": 0,
            "revision": int(first["revision"]),
            "row_count": len(rows),
            "published_at": first["published_at"],
            "published_by": first["published_by"],
        }
    else:
        version_info = {
            "sequence": int(version["sequence"]),
            "revision": int(version["revision"]),
            "row_count": int(version["row_count"]),
            "published_at": version["published_at"],
            "published_by": version["published_by"],
        }
    return {
        "section": section,
        "date": date,
        "revision": first["revision"],
        "published_at": first["published_at"],
        "published_by": first["published_by"],
        "sequence": version_info["sequence"],
        "version": version_info,
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


def read_day_snapshot(
    db_path: str, section: str, date: str
) -> Optional[dict]:
    """Read one published day plus its publication sequence in a read txn.

    Returns ``None`` when no complete day is published.  The returned
    ``sequence`` identifies the exact committed publication so callers can
    prove consistency across several reads.
    """
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN")
        result = _published_from_conn(conn, section, date)
        conn.execute("COMMIT")
        return result
    finally:
        conn.close()


def read_archives_together(
    db_path: str,
    upstream_section: str,
    downstream_section: str,
    date: str,
    next_date: str,
    _between_reads: Optional[object] = None,
) -> dict:
    """Read the three required day archives from one committed state.

    All three reads happen inside a single ``BEGIN`` (a read transaction in
    WAL mode sees one consistent database snapshot), so a publication that
    commits concurrently is either wholly visible or wholly invisible.

    ``_between_reads`` is a test seam: a zero-argument callable invoked after
    the first archive is read, still inside the read transaction.  The demo
    uses it to publish new versions mid-read and prove they cannot tear the
    snapshot.

    Returns ``{"archives": [...], "missing": [...], "snapshot_token": ...}``.
    ``missing`` lists human-readable reasons for each unavailable archive;
    callers must report those as "cannot determine", never as "no
    propagation".
    """
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN")
        upstream = _published_from_conn(conn, upstream_section, date)
        if _between_reads is not None:
            _between_reads()
        downstream = _published_from_conn(conn, downstream_section, date)
        downstream_next = _published_from_conn(conn, downstream_section, next_date)
        conn.execute("COMMIT")
    finally:
        conn.close()

    missing = []
    if upstream is None:
        missing.append(f"上游断面 {upstream_section} 在 {date}（UTC）无完整已发布日档")
    if downstream is None:
        missing.append(f"下游断面 {downstream_section} 在 {date}（UTC）无完整已发布日档")
    if downstream_next is None:
        missing.append(
            f"下游断面 {downstream_section} 在 {next_date}（UTC，次日）无完整已发布日档"
        )

    archives = []
    for role, section, day_date, day in (
        ("upstream", upstream_section, date, upstream),
        ("downstream", downstream_section, date, downstream),
        ("downstream_next", downstream_section, next_date, downstream_next),
    ):
        if day is None:
            continue
        if day.get("sequence") is None or len(day["rows"]) != 24:
            missing.append(
                f"{role} 日档 {section} {day_date} 不是一次完整发表的 24 行版本"
            )
            continue
        archives.append(
            {
                "role": role,
                "section": section,
                "date": day_date,
                "sequence": day["sequence"],
                "revision": day["revision"],
                "rows": day["rows"],
                "published_at": day["published_at"],
                "published_by": day["published_by"],
            }
        )

    token = snapshot_token(archives) if archives and not missing else None
    return {"archives": archives, "missing": missing, "snapshot_token": token}


def snapshot_token(archives: list[dict]) -> str:
    """Stable digest of the three archive identities."""
    payload = [
        [a["role"], a["section"], a["date"], a["sequence"], a["revision"]]
        for a in archives
    ]
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def check_snapshot_current(db_path: str, archives: list[dict]) -> list[dict]:
    """Re-read each archive's publication sequence and report any change.

    Raises ``SnapshotConflictError`` describing exactly which archive was
    replaced so the API can mark a stored result as stale.
    """
    changed = []
    for archive in archives:
        current = read_day_snapshot(db_path, archive["section"], archive["date"])
        if current is None:
            changed.append(
                f"{archive['role']} 日档已被移除（{archive['section']} {archive['date']}）"
            )
        elif current.get("sequence") != archive["sequence"]:
            changed.append(
                f"{archive['role']} 日档已换版（{archive['section']} {archive['date']} "
                f"sequence {archive['sequence']} -> {current.get('sequence')}，"
                f"修订号 {archive['revision']} -> {current['revision']}）"
            )
    if changed:
        raise SnapshotConflictError("；".join(changed))
    return changed


def save_analysis(
    db_path: str,
    *,
    upstream_section: str,
    downstream_section: str,
    date: str,
    threshold: float,
    max_lag: int,
    status: str,
    snapshot_token: str,
    snapshot: list[dict],
    archived: Optional[dict],
    result: Optional[dict],
    message: str,
) -> int:
    conn = get_conn(db_path)
    try:
        cur = conn.execute(
            "INSERT INTO analyses (upstream_section, downstream_section, date, "
            "threshold, max_lag_hours, status, snapshot_json, snapshot_token, "
            "archived_json, result_json, message, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                upstream_section,
                downstream_section,
                date,
                threshold,
                max_lag,
                status,
                json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")),
                snapshot_token,
                None if archived is None else json.dumps(
                    archived, ensure_ascii=False, separators=(",", ":")
                ),
                None if result is None else json.dumps(
                    result, ensure_ascii=False, separators=(",", ":")
                ),
                message,
                utc_now(),
            ),
        )
        return int(cur.lastrowid)
    finally:
        conn.close()


def list_analyses(db_path: str, limit: int = 30) -> list[dict]:
    conn = get_conn(db_path)
    try:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT id, upstream_section, downstream_section, date, threshold, "
                "max_lag_hours, status, snapshot_token, message, created_at "
                "FROM analyses ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        ]
    finally:
        conn.close()


def get_analysis(db_path: str, analysis_id: int) -> Optional[dict]:
    conn = get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM analyses WHERE id=?", (analysis_id,)
        ).fetchone()
        if row is None:
            return None
        out = dict(row)
        for key in ("snapshot_json", "archived_json", "result_json"):
            value = out.get(key)
            out[key] = None if value is None else json.loads(value)
        return out
    finally:
        conn.close()
