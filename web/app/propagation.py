"""Propagation-tracing service: one committed snapshot -> one verdict.

The analysis is only conclusive when all three required day archives exist
(upstream D, downstream D, downstream D+1).  The three archives are read in a
single SQLite read transaction, so they belong to one committed state even if
a second web process publishes concurrently.

Immediately after computing, the publication sequences are re-read.  If any
archive was replaced during the analysis the stored row is marked *stale* --
the numbers stay available as a historical computation but are never
presented as a current conclusion.  The same freshness check is repeated
whenever a stored analysis is opened again.
"""
from __future__ import annotations

import hashlib
import json
from typing import Optional

from . import analysis as algo
from . import db


def _rows_digest(rows: list[dict]) -> str:
    raw = json.dumps(
        [[r["time"], r["nh3n"], r["revision"]] for r in rows],
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def _archive_summary(archives: list[dict]) -> list[dict]:
    return [
        {
            "role": a["role"],
            "section": a["section"],
            "date": a["date"],
            "sequence": a["sequence"],
            "revision": a["revision"],
            "published_at": a["published_at"],
            "published_by": a["published_by"],
            "rows": len(a["rows"]),
            "rows_digest": _rows_digest(a["rows"]),
        }
        for a in archives
    ]


def _public_archive(archive: dict) -> dict:
    return {
        "role": archive["role"],
        "section": archive["section"],
        "date": archive["date"],
        "sequence": archive["sequence"],
        "revision": archive["revision"],
        "published_at": archive["published_at"],
        "published_by": archive["published_by"],
        "row_count": len(archive["rows"]),
        "rows_digest": _rows_digest(archive["rows"]),
    }


def _stored_snapshot(archives: list[dict]) -> list[dict]:
    return _archive_summary(archives)


def analyze(
    db_path: str,
    upstream_section: str,
    downstream_section: str,
    date: str,
    threshold: float,
    max_lag_hours: int,
    _after_compute: Optional[object] = None,
    _between_reads: Optional[object] = None,
) -> dict:
    """Run and persist one propagation check.  Never raises on missing data.

    ``_after_compute`` is a test seam: a zero-argument callable invoked after
    the match is computed but before the publication sequences are re-checked.
    ``_between_reads`` is forwarded to ``db.read_archives_together`` and runs
    between archive reads inside the snapshot transaction.  The demo uses
    both to prove stale detection and tear-free reads deterministically.
    """
    try:
        algo.parse_date(date)
    except algo.AnalysisInputError as exc:
        raise ValueError(str(exc)) from exc
    if not (threshold > 0):
        raise ValueError("突升阈值必须是正数")
    if not (1 <= max_lag_hours <= 6):
        raise ValueError("最大滞后必须在 1～6 小时之间")

    next_day = algo.next_date(date)
    read = db.read_archives_together(
        db_path, upstream_section, downstream_section, date, next_day,
        _between_reads=_between_reads,
    )
    archives = read["archives"]
    missing = read["missing"]

    if missing:
        message = "无法判断：缺少所需日档 —— " + "；".join(missing)
        analysis_id = db.save_analysis(
            db_path,
            upstream_section=upstream_section,
            downstream_section=downstream_section,
            date=date,
            threshold=threshold,
            max_lag=max_lag_hours,
            status="indeterminate",
            snapshot_token=read["snapshot_token"] or "incomplete",
            snapshot=_stored_snapshot(archives),
            archived={"missing": missing},
            result=None,
            message=message,
        )
        return get_analysis(db_path, analysis_id, recheck=False)

    by_role = {a["role"]: a for a in archives}
    try:
        result = algo.analyze_days(
            by_role["upstream"]["rows"],
            by_role["downstream"]["rows"],
            by_role["downstream_next"]["rows"],
            date,
            threshold,
            max_lag_hours,
        )
    except algo.AnalysisInputError as exc:
        message = f"无法判断：日档内容不完整 —— {exc}"
        analysis_id = db.save_analysis(
            db_path,
            upstream_section=upstream_section,
            downstream_section=downstream_section,
            date=date,
            threshold=threshold,
            max_lag=max_lag_hours,
            status="indeterminate",
            snapshot_token=read["snapshot_token"],
            snapshot=_stored_snapshot(archives),
            archived={"error": str(exc)},
            result=None,
            message=message,
        )
        return get_analysis(db_path, analysis_id, recheck=False)

    # Annotate every pair with the exact archive revision it drew from.
    for match in result["matches"]:
        match["upstream_revision"] = by_role["upstream"]["revision"]
        match["upstream_sequence"] = by_role["upstream"]["sequence"]
        if match["downstream_date"] == date:
            used = by_role["downstream"]
        else:
            used = by_role["downstream_next"]
        match["downstream_revision"] = used["revision"]
        match["downstream_sequence"] = used["sequence"]

    # Did either web process publish a new version while we were computing?
    if _after_compute is not None:
        _after_compute()
    stale_reasons: list[str] = []
    try:
        db.check_snapshot_current(db_path, archives)
    except db.SnapshotConflictError as exc:
        stale_reasons = [str(exc)]

    if stale_reasons:
        status = "stale"
        message = (
            "结果已过期：分析期间日档来源换版，以下配对仅反映旧快照，"
            "不能作为当前结论。请重新分析。"
        )
    else:
        status = "conclusive"
        if result["match_count"]:
            message = (
                f"发现 {result['match_count']} 组传播配对，"
                f"总滞后 {result['total_lag_hours']} 小时。"
            )
        else:
            message = (
                "三份日档齐全，但在给定阈值与最大滞后内未找到传播配对；"
                "这不等于证明不存在传播，可调整阈值或滞后后重试。"
            )

    analysis_id = db.save_analysis(
        db_path,
        upstream_section=upstream_section,
        downstream_section=downstream_section,
        date=date,
        threshold=threshold,
        max_lag=max_lag_hours,
        status=status,
        snapshot_token=read["snapshot_token"],
        snapshot=_stored_snapshot(archives),
        archived={"stale_reasons": stale_reasons, "missing": []},
        result=result,
        message=message,
    )
    return get_analysis(db_path, analysis_id, recheck=False)


def list_recent(db_path: str, limit: int = 30) -> list[dict]:
    """Recent analyses with their *current* status (stale is computed live)."""
    rows = db.list_analyses(db_path, limit)
    for row in rows:
        stored = get_analysis(db_path, row["id"])
        if stored is not None:
            row["status"] = stored["status"]
            row["message"] = stored["message"]
    return rows


def get_analysis(
    db_path: str, analysis_id: int, recheck: bool = True
) -> Optional[dict]:
    stored = db.get_analysis(db_path, analysis_id)
    if stored is None:
        return None

    archives = stored["snapshot_json"] or []
    stale_reasons: list[str] = []
    if stored["status"] == "stale":
        archived = stored["archived_json"] or {}
        stale_reasons = list(archived.get("stale_reasons") or [])
    elif stored["status"] == "conclusive" and recheck:
        try:
            db.check_snapshot_current(db_path, archives)
        except db.SnapshotConflictError as exc:
            stale_reasons = [str(exc)]

    effective_status = "stale" if stale_reasons else stored["status"]
    message = stored["message"]
    if stale_reasons and stored["status"] != "stale":
        message = (
            "结果已过期：这些日档在分析之后已换版，以下配对不再代表当前状态，"
            "不能作为当前结论。请重新分析。"
        )

    archived = stored["archived_json"] or {}
    return {
        "id": stored["id"],
        "upstream_section": stored["upstream_section"],
        "downstream_section": stored["downstream_section"],
        "date": stored["date"],
        "threshold": stored["threshold"],
        "max_lag_hours": stored["max_lag_hours"],
        "status": effective_status,
        "recorded_status": stored["status"],
        "message": message,
        "snapshot_token": stored["snapshot_token"],
        "created_at": stored["created_at"],
        "archives": archives,
        "missing": archived.get("missing", []),
        "stale_reasons": stale_reasons,
        "result": stored["result_json"],
    }
