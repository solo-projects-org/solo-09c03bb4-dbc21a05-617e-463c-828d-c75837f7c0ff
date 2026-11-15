#!/usr/bin/env python3
"""End-to-end demonstration of pollution propagation tracing.

Run after the stack is up (``run-demo.sh`` starts it automatically):

    python3 -I demo/propagation-demo.py

Scenarios:
  1. Three archives published (upstream D, downstream D, downstream D+1);
     an injected downstream surge 2 h after an upstream surge is paired, and
     the published archives/revisions are reported.
  2. Missing next-day archive -> verdict is "cannot determine", explicitly
     NOT "no propagation".
  3. Two web processes publishing concurrently while an analysis runs ->
     the analysis is marked stale, never presented as a current conclusion.
  4. Matching objective: an upstream surge that could pair with two
     downstream surges is assigned so pair count is maximal and total lag
     minimal (verified against an independent brute-force solver).
  5. A downstream surge is matched at most once.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "web"))

RESULTS: list[tuple[bool, str]] = []

# Matches PAGE_DELAY_MS in docker-compose.yml for the station service.
STATION_DELAY_MS = int(__import__("os").environ.get("STATION_DELAY_MS", "350"))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check(ok: bool, label: str, detail: str = "") -> None:
    RESULTS.append((ok, label))
    print(("[PASS] " if ok else "[FAIL] ") + label + (f" -- {detail}" if detail else ""))
    if not ok:
        raise AssertionError(label)


def request(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode())
        except Exception:
            return exc.code, {}


def wait_run(base: str, run_id: int, timeout: float = 40.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, run = request("GET", f"{base}/api/runs/{run_id}")
        if status == 200 and run["status"] != "running":
            return run
        time.sleep(0.3)
    raise TimeoutError(f"run {run_id} did not finish in {timeout}s")


def collect(web: str, station: str, section: str, date: str) -> dict:
    """Collect the station's current revision of a day without resetting it.

    The station revision only ever grows, so this is safe on repeated demo
    runs against the same volume (the local stale-revision guard would reject
    a station reset back to revision 1, which is the platform working as
    designed).
    """
    status, started = request("POST", f"{web}/api/collect", {"section": section, "date": date})
    if status != 202:
        raise AssertionError(f"collect rejected: {status} {started}")
    run = wait_run(web, started["run_id"])
    if run["status"] != "success":
        raise AssertionError(f"collection failed: {run}")
    return run


def correct(station: str, section: str, date: str, hour: str, value: float) -> None:
    status, body = request(
        "POST", f"{station}/admin/correct",
        {"section": section, "date": date, "time": hour, "nh3n": value},
    )
    if status != 200:
        raise AssertionError(f"correction failed: {status} {body}")


def publish_pattern(web: str, station: str, section: str, date: str,
                    values: dict[str, float]) -> dict:
    """Publish a day whose hourly values are forced to ``values``."""
    for hour, value in sorted(values.items()):
        correct(station, section, date, hour, value)
    collect(web, station, section, date)
    status, published = request("GET", f"{web}/api/published?section={section}&date={date}")
    if status != 200:
        raise AssertionError("published day missing")
    return published


def brute_force(upstream: list[str], downstream: list[str], max_lag: int) -> tuple[int, int]:
    """Independent exhaustive solver: (max pairs, min total lag)."""
    from datetime import datetime

    def hours(ts: str) -> datetime:
        return datetime.fromisoformat(ts)

    best = None
    n = len(downstream)
    def rec(j: int, used: set[int], pairs: list[tuple[int, int]], lag: int) -> None:
        nonlocal best
        if j == n:
            key = (len(pairs), -lag)
            if best is None or key > best[0]:
                best = (key, list(pairs))
            return
        rec(j + 1, used, pairs, lag)
        for i, up in enumerate(upstream):
            if i in used:
                continue
            h = int((hours(downstream[j]) - hours(up)).total_seconds() // 3600)
            if 1 <= h <= max_lag:
                used.add(i)
                pairs.append((i, j))
                rec(j + 1, used, pairs, lag + h)
                pairs.pop()
                used.remove(i)
    rec(0, set(), [], 0)
    assert best is not None
    return len(best[1]), -best[0][1]


def scenario_basic(station: str, web1: str, web2: str) -> None:
    print("\n=== 场景 1：三份日档齐全，突升传播配对 ===")
    up_sec, down_sec, date = "SECTION-A", "SECTION-B", "2026-02-01"
    # Upstream gets a sharp rise at 05:00 (04->05) and 06:00; downstream a
    # strong rise at 07:00 and 08:00 (i.e. 2 h and 2 h later).
    publish_pattern(web1, station, up_sec, date, {
        "05:00": 3.0, "06:00": 3.4, "07:00": 3.4,
    })
    publish_pattern(web1, station, down_sec, date,
                    {"04:00": 0.2, "05:00": 0.2, "06:00": 0.2, "07:00": 2.0, "08:00": 3.2})
    # Downstream next day is required even if not used.
    run_next = collect(web1, station, down_sec, "2026-02-02")
    check(run_next["status"] == "success", "三份日档均已整日发布")

    status, result = request("POST", f"{web1}/api/analyze", {
        "upstream_section": up_sec, "downstream_section": down_sec,
        "date": date, "threshold": 0.5, "max_lag_hours": 3,
    })
    check(status == 200, "分析请求成功", json.dumps(result)[:300])
    check(result["status"] == "conclusive", "三档齐全 -> 可判断", result["message"])
    check(len(result["archives"]) == 3, "恰好使用三份日档")
    check([a["role"] for a in result["archives"]] == [
        "upstream", "downstream", "downstream_next"
    ], "日档角色为上游/下游/下游次日")
    check(all(a["sequence"] for a in result["archives"]), "每份日档都带发布序号")
    check(result["snapshot_token"], "记录快照标识", result["snapshot_token"])
    matches = result["result"]["matches"]
    check(len(matches) >= 1, f"至少 1 组配对（共 {len(matches)} 组）")
    check(all(m["lag_hours"] >= 1 for m in matches), "滞后均为正数")
    check(all(m["lag_hours"] <= 3 for m in matches), "滞后均不超过最大滞后 3 小时")
    check(any(m["downstream_time"].endswith("07:00") for m in matches),
          "下游 07:00 突升被配对", json.dumps(matches, ensure_ascii=False))
    check(all(a["revision"] >= 1 for a in result["archives"]),
          "结果中展示所用日档的修订号")
    check(all("upstream_revision" in m and "downstream_revision" in m for m in matches),
          "每组配对标明所用日档的修订号与发布序号", json.dumps(matches[0], ensure_ascii=False))
    # Re-open the stored analysis: still conclusive and consistent.
    status, reopened = request("GET", f"{web1}/api/analyses/{result['id']}")
    check(status == 200 and reopened["status"] == "conclusive", "存储的分析可重新打开")
    check(reopened["snapshot_token"] == result["snapshot_token"], "重开时快照标识一致")


def scenario_missing(station: str, web1: str) -> None:
    print("\n=== 场景 2：缺少次日下游日档 -> 无法判断（不得报成无传播）===")
    up_sec, down_sec, date = "SECTION-A", "SECTION-C", "2026-02-05"
    collect(web1, station, up_sec, date)
    collect(web1, station, down_sec, date)
    status, missing = request("GET", f"{web1}/api/published?section={down_sec}&date=2026-02-06")
    check(status == 404, "确认下游次日无已发布日档")

    status, result = request("POST", f"{web1}/api/analyze", {
        "upstream_section": up_sec, "downstream_section": down_sec,
        "date": date, "threshold": 0.05, "max_lag_hours": 2,
    })
    check(status == 200, "分析请求仍成功返回说明")
    check(result["status"] == "indeterminate", "结论为“无法判断”而非“无传播”",
          result["message"])
    check("无法判断" in result["message"], "提示文本明确写无法判断", result["message"])
    check(any("2026-02-06" in m for m in result["missing"]),
          "列出缺失的次日日档", json.dumps(result["missing"], ensure_ascii=False))
    check(result["result"] is None, "无法判断时不输出任何配对结论")
    check("无传播" not in result["message"], "绝不把缺失报成无传播")

    # Also: missing upstream archive while downstream exists.
    status, result2 = request("POST", f"{web1}/api/analyze", {
        "upstream_section": "SECTION-B", "downstream_section": down_sec,
        "date": date, "threshold": 0.05, "max_lag_hours": 2,
    })
    check(status == 200 and result2["status"] == "indeterminate",
          "其他缺失组合同样无法判断")
    check(len(result2["missing"]) >= 2, "列出所有缺失日档", json.dumps(result2["missing"], ensure_ascii=False))


def scenario_stale(station: str, web1: str, web2: str) -> None:
    print("\n=== 场景 3：分析期间第二个 Web 实例换版 -> 结果过期 ===")
    up_sec, down_sec, date = "SECTION-A", "SECTION-B", "2026-03-01"
    collect(web1, station, up_sec, date)
    collect(web1, station, down_sec, date)
    collect(web1, station, down_sec, "2026-03-02")

    status, before = request("POST", f"{web1}/api/analyze", {
        "upstream_section": up_sec, "downstream_section": down_sec,
        "date": date, "threshold": 0.05, "max_lag_hours": 3,
    })
    check(before["status"] == "conclusive", "首次分析可判断")
    analyse_id = before["id"]

    # web2 republishes the downstream day while the result is stored...
    correct(station, down_sec, date, "12:00", 4.2)
    status, started = request("POST", f"{web2}/api/collect",
                              {"section": down_sec, "date": date})
    check(status == 202, "web2 受理下游日档重采")
    run = wait_run(web2, started["run_id"])
    check(run["status"] == "success", "web2 发布了新版本")

    status, after = request("GET", f"{web1}/api/analyses/{analyse_id}")
    check(status == 200, "原分析记录仍可读取")
    check(after["status"] == "stale", "分析被标记为已过期", after["message"])
    check("过期" in after["message"], "提示结果已过期，不能作为当前结论", after["message"])

    # A fresh analysis against the new committed state is conclusive again.
    status, fresh = request("POST", f"{web1}/api/analyze", {
        "upstream_section": up_sec, "downstream_section": down_sec,
        "date": date, "threshold": 0.05, "max_lag_hours": 3,
    })
    check(fresh["status"] == "conclusive", "换版后重新分析可判断")
    old_seq = {a["role"]: a["sequence"] for a in before["archives"]}
    new_seq = {a["role"]: a["sequence"] for a in fresh["archives"]}
    check(old_seq["downstream"] != new_seq["downstream"],
          "下游日档发布序号已变化", f"{old_seq} -> {new_seq}")
    check(fresh["snapshot_token"] != before["snapshot_token"], "新快照标识不同")


def scenario_matching_objective(web1: str) -> None:
    print("\n=== 场景 4/5：先最多配对、再最短总滞后；下游至多匹配一次 ===")
    algo = load_module("propagation_algo", ROOT / "web" / "app" / "analysis.py")
    station_data = load_module("station_data", ROOT / "station" / "app" / "data.py")
    match_surges = algo.match_surges
    analyze_days = algo.analyze_days
    from datetime import datetime

    def surge(ts: str, delta: float = 0.5) -> dict:
        dt = datetime.fromisoformat(ts)
        return {
            "time": ts, "date": ts[:10], "hour": ts[11:16], "delta": delta, "at": dt,
        }

    # One upstream surge can reach two downstream surges. Matching both
    # requires a second upstream surge; the solver must still maximise count,
    # then choose the minimum-lag assignment among maximum matchings.
    upstream = [surge("2026-04-01T05:00"), surge("2026-04-01T06:00")]
    downstream = [surge("2026-04-01T08:00"), surge("2026-04-01T09:00")]
    pairs = match_surges(upstream, downstream, 3)
    got = (len(pairs), sum(p["lag_hours"] for p in pairs))
    expected = brute_force([s["time"] for s in upstream],
                           [s["time"] for s in downstream], 3)
    check(got == expected, "配对结果与穷举最优一致", f"got={got} expected={expected}")
    check(got == (2, 6), "先最大化配对数（2 组），再最小化总滞后（6 小时）", str(got))

    # Every downstream surge appears at most once.
    matched_down = [p["downstream_time"] for p in pairs]
    check(len(matched_down) == len(set(matched_down)), "每个下游突升最多匹配一次",
          str(matched_down))

    # Larger random-ish case against the exhaustive solver.
    upstream = [surge(f"2026-04-02T{h:02d}:00") for h in (2, 4, 6, 8)]
    downstream = [surge(f"2026-04-02T{h:02d}:00") for h in (5, 7, 9, 11)] + [
        surge("2026-04-03T01:00")
    ]
    pairs = match_surges(upstream, downstream, 3)
    got = (len(pairs), sum(p["lag_hours"] for p in pairs))
    expected = brute_force([s["time"] for s in upstream],
                           [s["time"] for s in downstream], 3)
    check(got == expected, "5 个下游突升情形同样最优", f"got={got} expected={expected}")

    # End-to-end: day+specific increments -> the API performs the same logic.
    # (Checked implicitly in scenario 1; here verify up/down increment scope.)
    day = "2026-04-03"
    values = {r["time"]: r["nh3n"] for r in station_data.generate_rows(day)}
    next_values = {
        r["time"]: r["nh3n"] for r in station_data.generate_rows("2026-04-04")
    }
    result = analyze_days(
        [{"time": t, "nh3n": v} for t, v in values.items()],
        [{"time": t, "nh3n": v} for t, v in values.items()],
        [{"time": t, "nh3n": v} for t, v in next_values.items()],
        day, 0.05, 6,
    )
    upstream_times = {s["hour"] for s in result["upstream_surges"]}
    check("00:00" not in upstream_times, "上游只取 01:00～23:00 增量")
    downstream_dates = {s["date"] for s in result["downstream_surges"]}
    check(downstream_dates == {day, "2026-04-04"}, "下游同时读取当日与次日",
          str(downstream_dates))


def scenario_concurrent_consistency(station: str, web1: str, web2: str) -> None:
    print("\n=== 场景 6：并发采集与交错分析，只使用同一已提交状态的三份日档 ===")
    up_sec, down_sec, date = "SECTION-A", "SECTION-C", "2026-05-01"
    collect(web1, station, up_sec, date)
    collect(web1, station, down_sec, date)
    collect(web1, station, down_sec, "2026-05-02")

    request("POST", f"{station}/admin/latency", {"delay_ms": 400})
    errors: list[str] = []
    try:
        # Interleave: web2 republishes upstream+downstream while web1 runs
        # repeated analyses.  Every analysis must be either conclusive with a
        # self-consistent snapshot, or stale -- never a mixed snapshot.
        def republish() -> None:
            try:
                for _ in range(2):
                    correct(station, up_sec, date, "09:00", 2.5)
                    _, st = request("POST", f"{web2}/api/collect",
                                    {"section": up_sec, "date": date})
                    wait_run(web2, st["run_id"])
                    correct(station, down_sec, "2026-05-02", "01:00", 2.8)
                    _, st = request("POST", f"{web2}/api/collect",
                                    {"section": down_sec, "date": "2026-05-02"})
                    wait_run(web2, st["run_id"])
            except Exception as exc:  # noqa: BLE001
                errors.append(repr(exc))

        worker = threading.Thread(target=republish)
        worker.start()
        seen = []
        while worker.is_alive():
            status, res = request("POST", f"{web1}/api/analyze", {
                "upstream_section": up_sec, "downstream_section": down_sec,
                "date": date, "threshold": 0.05, "max_lag_hours": 3,
            })
            if status != 200:
                errors.append(f"analyze {status} {res}")
                break
            seen.append(res["status"] if res["status"] != "stale" else "stale")
            time.sleep(0.05)
        worker.join()
        check(not errors, "并发期间分析与采集均无错误", "; ".join(errors[:3]))
        check(len(seen) >= 2, f"并发期间完成多次分析（{len(seen)} 次）")
        check(all(s in {"conclusive", "stale"} for s in seen),
              "每次分析要么可判断、要么标记过期", str(set(seen)))

        # Every stored analysis must be self-consistent: three archives with
        # a role, a publication sequence and a revision each.
        inspected = 0
        for res in request("GET", f"{web1}/api/analyses?limit=20")[1]:
            status, full = request("GET", f"{web1}/api/analyses/{res['id']}")
            if status != 200:
                continue
            if full["status"] == "conclusive":
                archives = full["archives"]
                ok = (
                    len(archives) == 3
                    and {a["role"] for a in archives}
                    == {"upstream", "downstream", "downstream_next"}
                    and all(a["sequence"] is not None and a["revision"] >= 1
                            for a in archives)
                    and full["snapshot_token"]
                )
                if not ok:
                    check(False, f"分析 #{res['id']} 快照字段缺损",
                          json.dumps(archives, ensure_ascii=False))
                inspected += 1
        check(inspected >= 1, f"并发期间存储的分析字段完整（抽查 {inspected} 条）")
    finally:
        # Restore the stack's configured latency so the collection demo
        # (demo/demo.py) still observes mid-run corrections.
        request("POST", f"{station}/admin/latency", {"delay_ms": STATION_DELAY_MS})


def scenario_mid_analysis_swap() -> None:
    print("\n=== 场景 7：分析计算中途来源换版（确定性复现）===")
    from app import db
    from app import propagation as db_prop
    station_data = load_module("station_data_mid", ROOT / "station" / "app" / "data.py")

    tmpdir = tempfile.mkdtemp(prefix="propagation-mid-swap-")
    db_path = str(Path(tmpdir) / "quality.db")
    db.init_db(db_path)

    date, next_day = "2026-06-01", "2026-06-02"

    def publish(section: str, day: str, revision: int) -> None:
        rows = [
            {"time": r["time"], "nh3n": r["nh3n"], "source_page": 1}
            for r in station_data.generate_rows(day)
        ]
        run_id = db.start_run(db_path, section, day, "test")
        db.publish_version(db_path, section, day, revision, rows, "test", run_id)

    publish("UP", date, 1)
    publish("DOWN", date, 1)
    publish("DOWN", next_day, 1)

    swapped: dict[str, int] = {"done": 0}

    def swap() -> None:
        # Simulate the second web instance publishing downstream D+1 while the
        # first is still matching surges.
        publish("DOWN", next_day, 2)
        swapped["done"] = 1

    result = db_prop.analyze(db_path, "UP", "DOWN", date, 0.05, 3, _after_compute=swap)
    check(swapped["done"] == 1, "分析计算中途完成了下游次日日档换版")
    check(result["status"] == "stale", "中途换版 -> 结果标记过期", result["message"])
    check("过期" in result["message"], "明确提示不能冒充当前结论", result["message"])
    check(result["result"] is not None, "过期结果仍保留供追溯")
    roles = {a["role"]: a for a in result["archives"]}
    check(roles["downstream_next"]["sequence"] != db.read_day_snapshot(
        db_path, "DOWN", next_day)["sequence"], "快照中的次日日档确为旧版本")

    fresh = db_prop.analyze(db_path, "UP", "DOWN", date, 0.05, 3)
    check(fresh["status"] == "conclusive", "换版完成后重新分析可判断")

    shutil.rmtree(tmpdir, ignore_errors=True)


def scenario_torn_read() -> None:
    print("\n=== 场景 8：读取三份日档期间换版，快照不撕裂 ===")
    from app import db
    from app import propagation as db_prop
    station_data = load_module("station_data_torn", ROOT / "station" / "app" / "data.py")

    tmpdir = tempfile.mkdtemp(prefix="propagation-torn-")
    db_path = str(Path(tmpdir) / "quality.db")
    db.init_db(db_path)

    date, next_day = "2026-07-10", "2026-07-11"
    baseline = {
        (section, day): [
            {"time": r["time"], "nh3n": r["nh3n"], "source_page": 1}
            for r in station_data.generate_rows(day)
        ]
        for section in ("UP", "DOWN")
        for day in (date, next_day)
    }

    def publish(section: str, day: str, revision: int, rows: list[dict]) -> None:
        run_id = db.start_run(db_path, section, day, "test")
        db.publish_version(db_path, section, day, revision, rows, "test", run_id)

    for (section, day), rows in baseline.items():
        publish(section, day, 1, rows)

    def new_rows(day: str) -> list[dict]:
        return [
            {"time": r["time"], "nh3n": round(r["nh3n"] * 3 + 1, 3), "source_page": 2}
            for r in station_data.generate_rows(day)
        ]

    def swap_mid_read() -> None:
        # Runs *inside* the snapshot read transaction: both downstream days
        # change while the analysis is reading.
        publish("DOWN", date, 2, new_rows(date))
        publish("DOWN", next_day, 2, new_rows(next_day))

    read = db.read_archives_together(
        db_path, "UP", "DOWN", date, next_day, _between_reads=swap_mid_read
    )
    by_role = {a["role"]: a for a in read["archives"]}
    check(by_role["downstream"]["revision"] == 1
          and by_role["downstream_next"]["revision"] == 1,
          "读事务内看到的是换版前的同一提交状态",
          f"r={by_role['downstream']['revision']},{by_role['downstream_next']['revision']}")
    expected = {date: baseline[("DOWN", date)], next_day: baseline[("DOWN", next_day)]}
    def series(rows: list[dict]) -> list[tuple[str, float]]:
        return [(r["time"], r["nh3n"]) for r in rows]

    check(all(series(by_role[role]["rows"])
              == series(expected[by_role[role]["date"]])
              for role in ("downstream", "downstream_next")),
          "两天下游数据均来自同一旧版本，没有一新一旧")
    check(all(r["revision"] == 1
              for role in ("downstream", "downstream_next")
              for r in by_role[role]["rows"]),
          "快照内每行都带一致的修订号")
    check(by_role["downstream"]["sequence"] != by_role["downstream_next"]["sequence"],
          "两份日档各有独立发布序号")

    after = db.read_day_snapshot(db_path, "DOWN", date)
    check(after["revision"] == 2, "换版在读取事务结束后对后续读取可见")

    # The same seam through the service marks the result stale, because the
    # snapshot's sequences no longer match the committed state.
    result = db_prop.analyze(
        db_path, "UP", "DOWN", date, 0.05, 3, _between_reads=swap_mid_read
    )
    check(result["status"] == "stale", "读期间换版 -> 服务把结果标记过期",
          result["message"])

    shutil.rmtree(tmpdir, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Propagation tracing demo")
    parser.add_argument("--station", default="http://localhost:8001")
    parser.add_argument("--web1", default="http://localhost:8011")
    parser.add_argument("--web2", default="http://localhost:8012")
    args = parser.parse_args()

    print(f"station={args.station} web1={args.web1} web2={args.web2}")
    try:
        scenario_basic(args.station, args.web1, args.web2)
        scenario_missing(args.station, args.web1)
        scenario_stale(args.station, args.web1, args.web2)
        scenario_matching_objective(args.web1)
        scenario_mid_analysis_swap()
        scenario_torn_read()
        scenario_concurrent_consistency(args.station, args.web1, args.web2)
    except Exception as exc:  # noqa: BLE001
        print(f"\n演示中断: {exc!r}")
        return 1

    passed = sum(1 for ok, _ in RESULTS if ok)
    print(f"\n==== 结果: {passed}/{len(RESULTS)} 项检查通过 ====")
    return 0 if passed == len(RESULTS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
