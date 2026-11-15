#!/usr/bin/env python3
"""End-to-end demonstration for the water-quality collection platform.

Runnable with an isolated interpreter:

    python -I demo/demo.py

Scenarios:
  1. Mid-flight correction  -> stale cursor 409, run fails, nothing partial is
     published; re-collection publishes the new revision atomically.
  2. Transient read failure -> current page is retried, run still succeeds.
  3. Two web processes collecting the same day -> only one runs; plus an
     end-to-end stale-revision check proving a late lower revision can never
     overwrite a newer published revision, plus a direct DB-guard unit check.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "web"))  # db.py is stdlib-only, safe under -I

RESULTS: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    RESULTS.append((ok, label))
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {label}" + (f" -- {detail}" if detail else ""))
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
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode())
        except Exception:
            return exc.code, {}


def wait_run(base: str, run_id: int, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, run = request("GET", f"{base}/api/runs/{run_id}")
        if status == 200 and run["status"] != "running":
            return run
        time.sleep(0.4)
    raise TimeoutError(f"run {run_id} did not finish in {timeout}s")


def reset_day(station: str, section: str, date: str) -> None:
    request("POST", f"{station}/admin/reset", {"section": section, "date": date})


def published_or_none(web: str, section: str, date: str) -> dict | None:
    status, body = request("GET", f"{web}/api/published?section={section}&date={date}")
    return body if status == 200 else None


def scenario_mid_correction(station: str, web: str, section: str, date: str) -> None:
    print("\n=== 场景 1：采集中途县站更正记录（旧游标失效 / 整日发布）===")
    reset_day(station, section, date)

    # Capture a cursor minted at revision 1, before any correction happens.
    status, page = request("GET", f"{station}/observations?section={section}&date={date}")
    old_cursor = page["next_cursor"]
    check(status == 200 and page["revision"] == 1 and old_cursor, "取得修订号 1 的分页游标")

    status, started = request("POST", f"{web}/api/collect", {"section": section, "date": date})
    check(status == 202, "启动采集任务", json.dumps(started))
    run_id = started["run_id"]

    # Wait until the first page is being/has been processed, then correct.
    time.sleep(0.7)
    status, corr = request(
        "POST", f"{station}/admin/correct",
        {"section": section, "date": date, "time": "08:00", "nh3n": 9.999},
    )
    check(status == 200 and corr["revision"] == 2, "县站更正后修订号递增到 2", json.dumps(corr))

    run = wait_run(web, run_id)
    check(run["status"] == "failed", "混入旧修订页的采集任务失败", run.get("error", ""))
    check("revision" in run["error"].lower() or "cursor" in run["error"].lower(),
          "失败原因为修订变化/旧游标", run["error"])
    check(published_or_none(web, section, date) is None,
          "失败后没有任何部分数据被公布")

    # Direct proof that the old cursor is now dead at the source.
    status, _ = request("GET", f"{station}/observations?section={section}&date={date}&cursor={old_cursor}")
    check(status == 409, "复用更正前游标返回 409 stale_cursor")

    # Re-collect: full day at revision 2 gets published atomically.
    status, started2 = request("POST", f"{web}/api/collect", {"section": section, "date": date})
    check(status == 202, "重新启动采集")
    run2 = wait_run(web, started2["run_id"])
    check(run2["status"] == "success" and run2["revision"] == 2,
          "重新采集成功并按修订号 2 发布", json.dumps(run2))

    published = published_or_none(web, section, date)
    check(published is not None and published["revision"] == 2, "已发布版本修订号为 2")
    check(len(published["rows"]) == 24, "收齐总行数 24 行")
    times = [r["time"] for r in published["rows"]]
    check(len(set(times)) == 24 and all(t.endswith(":00") for t in times),
          "当日时刻唯一且为整点 UTC 时刻")
    row8 = next(r for r in published["rows"] if r["time"] == "08:00")
    check(row8["nh3n"] == 9.999, "更正后的数值被发布", json.dumps(row8))
    check(all(r["source_page"] >= 1 and r["revision"] == 2 for r in published["rows"]),
          "每行都记录来源页与修订号")

    # Same revision again: idempotent success, still revision 2.
    status, started3 = request("POST", f"{web}/api/collect", {"section": section, "date": date})
    run3 = wait_run(web, started3["run_id"])
    check(run3["status"] == "success" and run3["revision"] == 2,
          "同修订号重复采集幂等成功")


def scenario_read_failure(station: str, web: str, section: str, date: str) -> None:
    print("\n=== 场景 2：页读取失败，仅重试当前页后成功 ===")
    reset_day(station, section, date)
    request("POST", f"{station}/admin/fail-next",
            {"section": section, "date": date})

    # The injected failure surfaces directly at the station endpoint.
    status, _ = request("GET", f"{station}/observations?section={section}&date={date}")
    check(status == 500, "县站确实对下一次读取返回 500")

    # Re-arm: the probe above consumed the one-shot failure.
    request("POST", f"{station}/admin/fail-next",
            {"section": section, "date": date})

    status, started = request("POST", f"{web}/api/collect", {"section": section, "date": date})
    run = wait_run(web, started["run_id"])
    check(run["status"] == "success", "重试后整日采集成功", run.get("error", ""))
    check(run["page_attempts"] >= 4,
          "3 个页面中至少 1 页发生重试（尝试次数 > 页数）",
          f"page_attempts={run['page_attempts']}")
    check(len(published_or_none(web, section, date)["rows"]) == 24, "发布 24 行完整数据")


def scenario_concurrency(station: str, web1: str, web2: str, section: str, date: str) -> None:
    print("\n=== 场景 3：两个 Web 进程并发采集同日 + 陈旧修订防护 ===")
    reset_day(station, section, date)
    request("POST", f"{station}/admin/latency", {"delay_ms": 1200})
    try:
        s1, r1 = request("POST", f"{web1}/api/collect", {"section": section, "date": date})
        s2, r2 = request("POST", f"{web2}/api/collect", {"section": section, "date": date})
        statuses = sorted([s1, s2])
        check(statuses == [202, 409],
              "并发同日采集：一个受理、一个被唯一活动任务约束拒绝",
              f"web1={s1} web2={s2} detail={r2.get('detail')}")

        # While the winner runs, no partial day is visible anywhere.
        time.sleep(0.8)
        check(published_or_none(web1, section, date) is None
              and published_or_none(web2, section, date) is None,
              "采集进行中两进程均看不到未完成版本")

        winner_base = web1 if s1 == 202 else web2
        run = wait_run(winner_base, r1["run_id"] if s1 == 202 else r2["run_id"])
        check(run["status"] == "success", "胜出任务完整发布", json.dumps(run))
        check(published_or_none(web1, section, date)["revision"] == 1
              and published_or_none(web2, section, date)["revision"] == 1,
              "两个进程读取同一 SQLite，均看到修订号 1 的完整版本")
    finally:
        request("POST", f"{station}/admin/latency", {"delay_ms": 350})

    # End-to-end stale guard: publish rev 2, then make the station serve rev 1
    # again and confirm the late lower revision cannot overwrite the newer one.
    request("POST", f"{station}/admin/correct",
            {"section": section, "date": date, "time": "10:00", "nh3n": 7.777})
    _, started = request("POST", f"{web1}/api/collect", {"section": section, "date": date})
    run = wait_run(web1, started["run_id"])
    check(run["status"] == "success" and run["revision"] == 2, "采集并发布修订号 2")

    reset_day(station, section, date)  # station goes back to revision 1
    _, started = request("POST", f"{web2}/api/collect", {"section": section, "date": date})
    run = wait_run(web2, started["run_id"])
    check(run["status"] == "failed", "迟到的旧修订采集被拒绝")
    check("refusing to publish revision 1" in run["error"], "拒绝原因是陈旧修订防护", run["error"])
    published = published_or_none(web1, section, date)
    check(published["revision"] == 2, "已发布的较新修订号 2 未被覆盖")

    # Direct DB-layer check (the exact cross-process transaction guard).
    from app import db  # noqa: E402  (path inserted at top of file)

    tmp = ROOT / "data" / f"guard-{os.getpid()}.db"
    tmp.parent.mkdir(exist_ok=True)
    db.init_db(str(tmp))
    rid = db.start_run(str(tmp), section, date, "guard-test")
    db.publish_version(str(tmp), section, date, 2,
                       [{"time": "00:00", "nh3n": 1.0, "source_page": 1}], "guard-test", rid)
    rid2 = db.start_run(str(tmp), section, date, "guard-test")
    blocked = False
    try:
        db.publish_version(str(tmp), section, date, 1,
                           [{"time": "00:00", "nh3n": 0.5, "source_page": 1}],
                           "guard-test", rid2)
    except db.StaleRevisionError:
        blocked = True
    check(blocked, "DB 事务直接验证：旧修订无法覆盖新修订")
    kept = db.get_published(str(tmp), section, date)
    check(kept["revision"] == 2 and kept["rows"][0]["nh3n"] == 1.0,
          "防护触发后原有数据原样保留")
    tmp.unlink(missing_ok=True)
    for suffix in ("-wal", "-shm"):
        Path(str(tmp) + suffix).unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Water-quality platform demo")
    parser.add_argument("--station", default="http://localhost:8001")
    parser.add_argument("--web1", default="http://localhost:8011")
    parser.add_argument("--web2", default="http://localhost:8012")
    parser.add_argument("--section", default="SECTION-A")
    parser.add_argument("--date1", default="2026-11-13")
    parser.add_argument("--date2", default="2026-11-12")
    parser.add_argument("--date3", default="2026-11-11")
    args = parser.parse_args()

    print(f"station={args.station} web1={args.web1} web2={args.web2}")
    try:
        scenario_mid_correction(args.station, args.web1, args.section, args.date1)
        scenario_read_failure(args.station, args.web1, args.section, args.date2)
        scenario_concurrency(args.station, args.web1, args.web2, args.section, args.date3)
    except Exception as exc:
        print(f"\n演示中断: {exc!r}")
        return 1

    passed = sum(1 for ok, _ in RESULTS if ok)
    print(f"\n==== 结果: {passed}/{len(RESULTS)} 项检查通过 ====")
    return 0 if passed == len(RESULTS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
