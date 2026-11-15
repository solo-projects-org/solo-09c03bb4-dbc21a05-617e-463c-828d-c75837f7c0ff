"""FastAPI web service: trigger/inspect collections and read published days."""
from __future__ import annotations

import logging
import os
import socket
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from . import db
from .collector import CollectionError, collect_day
from .station_client import StationClient, StationError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("web")

DB_PATH = os.environ.get("DB_PATH", "/data/quality.db")
STATION_URL = os.environ.get("STATION_URL", "http://station:8000")
WORKER = os.environ.get("WORKER_NAME") or f"{socket.gethostname()}-{os.getpid()}"

# Background collection threads per process; the DB unique index is the real
# cross-process guard, this is just local bookkeeping.
_threads: dict[int, threading.Thread] = {}


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    db.init_db(DB_PATH)
    recovered = db.recover_interrupted(DB_PATH)
    if recovered:
        log.warning("marked %s interrupted run(s) from a previous worker life", recovered)
    log.info("worker %s ready, db=%s station=%s", WORKER, DB_PATH, STATION_URL)
    yield


app = FastAPI(title="Section water quality collector", version="1.0", lifespan=lifespan)


class CollectBody(BaseModel):
    section: str = Field(..., examples=["SECTION-A"])
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$", examples=["2026-11-14"])


def _run_collection(run_id: int, section: str, date_str: str) -> None:
    client = StationClient(STATION_URL)
    try:
        collect_day(DB_PATH, run_id, section, date_str, client, WORKER)
    except CollectionError as exc:
        log.error("run %s failed: %s", run_id, exc)
        db.fail_run(DB_PATH, run_id, str(exc))
    except StationError as exc:
        log.error("run %s station error: %s", run_id, exc)
        db.fail_run(DB_PATH, run_id, f"station error: {exc}")
    except Exception as exc:  # defensive: never leave a run dangling
        log.exception("run %s unexpected error", run_id)
        db.fail_run(DB_PATH, run_id, f"unexpected error: {exc}")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "worker": WORKER}


@app.get("/api/sections")
def api_sections() -> dict:
    try:
        return {"sections": StationClient(STATION_URL).list_sections()}
    except StationError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.post("/api/collect")
def api_collect(body: CollectBody) -> JSONResponse:
    try:
        run_id = db.start_run(DB_PATH, body.section, body.date, WORKER)
    except db.ActiveRunError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    thread = threading.Thread(
        target=_run_collection,
        args=(run_id, body.section, body.date),
        name=f"collect-{run_id}",
        daemon=True,
    )
    _threads[run_id] = thread
    thread.start()
    return JSONResponse(
        status_code=202,
        content={
            "run_id": run_id,
            "section": body.section,
            "date": body.date,
            "worker": WORKER,
            "status": "running",
        },
    )


@app.get("/api/runs/{run_id}")
def api_run(run_id: int) -> dict:
    run = db.get_run(DB_PATH, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    return run


@app.get("/api/runs")
def api_runs(section: str, date: str) -> list[dict]:
    return db.list_runs(DB_PATH, section, date)


@app.get("/api/published")
def api_published(section: str, date: str) -> dict:
    published = db.get_published(DB_PATH, section, date)
    if published is None:
        raise HTTPException(
            status_code=404,
            detail="no complete published version yet for this section/day",
        )
    return published


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return PAGE_HTML


PAGE_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>断面水质采集台 · 氨氮逐时观测</title>
<style>
  body { font-family: system-ui, "Segoe UI", sans-serif; margin: 2rem; background: #f6f8fa; color: #1f2933; }
  h1 { font-size: 1.3rem; }
  .panel { background: #fff; border: 1px solid #d9e2ec; border-radius: 8px; padding: 1rem 1.2rem; margin-bottom: 1.2rem; }
  label { margin-right: .4rem; font-weight: 600; }
  select, input { padding: .35rem .5rem; border: 1px solid #9fb3c8; border-radius: 6px; }
  button { padding: .4rem 1rem; border: 0; border-radius: 6px; background: #0b6e4f; color: #fff; cursor: pointer; }
  button:disabled { background: #94a3b8; cursor: default; }
  table { border-collapse: collapse; width: 100%; font-size: .9rem; }
  th, td { border: 1px solid #d9e2ec; padding: .35rem .6rem; text-align: left; }
  th { background: #eef2f6; }
  .muted { color: #627d98; font-size: .85rem; }
  .tag { display: inline-block; padding: .1rem .5rem; border-radius: 999px; font-size: .8rem; }
  .running { background: #fff3cd; } .success { background: #d1f0dd; }
  .failed { background: #f8d4d4; } .interrupted { background: #e1d5f7; }
  #msg { white-space: pre-wrap; font-family: ui-monospace, monospace; font-size: .85rem; }
</style>
</head>
<body>
<h1>断面水质采集台 — 逐时氨氮（NH₃-N）观测</h1>

<div class="panel">
  <label>断面
    <select id="section"></select>
  </label>
  <label>UTC 日期
    <input id="date" type="date" required>
  </label>
  <button id="collectBtn">开始采集并整日发布</button>
  <button id="viewBtn">查看已发布版本</button>
  <p class="muted">采集为后台任务：逐页核对修订号，收齐总行数并确认源端修订未变后才整日发布；修订变化或读取失败时保留旧版。</p>
</div>

<div class="panel">
  <h3>采集任务</h3>
  <div id="runs"></div>
</div>

<div class="panel">
  <h3>已发布观测（时刻 / 数值 / 来源页 / 修订号）</h3>
  <div id="published"></div>
</div>

<div class="panel">
  <h3>消息</h3>
  <div id="msg" class="muted"></div>
</div>

<script>
const $ = id => document.getElementById(id);
const msg = t => { $('msg').textContent = t; };
const esc = s => String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

async function loadSections() {
  const r = await fetch('/api/sections');
  if (!r.ok) { msg('无法获取断面列表：' + r.status); return; }
  const data = await r.json();
  $('section').innerHTML = data.sections.map(s => `<option>${esc(s)}</option>`).join('');
}

function params() {
  return new URLSearchParams({ section: $('section').value, date: $('date').value });
}

async function startCollect() {
  const date = $('date').value;
  if (!date) { msg('请选择 UTC 日期'); return; }
  $('collectBtn').disabled = true;
  try {
    const r = await fetch('/api/collect', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ section: $('section').value, date })
    });
    const data = await r.json();
    if (!r.ok) { msg('启动失败 (' + r.status + '): ' + JSON.stringify(data.detail)); return; }
    msg('任务已启动 run_id=' + data.run_id + ' worker=' + data.worker);
    pollRun(data.run_id);
  } finally {
    $('collectBtn').disabled = false;
  }
}

async function pollRun(runId) {
  const timer = setInterval(async () => {
    const r = await fetch('/api/runs/' + runId);
    const run = await r.json();
    await renderRuns();
    if (run.status !== 'running') {
      clearInterval(timer);
      msg('任务结束：' + run.status + (run.error ? ' — ' + run.error : ''));
      await renderPublished();
    }
  }, 700);
}

async function renderRuns() {
  const r = await fetch('/api/runs?' + params());
  const runs = await r.json();
  if (!runs.length) { $('runs').innerHTML = '<p class="muted">暂无任务</p>'; return; }
  $('runs').innerHTML = `<table><tr><th>run</th><th>worker</th><th>状态</th><th>修订号</th>
    <th>行数</th><th>页尝试</th><th>开始(UTC)</th><th>结束(UTC)</th><th>错误</th></tr>` +
    runs.map(r => `<tr><td>${r.id}</td><td>${esc(r.worker)}</td>
      <td><span class="tag ${r.status}">${r.status}</span></td>
      <td>${r.revision ?? '—'}</td><td>${r.rows_collected}</td><td>${r.page_attempts}</td>
      <td>${esc(r.started_at)}</td><td>${esc(r.finished_at ?? '—')}</td>
      <td>${esc(r.error ?? '')}</td></tr>`).join('') + '</table>';
}

async function renderPublished() {
  const r = await fetch('/api/published?' + params());
  if (r.status === 404) { $('published').innerHTML = '<p class="muted">尚无完整发布版本（不完整采集不会公布）。</p>'; return; }
  const d = await r.json();
  $('published').innerHTML =
    `<p>修订号 <b>${d.revision}</b> · 发布于 ${esc(d.published_at)} · 发布者 ${esc(d.published_by)} · 共 ${d.rows.length} 行</p>` +
    '<table><tr><th>时刻(UTC)</th><th>NH₃-N (mg/L)</th><th>来源页</th><th>修订号</th></tr>' +
    d.rows.map(x => `<tr><td>${esc(x.time)}</td><td>${x.nh3n}</td><td>${x.source_page}</td><td>${x.revision}</td></tr>`).join('') +
    '</table>';
}

$('collectBtn').onclick = startCollect;
$('viewBtn').onclick = async () => { await renderRuns(); await renderPublished(); };
$('section').onchange = async () => { await renderRuns(); await renderPublished(); };
$('date').onchange = async () => { await renderRuns(); await renderPublished(); };
$('date').value = new Date().toISOString().slice(0, 10);
loadSections().then(renderRuns).then(renderPublished);
</script>
</body>
</html>
"""
