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
from . import propagation
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


class AnalyzeBody(BaseModel):
    upstream_section: str = Field(..., examples=["SECTION-A"])
    downstream_section: str = Field(..., examples=["SECTION-B"])
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$", examples=["2026-11-14"])
    threshold: float = Field(..., gt=0, examples=[0.1])
    max_lag_hours: int = Field(..., ge=1, le=6, examples=[3])


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


@app.post("/api/analyze")
def api_analyze(body: AnalyzeBody) -> dict:
    """Run one propagation check against a single committed snapshot."""
    if body.upstream_section == body.downstream_section:
        raise HTTPException(status_code=400, detail="上下游断面不能相同")
    try:
        return propagation.analyze(
            DB_PATH,
            body.upstream_section,
            body.downstream_section,
            body.date,
            body.threshold,
            body.max_lag_hours,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@app.get("/api/analyses")
def api_analyses(limit: int = 30) -> list[dict]:
    return propagation.list_recent(DB_PATH, min(max(limit, 1), 200))


@app.get("/api/analyses/{analysis_id}")
def api_analysis(analysis_id: int) -> dict:
    stored = propagation.get_analysis(DB_PATH, analysis_id)
    if stored is None:
        raise HTTPException(status_code=404, detail="analysis not found")
    return stored


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
  .conclusive { background: #d1f0dd; }
  .indeterminate { background: #ffe3b3; }
  .stale { background: #f8d4d4; }
  .verdict { padding: .8rem 1rem; border-radius: 8px; margin: .8rem 0; font-weight: 600; }
  .verdict.indeterminate { background: #fff4e0; border: 1px solid #e0a94a; }
  .verdict.stale { background: #fdeaea; border: 1px solid #d64545; }
  .verdict.conclusive { background: #e6f7ec; border: 1px solid #2f9e5f; }
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
  <h3>污染传播溯源 — 上游突升是否传到下游</h3>
  <p class="muted">上游取当日 01:00～23:00 的逐时增量；下游取当日与次日（覆盖跨午夜传播）。
    每个下游突升至多匹配一次：先使配对数最多，再使总滞后最短。
    三份日档必须属于同一已提交状态，任一缺失即“无法判断”，不会报成无传播。</p>
  <label>上游断面 <select id="upSection"></select></label>
  <label>下游断面 <select id="downSection"></select></label>
  <label>UTC 日期 <input id="anDate" type="date" required></label>
  <label>突升阈值 (mg/L) <input id="threshold" type="number" min="0.001" step="0.001" value="0.05"></label>
  <label>最大滞后
    <select id="maxLag">
      <option>1</option><option>2</option><option selected>3</option>
      <option>4</option><option>5</option><option>6</option>
    </select> 小时
  </label>
  <button id="analyzeBtn">分析传播</button>
  <div id="analysis"></div>
</div>

<div class="panel">
  <h3>最近分析记录</h3>
  <div id="history"></div>
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
  const options = data.sections.map(s => `<option>${esc(s)}</option>`).join('');
  $('section').innerHTML = options;
  $('upSection').innerHTML = options;
  $('downSection').innerHTML = options;
  const idx = data.sections.indexOf('SECTION-B');
  $('downSection').selectedIndex = idx >= 0 ? idx : Math.min(1, data.sections.length - 1);
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

function fmtArchive(a) {
  return `<tr><td>${esc(a.role)}</td><td>${esc(a.section)}</td><td>${esc(a.date)}</td>
    <td>${a.revision}</td><td>${a.sequence}</td><td>${a.rows ?? a.row_count}</td>
    <td>${esc(a.published_at)}</td><td>${esc(a.published_by)}</td></tr>`;
}

const STATUS_LABEL = { conclusive: '可判断', indeterminate: '无法判断', stale: '结果已过期' };

function renderAnalysis(d) {
  const label = STATUS_LABEL[d.status] || d.status;
  let html = `<div class="verdict ${d.status}">[${esc(label)}] ${esc(d.message)}</div>`;
  if (d.missing && d.missing.length) {
    html += '<h4>缺失日档</h4><ul>' + d.missing.map(m => `<li>${esc(m)}</li>`).join('') + '</ul>';
  }
  if (d.stale_reasons && d.stale_reasons.length) {
    html += '<h4>换版详情</h4><ul>' + d.stale_reasons.map(m => `<li>${esc(m)}</li>`).join('') + '</ul>';
  }
  if (d.archives && d.archives.length) {
    html += `<h4>所用日档（快照 ${esc(d.snapshot_token)}，同一已提交状态）</h4>
      <table><tr><th>角色</th><th>断面</th><th>UTC 日期</th><th>修订号</th>
      <th>发布序号</th><th>行数</th><th>发布时间</th><th>发布者</th></tr>` +
      d.archives.map(fmtArchive).join('') + '</table>';
  }
  if (d.result) {
    html += `<h4>传播配对（${d.result.match_count} 组，总滞后 ${d.result.total_lag_hours} 小时；
      上游突升 ${d.result.upstream_surges.length} 个，下游突升 ${d.result.downstream_surges.length} 个）</h4>`;
    if (d.result.matches.length) {
      html += '<table><tr><th>上游突升时刻 (UTC)</th><th>下游突升时刻 (UTC)</th>' +
        '<th>滞后（小时）</th><th>上游增量</th><th>下游增量</th>' +
        '<th>上游日档修订</th><th>下游日档修订</th></tr>' +
        d.result.matches.map(m => `<tr><td>${esc(m.upstream_time)}</td>
          <td>${esc(m.downstream_time)}</td><td>${m.lag_hours}</td>
          <td>${m.upstream_delta}</td><td>${m.downstream_delta}</td>
          <td>${esc(m.upstream_date)} · r${m.upstream_revision} · seq ${m.upstream_sequence}</td>
          <td>${esc(m.downstream_date)} · r${m.downstream_revision} · seq ${m.downstream_sequence}</td></tr>`).join('') + '</table>';
    } else {
      html += '<p class="muted">给定阈值与最大滞后内没有可配对项。</p>';
    }
    if (d.result.downstream_surges.length) {
      html += '<details><summary>下游突升清单（含未配对项）</summary>' +
        '<table><tr><th>时刻 (UTC)</th><th>增量</th></tr>' +
        d.result.downstream_surges.map(s => `<tr><td>${esc(s.time)}</td><td>${s.delta}</td></tr>`).join('') +
        '</table></details>';
    }
  }
  if (d.id) {
    html += `<p class="muted">分析编号 #${d.id} · 计算于 ${esc(d.created_at)} ·
      参数：阈值 ${d.threshold} mg/L，最大滞后 ${d.max_lag_hours} 小时</p>`;
  }
  $('analysis').innerHTML = html;
}

async function runAnalysis() {
  const date = $('anDate').value;
  if (!date) { msg('请选择分析用的 UTC 日期'); return; }
  const body = {
    upstream_section: $('upSection').value,
    downstream_section: $('downSection').value,
    date,
    threshold: parseFloat($('threshold').value),
    max_lag_hours: parseInt($('maxLag').value, 10)
  };
  if (!(body.threshold > 0)) { msg('突升阈值必须是正数'); return; }
  if (body.upstream_section === body.downstream_section) { msg('上下游断面不能相同'); return; }
  $('analyzeBtn').disabled = true;
  try {
    const r = await fetch('/api/analyze', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)
    });
    const data = await r.json();
    if (!r.ok) { msg('分析失败 (' + r.status + '): ' + JSON.stringify(data.detail)); return; }
    renderAnalysis(data);
    await renderHistory();
  } finally {
    $('analyzeBtn').disabled = false;
  }
}

async function renderHistory() {
  const r = await fetch('/api/analyses?limit=15');
  const items = await r.json();
  if (!items.length) { $('history').innerHTML = '<p class="muted">暂无分析记录</p>'; return; }
  $('history').innerHTML = '<table><tr><th>#</th><th>上游 → 下游</th><th>UTC 日期</th>' +
    '<th>阈值</th><th>最大滞后</th><th>状态</th><th>时间</th></tr>' +
    items.map(a => `<tr><td>${a.id}</td>
      <td>${esc(a.upstream_section)} → ${esc(a.downstream_section)}</td>
      <td>${esc(a.date)}</td><td>${a.threshold}</td><td>${a.max_lag_hours}h</td>
      <td><span class="tag ${a.status}">${esc(STATUS_LABEL[a.status] || a.status)}</span></td>
      <td>${esc(a.created_at)}</td></tr>`).join('') + '</table>';
}

$('collectBtn').onclick = startCollect;
$('viewBtn').onclick = async () => { await renderRuns(); await renderPublished(); };
$('analyzeBtn').onclick = runAnalysis;
$('section').onchange = async () => { await renderRuns(); await renderPublished(); };
$('date').onchange = async () => { await renderRuns(); await renderPublished(); };
$('date').value = new Date().toISOString().slice(0, 10);
$('anDate').value = new Date(Date.now() - 86400000).toISOString().slice(0, 10);
loadSections().then(renderRuns).then(renderPublished).then(renderHistory);
</script>
</body>
</html>
"""
