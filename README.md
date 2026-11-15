# 断面水质逐时氨氮采集平台 + 污染传播溯源

县站分页接口的模拟服务 + FastAPI 采集网页（SQLite 落库），并附带"采集中途县站更正记录"的完整样例；
在已发布日档之上提供**上游氨氮突升是否传到下游**的溯源分析。

## 要解决的问题

### 采集侧

断面水质数据员要按 **断面 + UTC 日期** 从县站分页接口拉取某日逐时氨氮（NH₃-N）观测，交给污染复核组。县站可能在采集途中更正记录。平台保证：

1. **逐页核对修订号**：首页取得的 `revision` 被钉住，后续任何一页修订号不一致即中止。
2. **游标随修订失效**：县站更正后修订号递增，旧游标再用返回 `409 stale_cursor`，杜绝把两个修订版本的页拼成"弗兰肯数据"。
3. **当日且时刻唯一**：每行必须是该 UTC 日期的整点时刻，跨页出现重复时刻即中止。
4. **整日发布（all-or-nothing）**：收齐 `total_rows` 且收尾再次确认源端修订号未变，才在一个 SQLite 事务里替换整天；失败时旧版（若有）原样保留，不完整数据永远查不到。
5. **超时重试当前页**：网络错误、超时、5xx 只重试当前页（游标不前进），最多 3 次；`409` 属永久性错误不重试。
6. **双 Web 进程并发安全**：同一断面+日期同时只允许一个活动采集任务（SQLite 部分唯一索引，跨进程生效）；两个进程共享同一 SQLite 卷。
7. **迟到采集不能盖掉较新修订**：发布事务里比对已发布修订号，旧修订号写入直接拒绝（`StaleRevisionError`）。
8. **发布序号**：每次整日发布都写入单调递增的 `sequence`（即使修订号相同、数值相同的重新发布也算新版本），供分析侧判定快照是否换版。

### 溯源侧（污染传播分析）

污染溯源人员在采集网页上选择 **上游断面、下游断面、UTC 日期 D、正数突升阈值、最大滞后（1～6 小时）**，平台基于**已发布日档**给出结论：

1. **增量口径**：比较相邻整点的氨氮增量，取结束时刻 01:00～23:00（00:00 的增量需要前一日，不取）。
2. **上游**只取 D 日 01:00～23:00 的突升；**下游**取 D 日**与 D+1 日**，以覆盖跨午夜的传播窗口。
3. **突升** = 增量 ≥ 阈值（阈值必须为正）。
4. **可配条件**：上游突升时刻 `u`、下游突升时刻 `d`，满足 `1 ≤ d − u ≤ 最大滞后`。
5. **匹配目标**：每个下游突升至多匹配一次（上游同样至多一次）。**先使匹配数最多，再使总滞后最短**（最小费用最大基数二分匹配）。
6. **展示**：配对的上游/下游时刻、滞后小时数、以及三份日档各自的修订号与发布序号。
7. **缺档即"无法判断"**：三份日档（上游 D、下游 D、下游 D+1）缺少任意一份，结论为 **indeterminate（无法判断）** 并列出缺失项——绝不报告成"无传播"。
8. **同一已提交状态**：三份日档在**同一个 SQLite 读事务**内读取，两个 Web 实例交错采集发布也不会拼出跨版本的混合快照。
9. **换版提示过期**：计算完成后再核对三份日档的发布序号；若分析期间任一来源换版，结果标记 **stale（结果已过期）**，保留数据供追溯但不冒充当前结论。重新打开历史分析时也会再次核对。

## 架构

```
docker compose
├── station  县站模拟接口（可切换/递增修订号，分页 + 游标）   :8001
├── web1     采集 Web 进程 A（FastAPI + SQLite WAL）          :8011
└── web2     采集 Web 进程 B（同一份代码，同一 SQLite 卷）    :8012

卷 quality-data:/data  -> 两个 web 进程共享 quality.db
```

### 县站接口（`station/`，本地模拟，带管理端用于演示更正）

| 方法/路径 | 说明 |
|---|---|
| `GET /metadata?section=&date=` | 当前修订号、总行数、页大小 |
| `GET /observations?section=&date=&cursor=` | 分页返回 `revision`、`total_rows`、`page`、`next_cursor` 和行（含 `source_page`） |
| `POST /admin/correct` | 更正某时刻数值：修订号 +1，旧游标立即失效 |
| `POST /admin/reset` | 该日数据重置回修订号 1 |
| `POST /admin/fail-next` | 下一次分页读取返回 500（一次性，演示重试） |
| `POST /admin/latency` | 调整每页模拟延迟（演示并发用） |

游标是携带 `(section, date, revision, page)` 的不透明 token；服务端发现 token 里的修订号与当前不符即返回 409。

### Web 接口（`web/`）

| 方法/路径 | 说明 |
|---|---|
| `GET /` | 采集网页：选断面、UTC 日期，触发采集，查看任务与已发布版本 |
| `GET /api/sections` | 县站支持的断面 |
| `POST /api/collect` | 启动后台采集（同断面+日期已有活动任务时返回 409） |
| `GET /api/runs/{id}` | 单个任务状态（running/success/failed/interrupted、修订号、行数、页尝试次数、错误） |
| `GET /api/runs?section=&date=` | 该日全部采集任务（含两个 worker 的记录） |
| `GET /api/published?section=&date=` | 已发布整日版本：时刻、氨氮值、来源页、修订号、发布序号；无完整版本时 404 |
| `POST /api/analyze` | 传播溯源分析：上下游断面、UTC 日期、正数突升阈值、最大滞后 1～6 小时（示例见下） |
| `GET /api/analyses?limit=` | 最近分析记录（含结论状态） |
| `GET /api/analyses/{id}` | 单次分析：结论、所用三份日档（修订号 + 发布序号）、配对结果；来源换版后自动标记过期 |

`POST /api/analyze` 请求体：

```json
{
  "upstream_section": "SECTION-A",
  "downstream_section": "SECTION-B",
  "date": "2026-11-14",
  "threshold": 0.05,
  "max_lag_hours": 3
}
```

返回要点：`status` 为 `conclusive`（可判断）/ `indeterminate`（缺少日档，`missing` 列出原因）/
`stale`（来源换版，`stale_reasons` 列出哪份日档从哪个序号换到哪个）；`archives` 为三份日档的
修订号与发布序号；`result.matches` 每项含 `upstream_time`、`downstream_time`、`lag_hours`、
上下游增量与下游所用日档日期。

## 快速启动（Docker Compose）

```bash
./run-demo.sh                 # 构建、起栈、等待健康、自动跑全部演示检查
# 或手动：
docker compose up -d --build
# 网页 http://localhost:8011 （或 8012），县站文档 http://localhost:8001/docs
```

手动的中途更正样例（curl 逐步可观察）：

```bash
./demo/mid-correction.sh
```

### 不用 Docker 的本地运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r web/requirements.txt -r station/requirements.txt
PAGE_SIZE=8 .venv/bin/python -m uvicorn station.app.main:app --port 8001 &
DB_PATH=$PWD/data/quality.db STATION_URL=http://127.0.0.1:8001 WORKER_NAME=web1 \
  .venv/bin/python -m uvicorn web.app.main:app --port 8011 &
DB_PATH=$PWD/data/quality.db STATION_URL=http://127.0.0.1:8001 WORKER_NAME=web2 \
  .venv/bin/python -m uvicorn web.app.main:app --port 8012 &
python3 -I demo/demo.py
```

## 演示场景（`demo/demo.py`，29 项断言；`demo/propagation-demo.py`，49 项断言）

采集侧（`demo/demo.py`）：

1. **中途更正**：采集开始后县站更正 08:00 数值（修订号 1→2）→ 采集因 409 失败、无半成品公布、旧游标确认为 409；重新采集后整日按修订号 2 原子发布，24 行时刻唯一、来源页齐全。
2. **读取失败重试**：注入一次性 500 → 仅当前页重试（`page_attempts` 大于页数），最终成功发布。
3. **双进程并发 + 陈旧修订防护**：两进程同时采集同日 → 一个 202、一个 409；再构造"源端退回旧修订号的迟到采集"，发布事务拒绝旧修订号，已发布的新版本原样保留；另有直接对 DB 事务的防护单测。

溯源侧（`demo/propagation-demo.py`）：

4. **三档齐全 → 可判断**：下游 07:00 的突升与上游 05:00 的突升按 2 小时滞后配对；结果列出三份日档各自的修订号与发布序号。
5. **缺档 → 无法判断**：缺下游次日日档时返回 `indeterminate` 并点名缺失档案，绝不出现"无传播"式结论。
6. **交错换版 → 结果过期**：Web2 重新发布下游日档后，Web1 之前存下的分析被标记 `stale`；重新分析得到新的快照与序号。
7. **计算中途换版**：用测试注入口在"算完但未复核"之间发布新版本，确定性地验证结果被标记过期。
8. **匹配目标**：与独立的穷举求解器逐例比对，确认先最大化配对数、再最小化总滞后，且下游突升不被重复匹配。
9. **并发一致性**：采集与 90+ 次分析交错执行，每次分析要么可判断、要么过期，没有混合快照。

另验证过：worker 进程被 `kill -9` 后重启，遗留的 running 任务自动标记为 `interrupted`，可重新采集。

## 关键设计

- **发布事务**（`web/app/db.py: publish_version`）：`BEGIN IMMEDIATE` 下检查已发布修订号 → 删除旧整天数据 → 插入新整天 → 更新 `published_versions` 发布序号 → 任务置成功，单事务提交；任何步骤异常都回滚。
- **活动任务互斥**：`runs` 表上 `WHERE status='running'` 的部分唯一索引，跨进程互斥，不依赖单机内存锁。
- **采集状态机**（`web/app/collector.py`）：metadata 钉修订号 → 逐页（重试仅当前页、校验修订号/总行数/页序/时刻唯一）→ 收齐行数 → 再查一次 metadata 复核修订号 → 整日发布。
- **崩溃恢复**：worker 启动时把本库遗留的 `running` 任务标记为 `interrupted`，避免互斥锁泄漏。
- **SQLite 并发**：WAL + `busy_timeout=30s`，两个 uvicorn 进程共享同一数据库文件。
- **一致快照读取**（`web/app/db.py: read_archives_together`）：三份日档在同一个 `BEGIN` 读事务内读出；WAL 下该事务看到单一已提交状态，并发发布要么整体可见、要么整体不可见。
- **匹配算法**（`web/app/analysis.py`）：最小费用最大基数二分匹配（连续最短增广路，SPFA 处理残量负权），费用即滞后小时数；结果按下游时刻排序展示。
- **过期判定**（`web/app/propagation.py`）：计算后重读三份日档的 `sequence`，任一不同即 `stale`；打开历史分析时再核对一次，保证"过期"不会随时间被误当成当前结论。

## 目录

```
station/            县站模拟服务（FastAPI）
  app/main.py       分页/游标/修订号/管理端
  app/data.py       确定性 24 小时合成数据
web/                采集与分析网页（FastAPI + SQLite）
  app/main.py       HTTP API 与网页（采集 + 传播溯源）
  app/collector.py  逐页核对、重试、整日发布状态机
  app/station_client.py
  app/db.py         SQLite 模式、互斥、发布事务、一致快照与发布序号
  app/analysis.py   突升识别与最小费用最大基数匹配（纯标准库）
  app/propagation.py 快照读取、结论状态与过期判定
demo/demo.py        采集三场景自动化演示（python -I）
demo/propagation-demo.py  溯源场景自动化演示（python -I）
demo/mid-correction.sh  curl 手动中途更正样例
docker-compose.yml  station + web1 + web2 + 共享卷
```
