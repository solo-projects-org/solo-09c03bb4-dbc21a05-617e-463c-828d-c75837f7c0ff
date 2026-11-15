# 断面水质逐时氨氮采集平台

县站分页接口的模拟服务 + FastAPI 采集网页（SQLite 落库），并附带"采集中途县站更正记录"的完整样例。

## 要解决的问题

断面水质数据员要按 **断面 + UTC 日期** 从县站分页接口拉取某日逐时氨氮（NH₃-N）观测，交给污染复核组。县站可能在采集途中更正记录。平台保证：

1. **逐页核对修订号**：首页取得的 `revision` 被钉住，后续任何一页修订号不一致即中止。
2. **游标随修订失效**：县站更正后修订号递增，旧游标再用返回 `409 stale_cursor`，杜绝把两个修订版本的页拼成"弗兰肯数据"。
3. **当日且时刻唯一**：每行必须是该 UTC 日期的整点时刻，跨页出现重复时刻即中止。
4. **整日发布（all-or-nothing）**：收齐 `total_rows` 且收尾再次确认源端修订号未变，才在一个 SQLite 事务里替换整天；失败时旧版（若有）原样保留，不完整数据永远查不到。
5. **超时重试当前页**：网络错误、超时、5xx 只重试当前页（游标不前进），最多 3 次；`409` 属永久性错误不重试。
6. **双 Web 进程并发安全**：同一断面+日期同时只允许一个活动采集任务（SQLite 部分唯一索引，跨进程生效）；两个进程共享同一 SQLite 卷。
7. **迟到采集不能盖掉较新修订**：发布事务里比对已发布修订号，旧修订号写入直接拒绝（`StaleRevisionError`）。

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
| `GET /api/published?section=&date=` | 已发布整日版本：时刻、氨氮值、来源页、修订号；无完整版本时 404 |

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

## 演示场景（`demo/demo.py`，29 项断言）

1. **中途更正**：采集开始后县站更正 08:00 数值（修订号 1→2）→ 采集因 409 失败、无半成品公布、旧游标确认为 409；重新采集后整日按修订号 2 原子发布，24 行时刻唯一、来源页齐全。
2. **读取失败重试**：注入一次性 500 → 仅当前页重试（`page_attempts` 大于页数），最终成功发布。
3. **双进程并发 + 陈旧修订防护**：两进程同时采集同日 → 一个 202、一个 409；再构造"源端退回旧修订号的迟到采集"，发布事务拒绝旧修订号，已发布的新版本原样保留；另有直接对 DB 事务的防护单测。

另外验证过：worker 进程被 `kill -9` 后重启，遗留的 running 任务自动标记为 `interrupted`，可重新采集。

## 关键设计

- **发布事务**（`web/app/db.py: publish_version`）：`BEGIN IMMEDIATE` 下检查已发布修订号 → 删除旧整天数据 → 插入新整天 → 任务置成功，单事务提交；任何步骤异常都回滚。
- **活动任务互斥**：`runs` 表上 `WHERE status='running'` 的部分唯一索引，跨进程互斥，不依赖单机内存锁。
- **采集状态机**（`web/app/collector.py`）：metadata 钉修订号 → 逐页（重试仅当前页、校验修订号/总行数/页序/时刻唯一）→ 收齐行数 → 再查一次 metadata 复核修订号 → 整日发布。
- **崩溃恢复**：worker 启动时把本库遗留的 `running` 任务标记为 `interrupted`，避免互斥锁泄漏。
- **SQLite 并发**：WAL + `busy_timeout=30s`，两个 uvicorn 进程共享同一数据库文件。

## 目录

```
station/            县站模拟服务（FastAPI）
  app/main.py       分页/游标/修订号/管理端
  app/data.py       确定性 24 小时合成数据
web/                采集网页（FastAPI + SQLite）
  app/main.py       HTTP API 与网页
  app/collector.py  逐页核对、重试、整日发布状态机
  app/station_client.py
  app/db.py         SQLite 模式、互斥与发布事务
demo/demo.py        三场景自动化演示（python -I，仅标准库 + 本仓库 db 模块）
demo/mid-correction.sh  curl 手动中途更正样例
docker-compose.yml  station + web1 + web2 + 共享卷
```
