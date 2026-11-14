# 🌬️ 风电场振动读数数据湖（单进程演示）

把大量振动读数 CSV 小文件合并为按 **UTC 日分区的 Parquet**，减少故障回溯时的文件扫描。
单进程 FastAPI 应用，PyArrow 负责 Parquet 读写，SQLite（WAL 模式）只存元数据。

## 功能与约束对照

| 需求 | 实现方式 |
| --- | --- |
| 上传含机组号、UTC 时间、有限振动值的 CSV | `POST /api/readings`，整批校验：必需三列、ISO 8601 时间、有限数值、非空机组 |
| 每批 ≤ 10000 行且限同一 UTC 日 | `app/csv_import.py` 整体校验，任一规则不满足整批拒绝（不产生任何文件） |
| 整批追加为一个 Parquet，保留重复行 | 一批 = `lake/day=YYYY-MM-DD/<id>.parquet` 一个文件；PyArrow 拼接不去重 |
| 浏览器按机组和时段查读数、查看文件数 | 单页面 `GET /`；`GET /api/readings/query`、`GET /api/stats` |
| 启动压实，一次只压实一份日分区 | `POST /api/compactions?day=...`，进程内锁 + DB running 任务双重互斥，并发请求返回 409 |
| 合并启动时该日文件为一个 | 任务启动在同一事务内固定当时的 active 文件集合 |
| 压实期间仍能上传/查询，后来上传的文件须保留 | Parquet IO 在线程池、DB 仅短事务；新上传文件不并入本次任务，发布后仍为独立 active 文件 |
| 查询固定开始时的文件集合，切换不漏行、不重行、不中断 | 查询开始即创建 `snapshots/snapshot_files` 读快照，全程读快照内文件；压实只把旧件置 `superseded` |
| 旧文件无人读取后才回收 | 旧件被快照引用期间保留；引用归零后由 2 秒周期的后台回收器删除并登记 `deleted` |
| 读写 Parquet 不持数据库写锁 | 所有 PyArrow 调用均在 DB 事务之外的线程池中执行，DB 写事务只更新几行元数据 |
| 失败/切换中退出，重启保留完整已提交数据、清掉未发布新件、续完旧件回收 | 启动 `recover()`：running 任务置 failed、`_tmp/` 全清、磁盘孤儿清理、立即执行一轮回收 |
| 暂停合并及查询的演示开关 | `POST /api/demo/gates/merge|query?paused=true|false`，状态持久化于 SQLite，重启保持 |
| 虚构读数 | `POST /api/demo/generate`（页面按钮），确定性随机种子，可指定行数/日期/机组 |
| 按机组 + UTC 日导出训练 CSV | 页面「导出训练数据」；`POST /api/exports`，文件落在 `exports/`，页面可下载 |
| 同刻同值合并、记录原行数 | 同一时刻同值只输出一行，`row_count` 列给出合并前的原行数；输出按时间升序 |
| 同刻异值不发布并列出明细 | 返回 409，`conflicts` 列出时刻与全部不同取值；不写文件、不登记 |
| 导出登记文件、来源文件 ID、源行数 | SQLite `exports` + `export_files`，单事务登记；页面/接口可查 |
| 导出只含受理时读数 | 导出入口创建读快照固定文件集合，期间上传/压实不影响结果；旧件被快照保护 |
| 失败/退出不留可下载残件 | 先写 `exports/_tmp` 并 fsync，原子改名，登记失败即删成品；重启清理未登记孤儿 CSV |
| 重启仍可下载已发布文件 | 元数据与 `exports/*.csv` 持久化；重启只清未登记残件 |
| 可实际启动的 Docker Compose | `docker compose up --build`，数据存于命名卷 `viblake-data` |

## 目录结构

```
viblake/
├── app/
│   ├── main.py            # FastAPI 路由、生命周期（启动恢复）
│   ├── service.py         # 上传/查询/导出/统计/后台压实调度/虚构数据
│   ├── compaction.py      # 压实编排、回收器、崩溃恢复
│   ├── db.py              # SQLite 元数据（双连接 + WAL + 串行写事务 + 读快照）
│   ├── parquet_store.py   # PyArrow 读写（纯文件 IO，不碰 DB）
│   ├── export_store.py    # 训练 CSV：同刻合并/冲突检测、原子发布（不碰 DB）
│   ├── csv_import.py      # CSV 整批校验
│   ├── gates.py           # 暂停合并 / 暂停查询开关
│   ├── config.py          # 环境变量配置
│   └── web/index.html     # 单页面（无外部前端依赖）
├── tests/                 # 集成测试 + HTTP 冒烟测试 + 导出测试 + 崩溃演练
├── examples/              # 示例 CSV（含重复行示例）
├── Dockerfile
└── docker-compose.yml
```

## 快速开始（Docker Compose）

```bash
cd viblake
docker compose up --build
# 浏览器打开 http://localhost:8000
```

页面操作顺序建议：② 生成并上传虚构读数（同一点 2~3 次制造小文件）→ ① 查看文件数 →
⑤ 导出某机组某 UTC 日的训练 CSV → ⑥ 对该 UTC 日启动压实 → 观察 active 文件降为 1、
旧件经 superseded 后被回收。

## 本地直接运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
# 数据默认在 ./data（可用 VIBLAKE_DATA_DIR 覆盖）
```

上传示例：

```bash
curl -F "file=@examples/readings_sample.csv" http://localhost:8000/api/readings
curl "http://localhost:8000/api/readings/query?turbine=WTG-01&limit=100"
curl -X POST "http://localhost:8000/api/compactions?day=2026-11-13"
```

导出训练数据（同刻同值合并、异值拒绝）：

```bash
curl -X POST "http://localhost:8000/api/exports?turbine=WTG-01&day=2026-11-13"
# 201 {"export_id":"export-…","row_count":…,"source_rows":…,
#      "source_file_ids":[…],"source_file_count":…,
#      "download_url":"/api/exports/export-…/download"}
curl -OJ "http://localhost:8000/api/exports/<export_id>/download"
# 输出列：turbine,ts_utc,vibration,row_count
# 409 = 同一时刻存在不同振动值，响应 conflicts 列出时刻与全部取值，本次不发布
```

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/readings` | 上传 CSV（multipart 字段 `file`），422 = 整批校验失败 |
| GET | `/api/readings/query` | 参数 `turbine,start,end,limit`；返回快照 id、快照文件数、命中/返回行数 |
| GET | `/api/stats` | 各日分区 active/superseded/deleted 文件数与行数、开关状态 |
| POST | `/api/compactions?day=YYYY-MM-DD` | 后台压实，202 立即返回；重复请求 409 |
| GET | `/api/compactions[/{id}]` | 任务历史 / 单任务状态（running/published/failed） |
| POST | `/api/demo/gates/merge?paused=true|false` | 暂停/恢复合并 |
| POST | `/api/demo/gates/query?paused=true|false` | 暂停/恢复查询（查询请求服务端等待，不报错不丢失） |
| POST | `/api/demo/generate?rows=&day=&turbines=` | 生成并直接上传虚构读数 |
| POST | `/api/exports?turbine=&day=` | 生成训练 CSV（201 = 已发布；409 = 同刻异值未发布；404 = 无数据） |
| GET | `/api/exports/options` | 页面下拉数据：当前 active 数据中的机组与 UTC 日 |
| GET | `/api/exports` | 导出登记列表（文件 ID、机组、日、输出/源行数、来源文件数） |
| GET | `/api/exports/{id}` | 单条导出详情，含来源文件 ID 列表 |
| GET | `/api/exports/{id}/download` | 下载已发布 CSV（`turbine,ts_utc,vibration,row_count`） |
| GET | `/health` | 健康检查，含本次启动恢复报告 |

## 正确性设计要点

### 读快照（不漏行 / 不重行 / 不中断）

1. 查询开始时在一个 `BEGIN IMMEDIATE` 事务内：读取当前全部符合条件的 active 文件，
   并写入 `snapshots` + `snapshot_files`，文件集合就此固定；
2. 查询全程只读取快照内的 Parquet 文件。压实发布是把新合并件登记为 active、
   旧批次件改为 **superseded（不删除）**，因此进行中的查询读到的文件内容不变；
3. 查询结束释放快照引用；引用归零后旧件才进入可回收集合，后台回收器删除磁盘文件。

### 压实与并发

- 压实任务先在 DB 占位 `running` 行（双重互斥：asyncio.Lock + DB 状态），
  再由后台任务执行；HTTP 请求立即返回 202，不阻塞上传与查询；
- 合并结果先写 `lake/_tmp/`，`os.replace` 原子改名进入日分区，
  然后在**单个 DB 事务**内完成「新件 active + 旧件 superseded + 任务 published」；
- 合并行数与元数据总行数校验不一致则拒绝发布；
- 暂停合并开关在「任务启动前」和「临时件发布前」两个安全检查点生效，
  暂停时任务回滚（删临时件、置 failed），不留半成品。

### 崩溃恢复（`CompactionManager.recover()`）

启动时依次执行：

1. 释放上一个进程遗留的在途读快照（否则旧件永远无法回收）；
2. 所有 `running` 压实任务 → 删除其登记的临时件、置 `failed`（已提交数据从不触碰）；
3. 清空 `_tmp/` 中所有未发布文件；
4. 扫描磁盘，删除元数据中不存在的孤儿 Parquet；
5. 立即执行一轮旧件回收，并启动周期回收器继续「续完旧件回收」；
6. 导出侧：清空 `exports/_tmp/`，删除 `exports/` 中没有登记记录的孤儿 CSV
   （已发布文件及其元数据保留，重启仍可下载）。

### 训练数据导出（`POST /api/exports`）

1. **受理**：在单个写事务内按机组 + UTC 日读取 active 文件并写读快照
   （`snapshots/snapshot_files`），此后上传、压实都不改变本次文件集合；
2. **读取**：只读快照内的 Parquet，按 `ts_utc` 升序排序；旧件因被快照引用不会被回收，
   回收器不会打断读取；
3. **合并**：同一时刻同值只保留一行，`row_count` 列记录原行数；
   同一时刻出现不同振动值时整体拒绝（409，`conflicts` 列出时刻与全部取值），
   本次不写文件、不登记；
4. **发布**：CSV 先写 `exports/_tmp/<id>.csv` 并 `fsync`，`os.replace` 原子改名到
   `exports/<id>.csv`，再在单个 DB 事务内登记导出记录、来源文件 ID 与源行数；
5. **失败语义**：登记失败会立即删除成品；改名与登记之间硬退出则由重启第 6 步清理
   无元数据的孤儿 CSV，因此任何时刻都不会留下可下载的残件；已登记文件永久可下载
   （下载接口按元数据定位，不依赖读快照）。

## 测试

```bash
pip install -r requirements.txt -r requirements-dev.txt
python tests/test_integration.py   # 59 项断言：校验/重复行/快照/并发/开关/崩溃恢复/持久化
python tests/test_http_smoke.py    # 完整 HTTP 流程（含暂停查询、训练数据导出与下载）
python tests/test_export.py        # 46 项断言：同刻合并/异值拒绝/快照隔离/失败清理/重启
python tests/test_crash_drill.py   # 真实多进程硬退出（os._exit）重启恢复演练
python tests/test_export_crash.py  # 导出在改名后、登记前硬退出，重启无残件且旧文件可下载
```

### 为什么 Parquet IO 不持 DB 写锁

`parquet_store.py` 不导入数据库模块；`service.py` / `compaction.py` 通过
`ThreadPoolExecutor` 执行全部 PyArrow 调用，DB 写事务（`db.tx()`）只包裹
几条元数据语句，持锁时间为毫秒级，上传与压实互不阻塞。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `VIBLAKE_DATA_DIR` | `/data` | 数据根目录（`lake/` + `meta.db` + `exports/`） |
| `VIBLAKE_MAX_BATCH_ROWS` | `10000` | 单批最大行数 |
| `VIBLAKE_VIBRATION_MIN` / `_MAX` | `-1000` / `1000` | 振动值合法范围 |
| `VIBLAKE_QUERY_LIMIT_MAX` | `50000` | 单次查询返回上限 |
| `VIBLAKE_EXPORT_CONFLICT_LIMIT` | `500` | 409 响应中最多列出的冲突时间点数（完整数见 `conflict_count`） |
| `VIBLAKE_COMPACT_DELAY_MS` | `0` | 测试钩子：压实发布前额外延时（毫秒），仅崩溃演练使用，生产保持 0 |
| `VIBLAKE_EXPORT_DELAY_MS` | `0` | 测试钩子：导出改名后、登记前延时（毫秒），仅导出崩溃演练使用，生产保持 0 |
