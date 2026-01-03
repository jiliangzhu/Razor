# Project Razor — 项目说明（Phase 1 Dry-Run 为主）

本文件是对当前仓库代码的“可执行说明书”：覆盖项目目标、模块划分、主调用链（谁调用谁）、配置项、数据落盘含义、以及各个 CLI 工具的用途与输入/输出。

> 重要约束（代码已实现的安全门）
> - 默认只跑 Phase 1：`RAZOR_MODE=dry_run`
> - `RAZOR_MODE=live` 对应 **live_sim** 路径（Sniper/FSM + Calibration）：默认 `config.live.enabled=false` 只做 SIM 成交；若 `config.live.enabled=true` 需要 `RAZOR_LIVE_CONFIRM=1` 才能启动（仍然不会真实发单，`POST /order` 尚未实现）。
> - 费率/阈值/优势统一用 `Bps` 强类型，避免 0.02 vs 200 的单位事故。
> - Phase 1 的判死/判活来自 **shadow 会计** 与 **Day14 报告**，不依赖人工解释。

---

## 1) 仓库结构总览

```
config/                 # 示例/默认配置
data/                   # 运行产物（每次 run 独立 run_dir）
docs/                   # 设计/冻结口径/说明文档
src/                    # 核心 Rust 代码（razor + tools）
tests/                  # 单测（冻结 header/公式/分位数等）
```

关键文档：
- `AGENTS.md`：执行规则与冻结约束（Phase 1 禁写交易）
- `SPEC_FREEZE.md`：冻结声明（公式/判据不允许随意改）
- `docs/architecture.md`：Phase 1 冻结口径与“Day14 判决逻辑”
- `docs/market_selection.md`：`market_select` 口径与字段冻结

---

## 2) 如何运行（最常用）

### 2.1 Phase 1 Dry-Run（主链路）

```
RAZOR_MODE=dry_run cargo run -- --config config/config.toml
```

程序启动后会创建新的 run 目录，并更新：
- `data/run_latest/` → 指向最新 run 目录（Unix 下为 symlink）
- 以及 `data/<file>.csv` → 指向 `data/run_latest/<file>.csv` 的便捷 symlink（`src/main.rs`）。

### 2.2 Live-Sim（Phase 2 的 FSM/校准链路联调；仍然安全）

```
RAZOR_MODE=live cargo run -- --config config/config.toml
```

说明：
- `RAZOR_MODE=live` 会走 `Mode::LiveSim` 分支：启动 `Brain + Shadow + Sniper(SIM) + Calibration`。
- 默认 `config.live.enabled=false`：Sniper 使用 `ExecutionGateway::Sim`（只做可重复的模拟成交，不需要任何 key）。
- 若你要验证 “Polygon 私钥 → CLOB auth/api-key → 构造签名订单”的链路，可在 `config.toml` 里设 `live.enabled=true`，并设置：
  - `RAZOR_LIVE_CONFIRM=1`（启动安全门）
  - `${POLYGON_PRIVATE_KEY}`（或 `live.private_key_env` 指定的 env 变量名）
  - 注意：当前实现**仍不会**调用 `POST /order`（`execution.rs` 会 warn 并跳过），不会产生真实成交。

### 2.3 Day14 报告（对单次 run 进行统计）

```
cargo run --bin day14_report -- --data-dir data/run_latest
```

### 2.4 Market 选择工具（从 Gamma 候选池短采样选 2 个市场）

```
cargo run --bin market_select -- --config config/config.toml --probe-seconds 3600 --pool-limit 200 --prefer-strategy any
```

---

## 3) 运行目录（run_dir）与可复现性

每次启动 `razor` 都会创建独立目录（`src/run_context.rs`）：

```
data/run_YYYYMMDD_HHMMSS_<rand6>/
```

run_dir 内的关键文件（文件名在 `src/schema.rs` 冻结）：
- `config.toml`：本次运行使用的 config 快照（原文复制）
- `schema_version.json`：schema 版本与各文件版本映射
- `meta.json`：进程级 meta（host/pid/git_commit 等）
- `run_meta.json`：run 级 meta（run_id、schema_version、trade_ts_source 等）
- `raw_ws.jsonl`：原始 WS 消息（滚动写入，带 rotation/keep 策略）
- `ticks.csv`：按 token 的 top-of-book（bid/ask + depth3）落盘
- `snapshots.csv`：按 market 的快照采样（每秒/可配置）
- `trades.csv`：data-api 轮询 trades 落盘（带 ingest_ts/exchange_ts）
- `shadow_log.csv`：一行一个 signal 的完整影子会计分录（冻结 header）
- `trade_log.csv`：live_sim 下 Sniper 的 OMS 行为日志（dry_run 下可能不存在/为空）
- `calibration_log.csv`：live_sim 下校准样本日志（dry_run 下可能不存在/为空）
- `calibration_suggest.toml`：live_sim 下达到样本阈值后生成的 p25 建议值（只写建议）
- `health.jsonl`：心跳/限流/命中 limit 等运行健康事件
- `report.json` / `report.md`：程序退出时生成的汇总报告（不等同于 day14_report 输出，但字段接近）

---

## 4) 主调用链（Phase 1）—— 谁调用谁

### 4.1 入口：`razor` 二进制

入口文件：`src/main.rs`

主流程（简化）：

1. 解析 CLI：`Args::parse()`（`--config`，可选 `--mode`）
2. 读取配置：`toml::from_str` → `config::Config`
3. 配置自检：`Config::validate()`（窗口/retention/fill_share/bps 范围等）
4. 初始化 run_dir：`run_context::create_run_context()`
5. 写入 meta：
   - `schema::write_schema_version_json()`
   - `recorder::write_run_config_snapshot()`
   - `recorder::write_run_meta_json()` + `run_meta::RunMeta::write_to_dir()`
6. 拉取 market 定义：`feed::fetch_markets()`（Gamma → conditionId + tokenIds）
7. 初始化 channel：
   - `trade_tx/trade_rx: mpsc::Sender<TradeTick>`（trades 流）
   - `snap_tx/snap_rx: watch::Sender<Option<MarketSnapshot>>`（最新快照）
8. 启动后台任务（tokio tasks）：
   - `health::spawn_health_writer()` → 写 `health.jsonl`
   - `feed::run_market_ws()` → WS 消息 → `raw_ws.jsonl` + `ticks.csv` + 发布 `MarketSnapshot`
   - `snapshot_logger::run_snapshot_logger()` → 采样写 `snapshots.csv`
   - `feed::run_trades_poller()` → data-api poll → `trades.csv` + 发送 `TradeTick`
9. Mode 分支：
   - `dry_run`：`brain::run()`（消费 snapshot → 产出 Signal） + `shadow::run()`（消费 trades+signals → shadow_log）
   - `live_sim`：`brain::run()` + `shadow::run()` + `sniper::run()`（OMS/FSM；默认 SIM 成交）+ `calibration::run()`（p25 建议）
10. Ctrl-C / task 退出后：
   - 请求 shutdown（`graceful_shutdown`）
   - `report::generate_report_files()` 生成 `report.json`/`report.md`
   - `recorder::RecorderGuard::flush_all()` 强制落盘 flush/sync

### 4.2 并发与数据通道

- WS 线程只负责“读 + 落盘 + 更新最新快照”，不做策略判断。
- Brain 只看最新快照（watch），发 signal（mpsc）。
- Shadow 用内存 `TradeStore` 做 ring buffer，在固定窗口内按 `(market_id, token_id)` 统计成交量，再按冻结公式结算。

---

## 5) 核心模块说明（Phase 1）

### 5.1 `src/types.rs`（单位体系 + 核心数据结构）

关键类型：
- `Bps(i32)`：basis points 强类型
  - `Bps::FEE_POLY = 200`、`Bps::FEE_MERGE = 10`
  - `Bps::from_price_cost()`（ceil，成本/门槛侧，避免虚高 edge）
  - `Bps::from_price_proceeds()`（floor，展示/收益侧）
- `MarketDef { market_id, token_ids }`：从 Gamma 拉取的市场定义（market_id=conditionId）
- `MarketSnapshot { market_id, legs: Vec<LegSnapshot> }`：按 market 的快照
- `TradeTick { ts_ms, ingest_ts_ms, exchange_ts_ms, market_id, token_id, price, size, trade_id }`
- `Signal`：Brain 输出给 Shadow 的信号，包含会计锚点：
  - `signal_ts_ms`（本地 ms，作为 shadow window anchor）
  - `bucket`、`bucket_metrics`、`legs[*].limit_price/best_bid_at_signal/best_ask_at_signal`
  - `raw_cost_bps/raw_edge_bps/expected_net_bps`（Bps 域）

### 5.2 `src/schema.rs`（文件名 + CSV header 冻结）

集中定义：
- `SCHEMA_VERSION = "1.3.2a"`
- `FILE_*` 常量（run_dir 内文件名）
- `TRADES_HEADER` / `SNAPSHOTS_HEADER` / `SHADOW_HEADER`（严格冻结）

### 5.3 `src/recorder.rs`（落盘基础设施）

- `CsvAppender::open(path, header)`：append-only，首次创建写 header；若 header 不匹配会把旧文件 rotate 为 `*.schema_mismatch_*`
- `JsonlAppender`：用于 `raw_ws.jsonl` / `health.jsonl`，支持 rotation + keep
- `RecorderGuard`：进程退出时对关键文件 `sync_all()`，尽量减少 Ctrl-C 造成的半行/丢尾部数据

### 5.4 `src/feed.rs`（数据采集：WS + trades poller）

#### WS：`run_market_ws(cfg, markets, snap_tx, ticks_path, raw_ws_path, ...)`
- 从 `cfg.polymarket.ws_base` 连接 WS
- 订阅所有 token_id
- 每条 WS 文本：
  - 追加写 `raw_ws.jsonl`
  - 解析 `book`/`price_change` 事件：
    - 写 `ticks.csv`
    - 更新 market 内部状态
    - 当所有腿都 ready 时发布 `MarketSnapshot` 到 `snap_tx`

#### Trades：`run_trades_poller(cfg, markets, trade_tx, trades_path, ...)`
- 轮询 `GET {data_api_base}/trades?market=<conditionId>&limit=...`
- 每条 trade：
  - 必须有 `token_id`（asset_id），且必须属于当前 market 的 token set（避免跨市场污染）
  - 生成稳定 `trade_id` 去重（基于 tx + token + ts + price_bits + size_bits）
  - 写 `trades.csv`
  - `trade_tx.try_send()`（满则丢弃，并计数 dropped）
- 若每次 poll 返回条数达到 `trade_poll_limit`，会写 health 事件 `TradePollHitLimit`（可能漏单）

### 5.5 `src/buckets.rs`（Worst-leg 分桶）

入口：`classify_bucket(snapshot) -> BucketDecision`
- 对每腿计算 depth3（USDC）与 spread（bps）
- 选择 depth3 最小腿为 worst-leg
- 判据（冻结）：
  - `spread_bps_worst < 20` 且 `depth3_usdc_worst > 500` → `Liquid`
  - 否则 `Thin`
- 若 depth3 单位可疑/NaN，则强制 Thin 并附加 reason（例如 `DEPTH_UNIT_SUSPECT`/`BUCKET_THIN_NAN`）

### 5.6 `src/brain.rs`（Net-Edge Brain：只发信号）

入口：`brain::run(cfg, run_id, markets, snap_rx, signal_tx, ...)`
- watch 驱动：`snap_rx.changed().await`
- `eval_snapshot()`：
  - `raw_cost_bps = Bps::from_price_cost(sum(best_ask))`
  - `raw_edge_bps = 10000 - raw_cost_bps`
  - `hard_fees_bps = FEE_POLY + FEE_MERGE`
  - `expected_net_bps = raw_edge - hard_fees - risk_premium`
- 去重与冷却：
  - key = `(market_id, strategy, rounded_cost_bps)`（2bps 粗粒度）
  - cooldown 内相同 key 直接 suppress，并计数 `signals_suppressed`
  - 还有 TTL prune，避免 HashMap 无界增长
- 输出 `Signal` 时固化会计锚点字段，Shadow 不允许“用未来的 bid”

### 5.7 `src/trade_store.rs`（Shadow 用 ring buffer）

- `TradeStore::push(TradeTick)`：
  - retention 时间清理 + max_trades 硬上限
  - trade_id 去重（重复计数）
  - 若 out-of-order，启用 full-trim fallback
- `volume_at_or_better_price(market_id, token_id, start, end, limit)`：
  - 严格按 `(market_id, token_id)` + 时间窗口 + `price<=limit` 聚合 size
- `window_stats(market_id, start, end)`：
  - trades_in_window、max_gap_ms（按 ts 排序后计算）、max_trade_size、max_trade_notional

### 5.8 `src/shadow.rs`（Shadow Accounting：成套会计 + 残渣处刑）

入口：`shadow::run(cfg, markets, trade_rx, signal_rx, shadow_path, ...)`

核心行为：
- 接收 trades → push 到 `TradeStore`
- 接收 signals → 放入 pending
- 定时（50ms）检查是否到 window_end，满足则结算并写一行 `shadow_log.csv`

冻结会计（Matched Set Profit - Leftover Dump Loss）：
1. 对每腿 i：
   - `V_mkt_i = sum(size where price<=p_limit_i && ts in window && token match)`
   - `Q_fill_i = min(q_req, V_mkt_i * fill_share_p25(bucket))`
2. 成套：
   - `Q_set = min_i(Q_fill_i)`
3. 成套 PnL：
   - `Cost_set = Q_set * sum_i( FEE_POLY.apply_cost(p_limit_i) )`
   - `Proceeds_set = Q_set * FEE_MERGE.apply_proceeds(1.0)`（固定按 1.0）
   - `PnL_set = Proceeds_set - Cost_set`
4. 残渣处刑：
   - `ExitPrice_i = best_bid_at_signal_i * 0.95`
   - 若 bid 缺失/<=0：ExitPrice=0，reason=`MISSING_BID`（更保守、更诚实）
5. 写 `shadow_log.csv`（header 冻结，notes 为 reason code 列表）

### 5.9 `src/reasons.rs`（notes reason code 枚举化）

- `ShadowNoteReason`：所有 reason code 在此锁死
- `format_notes(reasons) -> String`：稳定排序去重后用逗号连接，例如：
  - `NO_TRADES,MISSING_BID`
- `parse_notes_reasons(notes)`：Day14/report 工具用于聚合统计

### 5.10 `src/report.rs`（run 退出时生成 report.json/md）

- `report::generate_report_files(run_dir, run_id, thresholds)`
  - 读取 `shadow_log.csv`
  - 计算 totals、by_bucket、by_strategy、worst_20
  - 写 `report.json` 与 `report.md`

### 5.11 `src/sniper.rs`（Phase 2：OMS/FSM（当前仅 SIM + live-auth dry-run））

- `sniper::run(...)` 只在 `RAZOR_MODE=live`（live_sim）时启动。
- `live.enabled=false`：使用 `ExecutionGateway::Sim`（按盘口 size × sim_fill_share 成交，可复现；支持故障注入 `RAZOR_SIM_FORCE_CHASE_FAIL=1`）。
- `live.enabled=true`：加载 Polygon 私钥 env，走 CLOB auth/api-key 派生，构造签名订单与 HMAC headers（但不会 `POST /order`）。

### 5.12 `src/execution.rs` / `src/clob.rs` / `src/clob_order.rs` / `src/eth.rs`（Phase 2：签名与鉴权基础设施）

- `execution.rs`：统一接口 `ExecutionGateway::{Sim,Live}`，对 Sniper 提供 `place_ioc()`。
- `clob.rs`：实现 Polymarket CLOB 的 L1/L2 headers（地址签名 + HMAC），并通过 `/auth/api-key` 派生 API creds。
- `clob_order.rs`：实现 CLOB exchange order 的 EIP-712 签名（tick_size/fee_rate/salt 参与）。
- `eth.rs`：Keccak256、EIP55 address、recoverable signature 等基础函数。

---

## 6) 数据文件说明（业务含义）

> 所有 CSV 的 header 均由 `src/schema.rs` 冻结；程序启动时会写入 `schema_version.json` 用于审计/回放。

### 6.1 `raw_ws.jsonl`
- 原样记录 WS 收到的文本（每行一个 JSON/或 PONG）
- 主要用于：
  - “raw_ws 有数据但 ticks/snapshots 不增长”的排查
  - 回放/解析升级前的证据留存

### 6.2 `ticks.csv`
header（见 `src/recorder.rs` 的 `TICKS_HEADER`）：
- `ts_recv_us`：本地接收时间（微秒）
- `market_id`：conditionId（通过 token→market 映射校正）
- `token_id`
- `best_bid`, `best_ask`
- `ask_depth3_usdc`：top3 asks 的 `price*size` 求和（USDC）

用途：盘口质量/分桶/回放诊断。

### 6.3 `snapshots.csv`
按 market 汇总的快照采样（默认 1s，`run.snapshot_log_interval_ms`）：
- 每行包含 market_id、legs_n、每腿 token_id/bid/ask/depth3

用途：离线回放（`razor_replay`）、market_select probe 指标来源。

### 6.4 `trades.csv`
header（`src/schema.rs::TRADES_HEADER`）：
- `ts_ms`：Phase1 冻结域（本地 ingest time，用于 shadow window 对齐）
- `ingest_ts_ms`：同上（冗余字段，保兼容）
- `exchange_ts_ms`：交易所时间（若可解析），仅用于诊断/去重
- `market_id`（conditionId）、`token_id`（asset_id）、`price`、`size`、`trade_id`

用途：Shadow 的 `V_mkt` 统计、poll hit limit 的漏单诊断、离线回放/对账。

### 6.5 `shadow_log.csv`
**一行一个 signal 的完整会计分录**（header 冻结见 `src/schema.rs::SHADOW_HEADER`）：
- signal 元信息：run_id/schema_version/signal_id/signal_ts/window/market/strategy/bucket/worst_leg_token_id
- 请求与填充：q_req/legs_n/q_set + 每腿 token_id/p_limit/best_bid/v_mkt/q_fill
- 会计：cost_set/proceeds_set/pnl_set/pnl_left_total/total_pnl
- 风险指标：q_fill_avg/set_ratio
- 参数落地：fill_share_p25_used/dump_slippage_assumed
- `notes`：枚举化 reason code（逗号分隔），用于 Day14 按原因聚合

### 6.6 `health.jsonl`
每 10 秒 heartbeat 一条 + 若 poll hit limit 会追加事件：
- 目的：长时间挂机时判断是否“活着”、是否漏抓、是否 backpressure

### 6.7 `report.json` / `report.md`
进程退出时生成的汇总报告（便于快速浏览 run 结果；最终 Day14 判决仍建议用 `day14_report` 输出）。

### 6.8 `trade_log.csv`（仅 live_sim：OMS 行为日志）

header（见 `src/schema.rs::TRADE_LOG_HEADER`）：
- 一行记录一次 Sniper 动作（FIRE_LEG1 / CHASE / FLATTEN / COOLDOWN / HARDSTOP / DEDUP_HIT）
- 包含：signal_id、market_id、bucket、leg_index、token_id、side、limit_price、req_qty、fill_qty、fill_status、expected_net_bps、notes

用途：验证 FSM 分支是否跑通、是否有 backpressure/去重/冷却命中、以及“何时进入 flatten/hardstop”。

### 6.9 `calibration_log.csv` / `calibration_suggest.toml`（仅 live_sim：fill_share p25 校准闭环）

- `calibration_log.csv`：每次下单（SIM 或未来真实）落一行样本，核心字段是 `filled_qty/req_qty`，并按 bucket 分桶。
- `calibration_suggest.toml`：当样本数达到阈值后输出 p25 建议值（仅写建议，不会自动修改 `config.toml`）。

---

## 7) CLI 工具（二进制）清单

### 7.1 `razor`（主程序）
- 入口：`src/main.rs`
- 命令：
  - `RAZOR_MODE=dry_run cargo run -- --config config/config.toml`
  - `RAZOR_MODE=live cargo run -- --config config/config.toml`（live_sim：Sniper/FSM + Calibration；默认仍为 SIM 成交）

### 7.2 `day14_report`（Day14 判决 + reason 分组统计）
- 入口：`src/bin/day14_report.rs`
- 默认读取：`data/run_latest/shadow_log.csv`
- 输出：终端打印（包含按 reason/bucket/strategy 分组与 tail 20）

### 7.3 `market_select`（短采样选 2 个 market）
- 入口：`src/bin/market_select.rs`
- 输出目录：`data/market_select/<run_id>/`
  - `market_scores.csv`（冻结 schema）
  - `recommendation.json`
  - `suggest.toml`（写 `[run].market_ids = [...]`，只建议不改 config）
- 说明：probe 过程中会增量追加 `market_scores.csv`（已完成 market 的行）；Ctrl-C 也会写出“部分结果 + recommendation.json”，便于长跑中断后复盘。

### 7.4 `razor_replay`（离线回放：用 snapshots+trades 重算信号与 shadow）
- 入口：`src/bin/razor_replay.rs`
- 输入：某次 run_dir（需含 `snapshots.csv`/`trades.csv`/`config.toml`）
- 输出：`<run_dir>/replay/`（replay_shadow_log + replay_report）

### 7.5 `shadow_sweep`（扫描 fill_share/dump_slippage 的网格敏感性）
- 入口：`src/bin/shadow_sweep.rs`
- 输入：`shadow_log.csv`
- 输出：`sweep_scores.csv` + `best_patch.toml` + `sweep_recommendation.json`

### 7.6 `run_compare`（多次 run 对比）
- 入口：`src/bin/run_compare.rs`
- 输出：runs_summary.csv（按 bucket/reason 的对比汇总）

### 7.7 `brain_sweep` / `dataset_split`
- `brain_sweep`：对历史数据做参数 patch 试跑与最优 patch 输出
- `dataset_split`：把 shadow_log 按天切分并生成 walk-forward 结构（用于回测/对比）

---

## 8) 典型排查路径（最常见问题）

### 8.1 raw_ws 有数据，但 ticks/snapshots/signal 不增长
优先看：
- `health` 日志里的 `last_tick_ingest_ms` 是否在跳
- `raw_ws.jsonl` 中 event_type 是否为 `book` 或 `price_change`
- token_id 是否能映射到 market（映射来自 Gamma 的 token_ids）

### 8.2 signals 长期为 0
常见原因：
- market 太薄/盘口一侧缺失导致 `best_ask=1.0`（保守设置），`sum_ask` 经常 ≥1 → edge<=0
- `min_net_edge_bps` 过高、`risk_premium_bps` 过高
- snapshot stale 被跳过（`brain.max_snapshot_staleness_ms` 太小）

### 8.3 shadow_log 里大量 NO_TRADES / WINDOW_EMPTY
优先看：
- `trades.csv` 是否增长
- `health.jsonl` 是否出现 TradePollHitLimit（可能漏单）
- `trade_poll_interval_ms` 是否过大（漏 burst）

---

## 9) 开发与测试

```
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
