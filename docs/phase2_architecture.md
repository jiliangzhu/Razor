# Phase 2 Architecture (Day 15–30) — 校准者 / 小额实盘（v1.3.2a）

状态：**规划文档（不等于已实现）**  
前置条件：**Phase 1 Day14 = GO**（只看 `shadow_log.csv` + `day14_report` 判决）  
非目标：本文件不更改 Phase 1 冻结口径；不讨论“扩大规模/多市场扫射/高频”。

> Phase 2 的唯一任务：把 Phase 1 的“影子会计”变成可验证的真实执行闭环，并用 **真实成交样本** 校准 `fill_share_*_p25`，证明 **实盘净利润 > 0** 才允许进入 Phase 3。

---

## 0) Frozen Invariants（不可谈判）

这些规则在 Phase 2 仍然是“公理”，实现不得绕开：

1. **单位安全**：费率/优势/预算全部走 `Bps` 强类型域；禁止用 `0.02` 这种 float 费率常量做算术。
2. **Worst-leg Bucket**：桶分配按“最薄腿原则”；Liquid 判据：`spread_bps_worst < 20 && depth3_usdc_worst > 500`。
3. **Budgeted Ladder**（追单预算）：`max_chase_bps = min(expected_net_bps / 2, 200bps)`（上限 200bps；宁可错杀不乱追）。
4. **Panic Flattening**（收敛型止损）：最多 3 档（例如 1% / 5% / 10%）+ 尝试次数上限；最终进入 `HardStop`（不退出进程，但不再交易）。
5. **Append-only 审计**：所有关键状态变化必须落盘（CSV/JSONL），禁止“只在内存里算过”。
6. **模式闸门**：默认 `dry_run`；进入实盘必须显式 `RAZOR_MODE=live` 且 `config.live.enabled=true`（Phase 2 初期建议先用 SIM 或小额开关逐步放开）。

---

## 1) Phase 2 目标（可验收）

### 1.1 能力闭环（必须跑通）
- Signal（Brain）→ 执行（OMS）→ 回执/成交（fills）→ 平仓（flatten）→ 落盘（trade_log）→ 复核（shadow 对照）→ 校准（p25 建议）。

### 1.2 校准闭环（必须可回填，但不自动改 config）
- 从真实成交得到 `real_fill_share` 样本（按桶 Liquid/Thin 分开）。
- 每桶样本数 ≥ 30 时输出 `calibration_suggest.toml`（p25），人工回填 `config.toml`。

### 1.3 风险边界（必须硬保证）
- 总资金上限（例如 $100）+ 每次信号最大风险暴露（notional 上限）+ HardStop。
- 任何“未知状态”（下单超时/回执不确定/成交查询失败）必须走 **更保守路径**：优先停止交易并 flatten，而不是“假设成功继续追”。

---

## 2) 模块边界（建议的代码组织）

> 不要求一次重构到位，但 Phase 2 的接口必须清晰可替换（SIM → LIVE）。

### 2.1 核心任务拓扑（Tokio）

```mermaid
graph TD
  WS["WS: Book/Ticks"] --> Feed["feed::run_*"]
  REST["REST: trades poll"] --> Feed

  Feed -->|watch: MarketSnapshot| Brain["brain::run"]
  Feed -->|mpsc: TradeTick| Shadow["shadow::run (对照)"]
  Brain -->|mpsc: Signal| Shadow

  Brain -->|mpsc: Signal| Oms["oms/sniper::run"]
  Feed -->|watch: MarketSnapshot| Oms
  Feed -->|mpsc: TradeTick (可选)| Oms

  Oms --> Exec["execution::Gateway (SIM/LIVE)"]
  Exec --> Oms

  Oms --> CSV_Trade["trade_log.csv"]
  Oms --> Calib["calibration::tracker"]
  Calib --> CSV_Calib["calibration_log.csv"]
  Calib --> TOML_Suggest["calibration_suggest.toml"]

  Shadow --> CSV_Shadow["shadow_log.csv"]
```

### 2.2 关键模块职责
- `brain`：只负责“发信号”，不做执行；Signal 必须携带会计锚点（limit_price、best_bid_at_signal、bucket、worst_leg_token_id）。
- `execution`（建议新增模块/trait）：抽象下单与查询成交（SIM 与 LIVE 共用接口）。
- `oms/sniper`：状态机（FSM），把 Signal 变成一组 order intents，并处理回执/成交/追单/平仓。
- `calibration`：按桶统计真实 `real_share`（分位数算法冻结：`idx=floor((n-1)*0.25)`），输出建议文件。
- `shadow`：Phase 2 建议继续并行跑（对照期望 vs 实际；用于审计“是不是 OMS 逻辑在偷换口径”）。

---

## 3) Phase 2 数据契约（Signal / Order / Fill / Position）

### 3.1 Signal（Brain → OMS）
最低必须字段（Phase 1 已具备则复用）：
- `signal_id`, `ts_ms`, `market_id`, `strategy`, `bucket`, `worst_leg_token_id`
- `expected_net_bps: Bps`（预算上限来源）
- `q_req: f64`
- `legs: Vec<SignalLeg>`（最多 3）：
  - `token_id`
  - `side`（Phase 2 必须 Buy/Sell；Phase 1 可固定 Buy）
  - `limit_price`
  - `best_bid_at_signal`（用于会计锚点与 flatten 基准）
  - （可选）`best_ask_at_signal`

### 3.2 OMS 内部订单语义（OrderIntent）
建议统一成“意图 + 结果”，每一次尝试都可落盘：
- `signal_id`, `attempt`, `leg_index`, `token_id`, `side`
- `price`, `qty`, `order_kind`（Phase 2 初期仅 IOC，避免撤单复杂性）
- `chase_budget_bps`（如果是 chase）

### 3.3 FillReport（Execution → OMS）
执行层必须返回：
- `requested_qty`, `filled_qty`, `avg_price`
- `status: None|Partial|Full`
- `order_id`（LIVE 必须；SIM 可固定字符串）
- `latency_ms`（可选，但建议写入日志便于定位）

### 3.4 PositionChunk（OMS → Flatten）
对“已成交仓位”显式建模，避免“忘记有残仓”：
- `token_id`, `side`（flatten 一律是反向）、`qty`, `entry_price`, `signal_id`, `leg_index`

---

## 4) OMS 状态机（FSM）— 可运行、可审计

> 目标：每个 signal 都能进入“终态”（成功成套 / 失败但 flatten 完毕 / hardstop）。禁止卡在中间状态不落盘。

### 4.1 核心状态（建议）
- `Idle`
- `FiringLeg1`
- `Leg1Filled`（含 partial）
- `Chasing`（对剩余腿做 Budgeted Ladder）
- `Flattening`（对 leftovers 做 PanicFlattening）
- `HardStop`
- `Cooldown`

### 4.2 Leg1 选择（最薄腿优先）
执行时的 leg 顺序必须与 bucket 的 worst-leg 一致：
- Leg1 = worst leg（最难成交的一腿先打）
- Leg2/Leg3 = 剩余腿（顺序固定：按 index 升序或 depth3 升序，确保确定性）

### 4.3 Budgeted Ladder（冻结）
仅在 Leg1 有成交后进入 chase：
- 若 `expected_net_bps <= 0`：禁止 chase，直接进入 Flattening（更诚实）。
- 预算：
  - `max_chase_bps = min(expected_net_bps / 2, 200bps)`
- 两档追单（IOC）：
  - `L1`: `best_ask * (1 + 10bps)`
  - `L2`: `best_ask * (1 + max_chase_bps)`
- 任一腿 L2 后仍无法补齐成套数量 → Flattening

### 4.4 Panic Flattening（冻结）
对所有 leftovers 做反向 IOC（最多 3 档）：
- `attempt1`: `best_bid * (1 - 1%)`
- `attempt2`: `best_bid * (1 - 5%)`
- `attempt3`: `best_bid * (1 - 10%)`
- attempt3 后仍未平完 → `HardStop`（停止交易，保留数据采集与 shadow 对照）

### 4.5 Backpressure / 幂等
- Signal channel 满：允许丢信号，但必须 `warn` + 计数。
- 同一个 `signal_id` 不允许重复执行：OMS 必须维护 `processed_signal_ids`（LRU/TTL），重复直接忽略并落盘一条 `DEDUP_SIGNAL`。

---

## 5) Execution Gateway（SIM → LIVE 可替换）

### 5.1 抽象接口（建议）
- `place_ioc(token_id, side, price, qty) -> OrderAck`
- `poll_fills(order_id, timeout, max_tries) -> FillSummary`
- （可选）`get_best_quotes(token_id)`（若 OMS 需要更“近端”的报价）

### 5.2 SIM 执行（Phase 2 早期必须有）
SIM 必须 **可解释、可重复、可故障注入**：
- 成交量模型：`filled_qty = min(req_qty, best_size * sim_fill_share_bucket)`（仅当限价越过盘口）
- 故障注入开关：例如 `RAZOR_SIM_FORCE_CHASE_FAIL=1` 强制 chase 失败以覆盖 Flatten/HardStop 分支。

### 5.3 LIVE 执行（Phase 2 后期）
上线前的安全闸门：
- `RAZOR_MODE=live` 且 `config.live.enabled=true` 才允许调用真实下单接口。
- 所有 HTTP 必须：
  - timeout（例如 3s）
  - 有限重试（≤2）
  - 严禁无限循环 silent retry

---

## 6) 落盘与审计（Phase 2 关键：能复盘能追责）

> Phase 2 不允许“只有最终结果”。必须能回答：哪一腿先打、为什么 chase、在哪一档 flatten、为何 hardstop。

### 6.1 trade_log.csv（建议冻结为 Phase2 v1）
每次下单尝试写一行（intent + result），字段建议：
- 时间：`ts_ms`, `run_id`, `mode=live|sim`
- Signal：`signal_id`, `market_id`, `strategy`, `bucket`, `expected_net_bps`
- Intent：`action=FIRE|CHASE|FLATTEN`, `attempt`, `leg_index`, `token_id`, `side`, `limit_price`, `req_qty`
- Result：`order_id`, `ack_status`, `filled_qty`, `avg_price`, `fill_status`, `result=FULL|PARTIAL|NONE|HARDSTOP`
- Note：`reason_codes`（枚举化，便于统计）

### 6.2 calibration_log.csv + calibration_suggest.toml
每次订单都写 calibration 样本（哪怕 `filled_qty=0`，也有信息量）：
- `real_share = filled_qty / req_qty`（req_qty>0）
- 按 bucket 分桶统计 p25，并写入 `calibration_suggest.toml`（覆盖写完整文件；不自动改 config）。

### 6.3 运行目录（run_dir）
沿用 Phase 1 的 run_dir 机制：
- config 快照、run_meta、schema_version、所有 CSV/JSONL
- Phase 2 增加：execution/logs、calibration 输出

---

## 7) 校准闭环（fill_share p25）— 口径与陷阱

### 7.1 样本定义（建议冻结）
Phase 2 的 `real_share` **不等价于** Phase 1 的 `V_mkt * fill_share`，但必须可对照：
- `real_share_req = filled_qty / req_qty`（最稳：不依赖 trades 统计）
- 若能可靠得到 window 市场成交量 `V_mkt`，可额外记录：
  - `real_share_mkt = filled_qty / V_mkt`（更贴近 Phase 1 fill_share 语义）

建议：两者都记录到 calibration_log（以免“抓 trades 口径偏差”污染校准）。

### 7.2 分位数算法（冻结）
对样本排序 asc：
- `idx = floor((n - 1) * 0.25)`
- `p25 = values[idx]`

### 7.3 输出规则（冻结建议）
- `min_samples_per_bucket = 30`
- 未满足样本数：输出 `insufficient_samples`，禁止用小样本更新参数。

---

## 8) 风险控制与 HardStop（Phase 2 生命线）

### 8.1 HardStop 触发（建议）
任一满足即进入 HardStop（停止交易，继续采集数据与 shadow）：
- flatten attempt3 后仍有残仓（无法完全平掉）
- 下单/查单连续超时或 API 错误率过高
- 发现“状态不一致”（例如认为无仓但余额显示有仓；或成交查询缺失）

### 8.2 Cooldown（必须）
每次处理完一个 signal（无论成功/失败/flatten）进入冷却：
- per-market cooldown（例如 1s~5s）
- 避免同一 market 在波动时疯狂重复触发

---

## 9) Phase 2 验收（建议的可操作清单）

### 9.1 SIM 阶段（先把 FSM 跑通）
- `RAZOR_MODE=live` + `config.live.enabled=false`：
  - 只跑 SIM execution（绝不触发真实下单）
  - `trade_log.csv` 必须出现：FIRE / CHASE / FLATTEN / COOLDOWN
  - 故障注入 `RAZOR_SIM_FORCE_CHASE_FAIL=1` 必须稳定覆盖 Flatten/HardStop

### 9.2 LIVE 小额阶段（再谈真单）
只在满足以下条件才允许开启：
- Day14 = GO
- SIM FSM 连续运行 24h 无崩溃、无内存增长
- 校准输出稳定（至少一个桶样本 ≥ 30，p25 不离谱）

---

## 10) Open Questions（实现前必须回答）
- Polymarket LIVE 下单需要哪些签名/鉴权材料？如何保证私钥不落盘（建议 Keychain/环境注入，进程内最小驻留）？
- 是否能稳定获得“成交明细 + 均价 + 成交量”以完成对账与校准？
- 多腿同时追单的并发策略：串行更稳但慢；并行更快但 legging 风险更大。Phase 2 先串行（最薄腿优先）更符合“宁可错杀”。

