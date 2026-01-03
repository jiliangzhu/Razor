# Market Selection (Phase 1) — Frozen Spec v1 (v1.3.2a)

目标：在 Phase 1（dry-run / shadow）只选 **2 个 market**（1×Liquid 主样本 + 1×Thin 压力样本），用最小流量把 **统计口径 + 会计闭环 + reason 归因** 做扎实，避免“跑很多市场但结论不可审计”。

本文件冻结的是：**指标定义、`market_scores.csv` schema、以及选取规则**。后续实现 `market_select` 工具时必须严格遵守。

---

## 1) 非目标（写死）

- 不做下单/签名/OMS（Phase 1 禁止交易写操作）。
- 不追求“覆盖更多市场”，只做对照实验（2 个 market）。
- 不用主观描述来选 market，必须输出可审计指标与理由。

---

## 2) ID 口径（非常重要）

配置文件 `config.toml` 的 `[run].market_ids` 使用 **Gamma market id**（例如 `"516861"`）。

选市场工具必须同时输出：

- `gamma_id`：Gamma 的 `id`（写入 config 的值）
- `condition_id`：Gamma 的 `conditionId`（WS / data-api trades 使用的 market_id）
- `token_ids`：Gamma 的 `clobTokenIds`（用于 token 维度 book/trades）

---

## 3) 数据源与采样方式（冻结）

### 3.1 候选池（Gamma 粗排，省流量）

只用于缩小候选范围，不做最终判断。

- Endpoint：`GET {gamma_base}/markets?active=true&closed=false&limit=<N>`
- 过滤规则（硬过滤）：
  - `clobTokenIds` 可解析为数组
  - 只保留 `legs_n ∈ {2,3}`（Binary / Triangle）
  - `conditionId` 非空
- 排序（确定性）：按 `volume24hr` 降序（若字段缺失则当作 0）

### 3.2 短采样（最终判据必须来自 Phase1 口径）

对每个候选 market 执行短采样（默认 **固定 60 分钟**；实现时允许 CLI override，但必须在输出里显式记录 `probe_seconds`）：

- WS：订阅该 market 的所有 `token_id`，采集盘口事件（用于 tick/snapshot）
- Trades：轮询 data-api trades（只读），采集成交（用于活跃度与数据完整性）
- 时间戳域：沿用 Phase1（`TradeTick.ts_ms = ingest_ts_ms`）以保证与 shadow window 同域

**短采样偏差必须显式落盘**（用于 Day14 回看解释“为什么当时特别薄/特别活跃”，避免误调参）：

- `probe_hour_of_day_utc`：采样开始时刻的 UTC 小时（0..23）
- `probe_market_phase`：距离 `endDate` 的粗分段（来自 Gamma）：
  - `GT_7D`：距离到期 > 7 天
  - `D1_TO_D7`：1–7 天
  - `LT_24H`：< 24 小时
  - `UNKNOWN`：无法解析 `endDate`

---

## 4) 指标字段列表（冻结定义）

每个候选 market 计算 4 类指标：

### 4.1 身份与 Gamma 粗指标（仅用于粗排与可读性）

- `gamma_id`：Gamma market id（写入 config）
- `condition_id`：WS / trades market_id
- `legs_n`：腿数（2/3）
- `strategy`：`binary|triangle`
- `token0_id/token1_id/token2_id`：不足 3 腿则留空
- `gamma_volume24hr`：Gamma `volume24hr`（f64）
- `gamma_liquidity`：Gamma `liquidity`（f64）
- （可选）`question`：Gamma `question`（仅展示；CSV 需正确 quoting）

### 4.2 数据质量（是否“能跑通口径”）

以短采样窗口内的 **snapshot 样本** 作为分母。

实现口径冻结：为了控制流量并让不同 market 的 `snapshots_total` 可比，`market_select` 按固定采样间隔对“当前最新快照”取样计数（默认 1Hz）。因此：
- `snapshots_total` ≈ `probe_seconds / snapshot_sample_interval_seconds`（WS 稳定连接时）
- recommendation.json 中必须输出 `snapshot_sample_interval_ms` 作为审计锚点（避免误读为“WS publish 次数”）

- `snapshots_total`：发布的 snapshot 数
- `ticks_total`：tick 样本数（实现时可等同 snapshots_total；或分别统计）
- `one_sided_book_rate`：
  - 定义：满足 `best_bid<=0` 或 `best_ask<=0` 或 `best_ask>=1.0` 的样本占比
  - 目的：识别“book 不完整 / 只有一边”的市场或 WS 数据问题
- `bucket_nan_rate`：
  - 定义：`buckets::classify_bucket()` 产生 `BUCKET_THIN_NAN/BUCKET_LIQUID_NAN` 的样本占比
  - 目的：识别盘口缺失/NaN 导致 bucket 降级的市场
- `depth3_degraded_rate`：
  - 定义：`BucketMetrics.is_depth3_degraded == true` 的样本占比
  - 目的：识别无法正确计算 depth3 的市场（会污染 Liquid/Thin 对照）

Depth3 单位 sanity-check（冻结）：

- 目的：防止把 size 单位误读（份数 vs notional）导致 `depth3_usdc` 失真，从而扭曲 Liquid/Thin 分桶。
- 规则（任一满足即视为 degraded）：
  - `ask_depth3_usdc` 非有限数 / <= 0
  - `ask_depth3_usdc > 10_000_000`（明显不合理，强制降级）
- 触发时：
  - 将该样本计入 `depth3_degraded_rate`
  - 在 `recommendation.json` 的 `probe_warnings` 里追加 `DEPTH_UNIT_SUSPECT`（见 §7）

### 4.3 流动性桶分布（对照实验核心）

基于 Worst-leg bucket（与 Phase1 Brain/Shadow 共用同一实现）：

- `liquid_bucket_rate`：bucket==Liquid 的样本占比
- `thin_bucket_rate`：bucket==Thin 的样本占比（应满足 `1 - liquid_bucket_rate`，允许 NaN/缺失导致偏差）
- `worst_spread_bps_p50`：
  - 定义：`BucketMetrics.worst_spread_bps` 的 p50（整数 bps）
- `worst_depth3_usdc_p50`：
  - 定义：`BucketMetrics.worst_depth3_usdc` 的 p50（f64；若 degraded 则为 NaN，不参与分位数）

分位数算法（冻结）：对样本排序，取 `idx = floor((n-1)*q)`。

### 4.4 “会不会出样本”（Brain 口径的机会强度）

对每个 snapshot 计算 Brain 的 **冻结版公式**（不发 Signal，只统计是否“会通过门槛”）：

- `sum_ask = Σ best_ask_i`
- `raw_cost_bps = Bps::from_price_cost(sum_ask)`（成本侧 ceil，禁止用 floor）
- `raw_edge_bps = 10000 - raw_cost_bps`
- `hard_fees_bps = FEE_POLY + FEE_MERGE`（=210bps）
- `risk_premium_bps = cfg.brain.risk_premium_bps`
- `expected_net_bps = raw_edge_bps - hard_fees_bps - risk_premium_bps`
- `passes_min_net_edge = expected_net_bps >= cfg.brain.min_net_edge_bps`

统计指标：

- `snapshots_eval_total`：参与计算的 snapshot 数（排除 NaN/负数）
- `passes_min_net_edge_count`：通过门槛次数
- `passes_min_net_edge_per_hour`：`passes_min_net_edge_count / probe_seconds * 3600`
- `expected_net_bps_p50/p90/max`：对 `expected_net_bps.raw()` 做分位数/最大值（整数 bps）

### 4.5 Trades 活跃度（避免死市场）

以 `trades.csv` 中 `(condition_id, token_id)` 过滤后的 trade ticks 为样本。

- `trades_total`：采样窗口内 trades 数（去重后）
- `trades_per_min`：`trades_total / probe_seconds * 60`
- `trade_poll_hit_limit_count`：data-api 返回数量达到 `trade_poll_limit` 的次数（提示可能漏单）
- `trades_duplicated_count`：去重命中次数

Trades 完整性/连续性强校验（必须落盘到 `recommendation.json`，不进 `market_scores.csv`）：

- `poll_gap_max_ms`：相邻两次 **成功** poll 的最大间隔（本地时间差）
- `trade_gap_max_ms`：相邻两条 trade 的最大时间间隔（优先 `exchange_ts_ms`，否则 `ingest_ts_ms`）
- `trade_time_coverage_ok`：`trade_gap_max_ms <= 300_000`（5 分钟）则为 true，否则 false
- `estimated_trades_lost`（粗估下限，用于快速判断 “trades_total 可能只是下限”）：
  - 定义：`estimated_trades_lost = trade_poll_hit_limit_count * trade_poll_limit`
  - 注意：这是 **保守下限估计**（不保证真实丢失量等于该值），仅用于诊断是否存在“poll 机制漏抓/限流”风险。

> 解释：这不是为了证明“市场一定活跃”，而是为了防止把“漏抓/卡顿”误当成“市场没 trades”。

### 4.6 机会强度的“连续性”（防 burst 误选）

`passes_min_net_edge_per_hour` 只能说明“会不会出样本”，但不能说明“是不是持续出样本”。

必须额外在 `recommendation.json` 输出通过门槛事件的间隔统计（不进 `market_scores.csv`）：

- `passes_gap_p50_ms`：相邻两次 `passes_min_net_edge` 的时间间隔 p50
- `passes_gap_p90_ms`：p90
- `passes_gap_max_ms`：最大空窗（max gap）

---

## 5) `market_scores.csv` schema（冻结：列名+顺序）

实现工具必须输出 `market_scores.csv`，header 必须严格一致：

```
run_id,probe_start_unix_ms,probe_end_unix_ms,probe_seconds,gamma_id,condition_id,legs_n,strategy,token0_id,token1_id,token2_id,gamma_volume24hr,gamma_liquidity,snapshots_total,one_sided_book_rate,bucket_nan_rate,depth3_degraded_rate,liquid_bucket_rate,thin_bucket_rate,worst_spread_bps_p50,worst_depth3_usdc_p50,trades_total,trades_per_min,trade_poll_hit_limit_count,trades_duplicated_count,snapshots_eval_total,passes_min_net_edge_count,passes_min_net_edge_per_hour,expected_net_bps_p50,expected_net_bps_p90,expected_net_bps_max
```

字段格式冻结：

- `*_rate` 一律为 `f64`（0..=1，允许 NaN 但尽量避免）
- `*_bps_*` 一律输出整数 bps（`i32` 的十进制字符串）
- `*_usdc_*` 为 `f64`
- `token2_id` 对 binary 为空字符串

---

## 6) 选取规则（冻结：确定性选择 2 个 market）

目标：输出 2 个 market（`Liquid` 主样本 + `Thin` 压力样本），并生成 `suggest.toml`（仅建议，不自动改 config）。

### 6.1 通用硬门槛（不过直接淘汰）

候选 market 必须满足：

- `legs_n ∈ {2,3}`
- `snapshots_total >= 300`（30 分钟内平均每 6 秒至少 1 个 snapshot；可后续实现为 CLI 可配，但默认冻结）
- `trades_total >= 10`（避免死市场）
- `bucket_nan_rate <= 0.20`（否则 bucket 对照不可用）
- `passes_min_net_edge_count >= 1`（否则 Phase1 signals 大概率长期为 0）

### 6.2 Liquid 主样本选择（确定性排序）

从剩余候选中，筛：

- `liquid_bucket_rate >= 0.50`
- `one_sided_book_rate <= 0.30`

然后按以下 key 依次排序（降序），取第一名：

1) `passes_min_net_edge_per_hour`
2) `liquid_bucket_rate`
3) `trades_per_min`
4) `gamma_volume24hr`

### 6.3 Thin 压力样本选择（确定性排序）

从剩余候选中，筛：

- `thin_bucket_rate >= 0.70`
- `trades_per_min >= 0.2`

然后按以下 key 依次排序（降序），取第一名：

1) `passes_min_net_edge_per_hour`
2) `thin_bucket_rate`
3) `trades_per_min`
4) `gamma_volume24hr`

### 6.4 连续性建议（不改变排序，但必须在推荐理由里解释）

对 Liquid 主样本，必须在 `recommendation.json` 里输出并解释：

- 若 `passes_gap_max_ms` 过大（例如 > 30 分钟），即使排序第一，也必须在推荐理由中标注 `BURSTY_PASSES`，提示这是“间歇性样本”，可能不利于 Phase1 调参效率。

### 6.5 互斥规则

- Thin market 不能与 Liquid market 相同。
- 若要求“控制变量”，可启用 `prefer_strategy=binary|triangle`：两者必须同 strategy，否则拒绝并提示重新跑候选池。

### 6.6 产物（冻结）

- `suggest.toml`（写入 `config.toml` 需要的片段）：

```toml
[run]
market_ids = ["<gamma_id_liquid>", "<gamma_id_thin>"]
```

- `recommendation.json`：包含两者的核心指标快照（用于审计“为什么选它”）

---

## 7) `recommendation.json`（补充冻结：必含字段）

实现工具时，`recommendation.json` 至少必须包含以下键（允许额外字段）：

- `probe_hour_of_day_utc`
- `probe_market_phase`
- `probe_seconds`（本次采样实际时长，默认应为 3600）
- `poll_gap_max_ms`
- `trade_gap_max_ms`
- `trade_time_coverage_ok`
- `estimated_trades_lost`
- `passes_gap_p50_ms`
- `passes_gap_p90_ms`
- `passes_gap_max_ms`
- `bucket_after_degrade`：当 `depth3_degraded_rate > 0` 或出现 `DEPTH_UNIT_SUSPECT` 时，必须显式写出降级后的 bucket 策略；Phase 1 固定为 `"thin"`（强制 Thin）
- `probe_warnings`：字符串数组；至少可能包含：
  - `DEPTH_UNIT_SUSPECT`
  - `BURSTY_PASSES`

> 这些字段是为了让 Day14 回看时能解释“样本为什么稀疏/为什么特别薄”，避免把数据问题错当成策略问题。

---

## 8) 为什么这套规则适合 Phase 1（解释性，不参与实现）

- Liquid 样本：保证 `shadow_log.csv` 有足够“可结算”的行，用来验证会计口径与 reason 分布。
- Thin 样本：保证能逼出腿断/残渣处刑等尾部形态，用来验证 Day14 的 NO_GO 归因链条是否可信。

Day14 判决必须同时审阅 Liquid/Thin：

- Liquid 过、Thin 全崩：说明尾部风险/数据质量/会计锚点可能有问题，Phase 1 仍应判 NO_GO（至少不进入真钱 Phase 2）。
