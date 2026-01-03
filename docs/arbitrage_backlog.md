# Razor Backlog — Replay / Sweep / Stress (Borrowed from @the_smart_ape)

更新时间：2025-12-31  
目标：把 `docs/the_smart_ape.md` 里的“工程方法论”（自建数据集 + 多参数回测 + 明确承认回测局限）转成 Razor 的**可执行 backlog**，用于持续提升套利能力与 Day14 判决可信度。

> 本 backlog 以 **Frozen Spec v1.3.2a** 为前提：任何会改变 Brain/Shadow 公式含义、或更改已冻结 CSV schema 的事项，都必须单独开 “Spec bump PR” 并写迁移说明。

---

## 0) 背景：我们从 smart_ape 借鉴什么

smart_ape 的核心价值不在“具体策略”，而在工程闭环：

1) **历史数据不可靠 → 自己录数据集**（可回放、可复现）  
2) **参数决定生死 → 多参数对比跑分**（不是靠感觉调参）  
3) **承认回测局限 → 把局限量化为风险项**（避免自欺）  

Razor 当前已具备基础材料（`run_dir`、`ticks.csv`、`trades.csv`、`shadow_log.csv`、`day14_report`、reason codes）。本 backlog 的任务是把这些材料升级为：**可回放 / 可 sweep / 可 stress / 可对比**。

---

## 1) Backlog 工作原则（写死）

1) **确定性**：所有排序/分位数/筛选必须确定性；遇到 NaN 一律视为最小值；tie-break 用 id 字典序。  
2) **审计优先**：宁可输出更多“诊断文件”，也不要在内存里算完就丢。  
3) **Frozen outputs**：任何新 CSV 都必须冻结 schema（列名+顺序），并且要有 header。  
4) **工具不改 config**：工具只能输出 `suggest.toml` / `patch.toml`（建议文件），禁止自动覆盖 `config.toml`。  
5) **Phase1 安全门不动**：Phase1 仍只 dry-run；工具只读网络/只写本地文件。  

---

## 2) 里程碑总览（按优先级）

> 约定：每个里程碑都以一个 PR 交付（小步可验收）；每个 PR 必须有 `cargo test` 覆盖核心计算。

### P0（立刻做：直接提升调参效率 / 复现能力）

**M0 — market_select Ctrl‑C 也产出部分结果（checkpoint）**  
**M1 — Shadow Ledger Sweep：用 `shadow_log.csv` 做 fill_share/dump 的参数 sweep**  
**M2 — Day14 Report Stress：在不改变 verdict 的前提下输出 stress 视角**  
**M3 — Run Compare：多 run_dir 横向对比（找“今天为什么死了”）**

### P1（中期做：让 Brain 阈值也能离线调参）

**M4 — Full Snapshot 录制（可控采样率）：新增 `snapshots.csv`（不改现有 schema）**  
**M5 — Offline Replay：离线重放 snapshots+trades 重算 brain/shadow（验证可复现）**  
**M6 — Brain Threshold Sweep：在离线 replay 上做 min_net_edge/risk_premium/cooldown sweep**

### P2（长期做：把“回测局限”系统性纳入风险评估）

**M7 — Execution Stress Harness：延迟/掉包/限流/盘口缺档 的可控故障注入（SIM only）**  
**M8 — Walk‑forward/分日评估：避免参数过拟合（Day14 更可信）**

---

## 3) 里程碑详解（可直接照此开发）

### M0 (P0) — market_select checkpoint：Ctrl‑C 也能产出已完成结果

**痛点**：probe 跑很久（甚至 1h/24h）后 Ctrl‑C，若没有 flush/checkpoint，就会“看起来没输出”。  
**目标**：无论 Ctrl‑C 何时发生，`out_dir` 内都至少有：
- `market_scores.csv`（header + 已完成 market 的 rows）
- `recommendation.json`（包含 partial 统计 + 已完成 market 列表）
- `suggest.toml`（若不足以选 2 个 market，则写 `insufficient_data=true` + 原因）

**实现清单**
1) `market_scores.csv` 改为 **每个 probe 完成就 append 一行**，并 periodic flush（例如每 20 行或每 2s）。  
2) `recommendation.json` 增加一个 `progress` 区块（每 N 秒覆盖写）：
   - `markets_total`
   - `markets_done`
   - `markets_failed`
   - `elapsed_seconds`
   - `last_ok_gamma_id`
3) Ctrl‑C 时：
   - 停止派发新 probe
   - 等待 in-flight probe 完成或超时（例如 10s）
   - 立刻写一次最终的 `recommendation.json` 与 `suggest.toml`

**验收**
- 命令：`cargo run --bin market_select -- --config config/config.toml --probe-seconds 3600 --pool-limit 50`  
- 运行 30–120 秒 Ctrl‑C：`out_dir` 必须存在上述 3 个文件，且 `market_scores.csv` 至少有若干行。

---

### M1 (P0) — Shadow Ledger Sweep（最小成本的“多参数回测”）

smart_ape 的核心教训：**参数决定生死**。Razor 的 Phase1 已经把每条 signal 的会计中间量写进 `shadow_log.csv`（v_mkt、p_limit、best_bid、q_req…），所以我们可以不做复杂 replay，直接做 ledger 级 sweep。

**目标**：新增二进制 `shadow_sweep`，对一份 `shadow_log.csv` 进行参数 sweep，输出：
- `sweep_scores.csv`（每组参数一行）
- `best_patch.toml`（建议回填的 fill_share / dump_slippage）
- `sweep_recommendation.json`（包含 topN、失败原因、与 baseline 对比）

**不改变**：原始 `shadow_log.csv`，也不改变 Day14 的基准 verdict（工具只是分析）。

#### 关键 sweep 参数（冻结）
1) `fill_share_liquid`（0..1）  
2) `fill_share_thin`（0..1）  
3) `dump_slippage_assumed`（默认 0.05；可 sweep 0.03/0.05/0.10）  
4) `set_ratio_threshold`（默认 0.85；仅用于统计“腿断率”，不改变会计）  

#### Ledger 重算口径（冻结）
对每条 `shadow_log` 记录（每条 signal）：
- 已知：`q_req`、`legs_n`、每腿 `p_limit` / `best_bid` / `v_mkt`  
- 用 sweep 的 `fill_share_*` 重算：
  - `q_fill_i = min(q_req, v_mkt_i * fill_share_used)`  
  - `q_set = min_i(q_fill_i)`  
  - leftovers：`q_left_i = q_fill_i - q_set`  
- 用 sweep 的 `dump_slippage_assumed` 重算 leftover exit：
  - `exit_i = best_bid_i * (1 - dump_slippage_assumed)`  
- 会计公式保持 Frozen Spec：
  - `cost_set = q_set * Σ FEE_POLY.apply_cost(p_limit_i)`  
  - `proceeds_set = q_set * FEE_MERGE.apply_proceeds(1.0)`  
  - `pnl_set = proceeds_set - cost_set`  
  - `pnl_left_i = q_left_i * (FEE_POLY.apply_proceeds(exit_i) - FEE_POLY.apply_cost(p_limit_i))`  
  - `total_pnl = pnl_set + Σ pnl_left_i`  
  - `q_fill_avg = avg(q_fill_i)`  
  - `set_ratio = if q_fill_avg>0 { q_set/q_fill_avg } else { 0 }`

#### `sweep_scores.csv` schema（冻结）
新增文件（不影响现有 schema），header 必须固定为：

```text
run_id,rows_total,rows_ok,rows_bad,fill_share_liquid,fill_share_thin,dump_slippage_assumed,set_ratio_threshold,total_pnl_sum,total_pnl_avg,set_ratio_avg,legging_rate,worst_20_pnl_sum
```

字段解释：
- `legging_rate`：`set_ratio < set_ratio_threshold` 的占比  
- `worst_20_pnl_sum`：按 `total_pnl` 升序取最差 20 条求和（尾部压力）

#### 验收
1) `cargo test`：至少覆盖一条 mock 行的重算（q_fill/q_set/total_pnl）与分位/尾部计算稳定。  
2) `cargo run --bin shadow_sweep -- --input data/run_latest/shadow_log.csv --out-dir data/sweep/<id>`：必须输出 3 个文件。  
3) 输出确定性：同一输入同一参数，输出逐字节一致（json 可允许字段顺序固定）。

---

### M2 (P0) — Day14 Report Stress（不改 verdict，新增“压力视角”）

smart_ape 明确承认：回测最危险的是“执行假设过乐观”。Razor 的 Day14 verdict 仍按 Frozen Spec，但我们要把“结论对假设的敏感性”量化出来。

**目标**：`day14_report` 新增 Stress section（不改变 GO/NO GO 判据），输出：
- baseline（当前 shadow_log）
- stressA：`dump_slippage_assumed = 0.10`（更惨烈的残渣处刑）
- stressB：`fill_share_used *= 0.7`（更保守的排队份额）
- stressC：A+B 同时

**输出要求**
- 终端输出增加 `Stress Summary`（固定格式）  
- `report.json` 中新增 `stress` 字段（注意：这是 report 输出文件，不是冻结的 shadow/trades/ticks schema）

**验收**
- `cargo run --bin day14_report -- --data-dir data/run_latest`：输出必须含 baseline + 3 个 stress 的 `total_pnl_sum` 与 `legging_rate`。  
- 单测：给 3 行 mock shadow_log，baseline/stress 的结果必须可预期且稳定。

---

### M3 (P0) — Run Compare：横向对比多个 run_dir（找 “今天为什么死”）

**目标**：新增 `run_compare` 工具（或在 day14_report 增加 multi-run 模式）：
- 输入：`data/` 下多个 `run_*` 目录或显式 `--runs <dir1,dir2,...>`  
- 输出：`runs_summary.csv` + `runs_summary.md`

**必须输出的对比维度**
- `total_pnl_sum`、`pnl_set_sum`、`pnl_left_sum`  
- `set_ratio_avg`、`legging_rate`  
- `count_by_bucket`、`pnl_by_bucket`  
- `top_reasons`（全局 + 分 bucket）

**验收**
- 目录里有 2 个 run 时，能生成汇总表；并且按 `run_id` 字典序确定性排序。

---

### M4 (P1) — 新增 snapshots.csv（可控采样率，用于真正的离线 replay）

smart_ape 的实现本质是 “录 snapshot 数据集”。Razor 若要离线重放 Brain（尤其是调 `min_net_edge/risk_premium/cooldown`），必须有**可重建的全量快照流**，仅靠 `ticks.csv`（单腿 tick）不够。

**目标**：在不改现有 ticks/trades/shadow schema 的前提下，新增一个可选输出文件：
- `snapshots.csv`：每次 `MarketSnapshot` publish 时记录一条“宽表快照”（最多 3 腿）

**采样率控制（避免 6GB/4days 的数据爆炸）**
- config 增加 `run.snapshot_log_interval_ms`（默认 500ms 或 1000ms）  
- publish 很频繁时，只按间隔落盘（类似采样），并在 `run_meta.json` 写入该采样参数

**snapshots.csv schema（冻结）**

```text
ts_ms,market_id,legs_n,leg0_token_id,leg0_best_bid,leg0_best_ask,leg0_depth3_usdc,leg1_token_id,leg1_best_bid,leg1_best_ask,leg1_depth3_usdc,leg2_token_id,leg2_best_bid,leg2_best_ask,leg2_depth3_usdc
```

**验收**
- dry-run 运行 60 秒：`snapshots.csv` 增长且能反映 2/3 腿市场；binary 的 leg2_* 为空或 0。  
- 单测：写一条 snapshot，csv 行字段数固定，header 正确。

---

### M5 (P1) — Offline Replay：离线重放 snapshots+trades，重算 brain/shadow（复现性）

**目标**：新增 `razor_replay` 工具：
- 输入：`--run-dir data/run_xxx`（读取 snapshots.csv、trades.csv、config.toml）  
- 输出：`replay/` 子目录：
  - `replay_shadow_log.csv`（同 shadow schema）
  - `replay_report.json/md`（同 day14_report 口径）

**核心验收（复现性）**
- 若使用同一份 config、同一份 snapshots/trades：`replay_shadow_log` 的聚合统计（total_pnl_sum / set_ratio_avg / reasons breakdown）必须与原 run 在可接受误差内一致。  
  - 允许误差的唯一来源应是：原系统并发调度导致的窗口边界差异；replay 需要写明它采用的窗口边界策略（闭/开区间）并锁死。

**测试**
- 用 `tests/fixtures/` 放一段极小的 snapshots+trades+config（几十行），对 replay 输出做快照测试（CSV header + totals）。

---

### M6 (P1) — Brain Threshold Sweep（在离线 replay 上调 min_net_edge/risk_premium/cooldown）

smart_ape 的“参数选择决定 ROI”在 Razor 的对应物是：Brain 的门槛与冷却、Shadow 的 fill_share/dump 组合。

**目标**：新增 `brain_sweep` 工具：
- 输入：`--run-dir`（使用 M5 的 replay 能力）  
- sweep 维度（冻结）：
  - `brain.min_net_edge_bps`（例如 10/20/30/40）
  - `brain.risk_premium_bps`（例如 60/80/100）
  - `brain.signal_cooldown_ms`（例如 500/1000/2000）
- 输出：
  - `brain_sweep_scores.csv`
  - `best_brain_patch.toml`

**验收**
- 同一 run_dir 上，输出必须确定性；并且 `best` 选择规则写死（例如：先最大化 total_pnl_sum，其次最小 legging_rate，再其次 signals_count）。

---

### M7 (P2) — Execution Stress Harness（SIM only，量化“回测局限”）

smart_ape 说的局限：延迟、限流、超时、缺档，在真钱阶段都会变成真实亏损。Phase2 进入真钱前，我们必须在 SIM 里“可控复现”这些故障。

**目标**：在 SIM execution（不是真下单）下新增故障注入参数，并把注入状态写入：
- `trade_log.csv notes`（reason codes + 参数）
- `run_meta.json`（stress profile）

**建议的注入开关（冻结命名）**
- `RAZOR_SIM_FORCE_CHASE_FAIL=1`（已存在）
- `RAZOR_SIM_LATENCY_SPIKE_MS=<N>`：每 K 次请求增加 N ms
- `RAZOR_SIM_DROP_BOOK_PCT=<0..1>`：按概率把 best_bid/best_ask 置空（模拟 WS 缺档）
- `RAZOR_SIM_HTTP_429_EVERY=<K>`：每 K 次 poll 模拟一次 429（只用于 market_select/probe 或未来 execution mock）

**验收**
- 开启注入后，FSM 必须稳定覆盖 Flatten/HardStop 分支，并且日志能解释“为什么进入该分支”。

---

### M8 (P2) — Walk‑forward / 分日评估（防止过拟合）

smart_ape 只用几天数据得到极高 ROI，但他自己也承认样本不足。Razor 要避免 Day14 被“偶然行情”误导，必须做 walk-forward。

**目标**：
- 新增 `dataset_split` 工具：按天/小时分割 run_dir 的 shadow_log（或 replay 输出），生成：
  - `daily_scores.csv`（每天 totals + tail）
  - `walk_forward.json`（训练区间选参、验证区间评估）
- 输出固定的 `overfit_risk_score`（机械定义，例如：训练期 vs 验证期 pnl 差异、legging_rate 漂移）

**验收**
- 至少能在 3 天 run 数据上跑通，输出可解释的“是否过拟合”提示。

---

## 4) 与 Frozen Spec 的关系（必须写清楚）

### 不需要 Spec bump 的（建议优先）
- M0/M1/M2/M3：全是工具/报表/输出文件；不改变 Brain/Shadow 公式语义。  
- M4：新增 `snapshots.csv` 文件，不更改既有 CSV schema。  
- M5/M6：离线工具，不改变线上会计口径。  

### 需要 Spec bump 才能做的（暂不列入 P0/P1）
- 改 bucket 判据（20bps/500usdc）  
- 改 Shadow 会计公式（残渣处刑比例、merge proceeds 口径）  
- 改 Day14 verdict 判据（例如加入 tail 风险门槛）  

---

## 5) 每个 PR 的统一验收模板（照抄即可）

每个 PR 必须提供：

1) 变更文件列表  
2) 本地命令：
```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
3) smoke（按该 PR 新增工具给出一条可跑命令）  
4) 验收产物：输出文件路径 + header（CSV）或关键字段（JSON）  

