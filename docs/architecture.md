# Project Razor — 架构说明（冻结版 / 仅限执行）

> 核心目标：90 天内证明 **净利润 > 0**  
> 生存原则：死于波动是天灾，死于会计/单位错误是人祸  
> 状态：**Frozen Spec**。任何变更必须显式升版本号，否则视为破坏审计。

---

## 0. 范围与阶段（Phase）

### Phase 1（Day 1–14）：验尸官 / 纯影子
- **禁止交易、禁止下单、禁止签名**（不实现 Sniper）。
- 连接 Polymarket 数据流（WS + 必要的只读 REST 元数据）。
- 生成可审计 CSV 日志：
  - `trades.csv`
  - `shadow_log.csv`
  - `ticks.csv`

**Day 14 判决只看数据：**
- `TotalShadowPnL > 0` 且 `SetRatio >= 0.85` => **GO**
- 否则 => **NO GO**（项目结束）

### Phase 2（Day 15–30）：校准者 / 小额实盘
- 启用 Sniper OMS，严格限制（总资金 $100，微仓位）。
- 按桶（Liquid/Thin）校准 `fill_share_*_p25`，每周滚动更新。

Phase 2（live）新增落盘：
- `trade_log.csv`：OMS 执行轨迹（每次下单 intent + ack + fills + 结果）
- `calibration_log.csv`：按桶累计真实 `real_fill_share` 样本与建议 p25
- `calibration_suggest.toml`：可回填建议（不自动改 config）

### Phase 3（Day 31–90）：规模化
- AWS 部署属于运维动作，不是架构重构。
- 只在 Phase 2 实盘 PnL 为正后再扩容。

---

## 1. 系统总览（钢线交付）

单体 Rust 二进制。所有状态在内存。持久化全部走追加式 CSV 日志。

```mermaid
graph TD
  WS["Polymarket WS"] --> Feed["Task: Feed & Recorder"]
  Feed -->|Trades (mpsc)| Shadow["Task: Accounting Shadow"]
  Feed -->|Book Snapshot (watch)| Brain["Task: Net-Edge Brain"]
  Brain -->|Signals (mpsc)| Shadow

  Shadow --> CSV_Shadow["shadow_log.csv"]
  Feed --> CSV_Trades["trades.csv"]
  Feed --> CSV_Ticks["ticks.csv"]
```

### 并发模型（Tokio）
- Feed 输出：
  - Trades：`mpsc`（有缓冲，尽量不丢）
  - 最新盘口快照：`watch`（只要最新，旧的丢了没关系）
- Brain 消费快照并产生 `Signal`
- Shadow 同时消费 `Signal` 和 `TradeTick`，维护短期缓冲并按窗口会计结算

---

## 2. 核心类型与单位体系（不可妥协）

### 2.1 Bps（强类型基点）
- 所有费率/优势/预算必须在 `Bps` 域中计算
- 浮点只能用于**价格**与**数量**，禁止用于费率算术

必须实现：
- `Bps(i32)` 与常量：
  - `ZERO`
  - `ONE_HUNDRED_PERCENT = 10000`
  - `FEE_POLY = 200`（2%）
  - `FEE_MERGE = 10`（0.1%）
- `apply_cost(price)` 与 `apply_proceeds(price)`（避免买卖方向误用）
- `Add/Sub` 等运算与 clamp 辅助

### 2.2 最小市场快照（热路径）
只保留：
- `market_id`
- `legs: Vec<LegSnapshot>`，每腿：
  - `token_id`
  - `best_ask`, `best_bid`（f64）
  - `ask_depth3_usdc`（前三档卖盘名义 USDC：Σ px*sz）
  - `ts_recv_us`（本地接收时间）

### 2.3 Signal（Brain -> Shadow）
Shadow 会计输入：
- `signal_id`
- `ts_signal_us`
- `market_id`
- `strategy`（binary / triangle）
- `bucket`（Liquid / Thin）
- `q_req`
- `expected_net_bps: Bps`
- `legs: Vec<SignalLeg>`：
  - `token_id`
  - `p_limit`（f64）
  - `best_bid_at_t0`（f64，可在结算时刷新）

### 2.4 TradeTick（Feed -> Shadow）
**不信 side，不用 side**。只看：
- `ts_recv_us`
- `market_id`, `token_id`
- `price`, `size`

---

## 3. 流动性分桶（最薄腿原则）

Brain + Shadow 共享桶逻辑。对每条腿 i：
1) `mid = (best_ask + best_bid) / 2`
2) `spread_bps_i = ((best_ask - best_bid) / mid) * 10000`
3) `depth3_usdc_i = Σ_{lvl=1..3}(ask_px * ask_sz)`（仅 ask 侧）

最薄腿：选择 `depth3_usdc_i` 最小的那条腿。

桶规则：
- **Liquid**：`spread_bps_worst < 20` 且 `depth3_usdc_worst > 500`
- 否则 **Thin**

校准参数（config.toml）：
- `fill_share_liquid_p25`（默认 0.30）
- `fill_share_thin_p25`（默认 0.10）

---

## 4. Brain（净优势大脑）

### 目标
在源头掐死“看起来赚钱其实负期望”的机会。

### Bps 域净优势
令 `sum_prices = Σ best_ask_i`：
- `raw_cost_bps = floor(sum_prices * 10000)`
- `raw_edge_bps = 10000 - raw_cost_bps`

成本（Bps）：
- 硬成本：`FEE_POLY + FEE_MERGE`
- 生存预算（默认 80 bps，策略可分档）

`net_edge = raw_edge - hard_fees - risk_premium`

门槛：
- 只有 `net_edge > 10 bps` 才发 Signal

Phase 1 策略范围：
- S1 Binary（2 腿）
- S2 Triangle（3 腿）

---

## 5. Shadow（成套会计影子）

### 目的
计算包含“排队份额 + 残渣处刑”的真实期望值。

### 时间窗口
对每个信号 `T0=ts_signal_us`：
- 结算 trades 窗口：`[T0+100ms, T0+1100ms]`

### A：每腿市场可成交量（限价内）
每条腿 i 的限价 `P_i`：
- `V_mkt_i = Σ trade.size`
- 条件：窗口内、token_id 匹配、且 `trade.price <= P_i`

### B：应用保守成交份额（按桶 p25）
- `S = fill_share_p25(bucket)`
- `V_my_i = V_mkt_i * S`
- `Q_fill_i = min(Q_req, V_my_i)`

### C：成套与残渣
- `Q_set = min_i(Q_fill_i)`
- `Q_left_i = Q_fill_i - Q_set`

### D：会计损益（Matched Set + Leftover Dump）

成套（安全）：
- `Cost_set = Q_set * Σ P_i`
- `Proceeds_set = Q_set * 1.0`
- `PnL_set = Proceeds_set - Cost_set - fee_wear`（保守扣除常量费用）

残渣（处刑）：
- `Exit_i = best_bid_i * 0.95`
- `PnL_left_i = Q_left_i * (Exit_i - P_i) - fee_left`

总计：
- `PnL_total = PnL_set + Σ PnL_left_i`

**Day14 指标：**
- `TotalShadowPnL = Σ PnL_total`
- `SetRatio = Q_set / avg(Q_fill_i)`（或用 `Q_set / Q_req` 的保守版本）
- Tail：`pnl_total` 的最差 1% 分位

---

## 6. 日志与 Schema（冻结）

日志追加写入，不改列名、不改顺序。

### trades.csv（最小）
- `ts_recv_us, market_id, token_id, price, size`

### shadow_log.csv（建议固定列，Phase 1 最多 3 腿）
基础：
- `ts_signal_us, signal_id, market_id, strategy, bucket, q_req`
每腿：
- `token1, p1, v_mkt1, q_fill1, best_bid1, exit1`
- `token2, p2, v_mkt2, q_fill2, best_bid2, exit2`
- `token3, p3, v_mkt3, q_fill3, best_bid3, exit3`
成套/残渣：
- `q_set, q_left1, q_left2, q_left3, set_ratio`
PnL：
- `pnl_set, pnl_left, pnl_total`
参数：
- `fill_share_used, risk_premium_bps, expected_net_bps`

---

## 7. Day14 报告（必须交付）
脚本读取 `shadow_log.csv` 并输出：
- TotalShadowPnL
- SetRatio（整体/分桶/分策略）
- 机会数与贡献
- worst1% tail
- GO/NO GO

---

## 8. Phase 1 运行手册（Runbook）

环境变量：
- `RAZOR_MODE=dry_run`

命令：
- `cargo run --release`

检查：
- CSV 持续增长
- panic / deadlock / backpressure
- 内存是否持续上涨（泄漏）

Phase 1 禁止项：
- Sniper OMS
- 数据库
- 延迟竞赛
- AWS 部署
