# AGENTS.md — Project Razor v1.3.2a 执行规则（冻结）

本仓库遵循 **Frozen Spec v1.3.2a**。唯一使命：
> 在 Day 14 用“成套会计 + 残渣处刑”的口径，给这个 idea 判死刑或判活。

---

## 0) 角色分工

### 朱继良（人类）
- 规格冻结 + 最终审计 + Day14 判决
- 任何 scope / schema 变更必须明确批准并升版本

### Codex（工程兵）
- 严格按 Frozen Spec 实现
- 小步提交，每步可验收
- **Phase 1 禁止实现任何交易写操作**

---

## 1) 硬约束（不可谈判）

1. Phase 1 必须纯 dry-run：
   - 禁止下单、禁止签名、禁止 OMS、禁止私钥加载、禁止任何写 API

2. 单位安全：
   - 费率/优势/预算全部用 `Bps`
   - 禁止用 float 做费率算术（float 仅用于价格/数量）

3. Shadow 会计口径冻结：
   - `Q_set = min(Q_fill_i)` 才能视为可 Merge
   - 残渣按 `best_bid * 0.95` 处刑
   - PnL 采用：`Matched Set Profit - Leftover Dump Loss`
   - 手续费使用：`FEE_POLY.apply_cost/apply_proceeds` + `FEE_MERGE.apply_proceeds(1.0)`

4. 分桶冻结：
   - 最薄腿原则（Worst Leg）
   - `spread_bps_worst < 20` 且 `depth3_usdc_worst > 500` => Liquid，否则 Thin
   - `fill_share_*_p25` 按桶使用，且采用 p25（保守）

5. CSV schema 冻结：
   - 不允许私自改列名/列顺序
   - 任何 schema 变更必须升版本并提供迁移说明

---

## 2) 开发工作流（按顺序交付）

PR-000：骨架 + 文档落盘（architecture.md + AGENTS.md + config stub）  
PR-001：`types.rs`（Bps + Snapshot/Signal/TradeTick）  
PR-002：Recorder（CSV writer 封装 + schema 冻结）  
PR-003：Feed（WS/Trades 解析 + 写 trades.csv）  
PR-004：Ticks/Book Snapshot（用于分桶/复核）  
PR-005：Brain（Net Edge 过滤 + Signal 输出）  
PR-006：Shadow（窗口统计 + 成套会计 + 写 shadow_log.csv）  
PR-007：Day14 report 脚本（GO/NO GO）  
PR-008：挂机稳态修补（backpressure、心跳、可观测性）

每个 PR 必须附：
- 本地运行命令
- smoke check
- 验收标准

---

## 3) 编码标准

- 明确优于抽象，不要框架化
- 热路径避免频繁分配（复用缓冲）
- 错误可观测：日志清晰，拒绝 silent retry
- 时间戳：落盘用本地接收时间；Shadow 窗口以 `ts_signal_us` 为基准

---

## 4) Phase 1 验收口径

Phase 1 完成标准：
- `cargo run --release` 稳定运行数小时
- `data/trades.csv` 持续增长，字段正确
- `data/shadow_log.csv` 持续增长，且每行包含：
  - 每腿 `V_mkt/Q_fill/Q_left`
  - `Q_set`、`set_ratio`
  - `pnl_set/pnl_left/pnl_total`
- `scripts/day14_report.py` 输出：
  - TotalShadowPnL_sum、SetRatio、GO/NO GO

---

## 5) 禁止项（Forbidden）

- Phase 1 实现 Sniper/OMS/下单/签名/私钥加载
- 引入数据库（SQLite/Postgres/Redis）
- 使用 float 做费率计算
- 未升版本就修改 CSV schema
