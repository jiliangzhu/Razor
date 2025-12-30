# Phase 1 Follow-up Backlog (P0/P1/P2)

目的：把目前代码里仍存在的疏漏/风险点**锁死成清单**，便于后续按优先级逐个修复，保证 Phase 1（dry-run / shadow / Day14 report）的结论“可复现、可解释、可审计”。

范围：仅 Phase 1（禁止真实下单/OMS；不改 Shadow 会计公式；不改冻结 CSV schema）。

更新时间：2025-12-30

---

## P0（必须优先修：会直接导致 signals=0 / bucket 失真 / Day14 结论不可信）

1) **WS `book` 解析只支持 string，若字段为 number 会把盘口退化成 bid=0 / ask=1**
   - 影响：bucket 判定与 raw_cost 会被污染，Brain 可能长期无信号（表现为 raw_ws 有数据但 ticks/snapshot/signal 不增长或 signal=0）。
   - 位置：
     - `src/feed.rs:530`（`best_level()` 只 `as_str()`）
     - `src/feed.rs:568`（`ask_depth3_usdc()` 只 `as_str()`）
     - `src/market_select/probe.rs:528`
     - `src/market_select/probe.rs:566`
   - 建议修法：
     - 复用 `parse_f64()`（string/number 双兼容）解析 price/size。
     - 补单测：price/size 为 number 时也能正确得到 best_bid/best_ask/depth3。

2) **WS `book` 使用消息里的 `market` 字段作为 market_id，可能与 token→market 映射不一致**
   - 影响：ticks 可能写了，但 `market_states.get_mut(market_id)` 失败导致 snapshot 不更新；Brain 收不到正确 snapshot。
   - 位置：
     - `src/feed.rs:349`（`obj["market"]`）
     - `src/feed.rs:394`（`market_states.get_mut(market_id)`）
   - 建议修法：
     - 以 `token_to_market[token_id].0` 的 market_id 为权威。
     - 若 obj.market 与映射不一致：`warn!` 记录并继续使用映射值（避免静默丢数据）。

3) **market_select 的 trades 时间戳域与 Phase1 冻结口径不一致（probe 里用 exchange_ts 优先）**
   - 影响：market_select 输出的 trade_gap/coverage 统计与 Phase1 实际 shadow window 同域不一致，选出来的 market 可能“看起来很好”，但 Phase1 跑出来是另一回事。
   - 位置：`src/market_select/probe.rs:363`（`ts_ms = exchange_ts_ms else ingest_ts_ms`）
   - 建议修法：
     - market_select 内部统计域固定使用 ingest_ts_ms（与 Phase1 shadow window 同域）。
     - exchange_ts_ms 仍可用于去重 key 或作为“诊断字段”写入 recommendation.json。

4) **day14_report 当前按“notes 组合键”分组，而不是按“单个 reason code”拆分统计**
   - 影响：`NO_TRADES,MISSING_BID` 会作为一个整体分组，调参/归因效率低（看不出 NO_TRADES 总体占比/贡献）。
   - 位置：`src/bin/day14_report.rs`（`canonical_notes_key()` + `by_notes`）
   - 建议修法：
     - 新增一个 section：按 reason 展开统计（把每行 notes 分裂成多个 reason，累加 count/sum_pnl/avg_pnl，并按 bucket 再拆）。

---

## P1（建议尽快修：稳定性/可观测性问题，影响 7–14 天游泳质量）

1) **多个 `tokio::time::interval` 未设置 `MissedTickBehavior::Delay`，卡顿后可能 burst “追账式”触发**
   - 风险：短时间内集中发送 HTTP/WS/写盘/CPU 峰值，可能引发连锁抖动（poll hit limit、队列堆积、错过窗口）。
   - 位置（部分）：
     - `src/feed.rs:226`（WS ping）
     - `src/feed.rs:656`（trades poll interval）
     - `src/shadow.rs:38`（shadow settle tick）
     - `src/health.rs:171`（health heartbeat）
     - `src/main.rs:164`（health log）
     - `src/market_select/probe.rs:85` / `src/market_select/probe.rs:89` / `src/market_select/probe.rs:121`
   - 建议修法：
     - 统一 `interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay)`。

2) **Brain 的去重表 `last_by_key` 可能无界增长（长期运行累积很多 (market,strategy,cost_bucket) key）**
   - 影响：长期挂 7–14 天后内存不可控，或 HashMap 操作变慢。
   - 位置：`src/brain.rs:55`
   - 建议修法：
     - 引入 TTL prune（例如每分钟清理 “超过 1h 未命中”的 key）。
     - 或对每 market 设置最大 key 数（LRU/环形）。

3) **TradeStore `trim()` 隐含“push 基本按时间单调递增”的假设**
   - 风险：如果出现 out-of-order trade tick（调度抖动/网络），过老 trade 可能滞留在队列中，影响窗口统计/内存占用。
   - 位置：`src/trade_store.rs:197`
   - 建议修法：
     - 若检测到 `new_ts < last_ts`：记录计数/告警。
     - 周期性做一次 O(n) retain 清理兜底（Phase1 正确性优先）。

4) **bucket 的 depth3 degraded 判据未覆盖“单位误读导致 depth3 巨大”的情况**
   - 影响：depth3 若单位被误读可能出现极大值，worst-leg 选择/分桶会被扭曲；Day14 归因会自欺。
   - 位置：`src/buckets.rs:41`
   - 建议修法：
     - 与 market_selection 冻结一致：`ask_depth3_usdc > 10_000_000` 直接标记 degraded，并强制 Thin + reason。

---

## P2（体验/可维护性/防自欺增强：不一定致命，但提升工程质量）

1) **market_select 目前是“全部 probe 完成后才落盘”，若进程异常退出会丢已完成 probe 结果**
   - 现状：Ctrl-C 已支持 partial 输出，但 kill -9 / crash 仍会丢。
   - 位置：`src/market_select/mod.rs:151`（最终统一写 `market_scores.csv`）
   - 建议修法：
     - 每个 probe 完成就 append 一行 `market_scores.csv` 并 periodic flush（类似 recorder 的批量 flush）。

2) **缺少 config 自检（防止手滑导致长期静默退化）**
   - 建议新增启动时 validate（不改策略口径，仅防错）：
     - `shadow.window_end_ms > window_start_ms`
     - `shadow.trade_retention_ms >= shadow.window_end_ms`
     - `buckets.fill_share_*` ∈ [0,1] 且 finite
     - `trade_poll_interval_ms > 0`
   - 建议位置：`src/main.rs`（parse config 后立即 validate，失败直接退出）

3) **trades poller 固定 `takerOnly=true` 可能系统性低估成交量**
   - 风险：会放大 `NO_TRADES`，并可能误导 Day14 的“市场死/策略死”判断。
   - 位置：`src/feed.rs:673`
   - 建议修法：
     - 做成 config 可控，并把该口径写入 run_meta/report（便于复现与解释）。

