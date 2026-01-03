# Project Razor (Phase 1 dry-run / Phase 2 live-sim)

唯一真相：`docs/architecture.md`（规格冻结）。Phase 1 禁止任何交易写操作。

## Phase 1 dry-run

```bash
RAZOR_MODE=dry_run cargo run -- --config config/config.toml
```

`data/run_latest` 指向最后一次运行结果目录。

## Phase 2 live-sim（不发真实订单）

> 保护闸门：`config.toml` 的 `[live].enabled` 必须为 `false`，否则程序会拒绝启动（避免误下单）。

```bash
RAZOR_MODE=live cargo run -- --config config/config.toml
```

产物：`data/run_latest/trade_log.csv`、`data/run_latest/calibration_log.csv`、`data/run_latest/calibration_suggest.toml`。

故障注入（稳定覆盖 Flatten/HardStop 分支）：

```bash
RAZOR_MODE=live RAZOR_SIM_FORCE_CHASE_FAIL=1 cargo run -- --config config/config.toml
```

## Day 14 report

```bash
cargo run --bin day14_report -- --data-dir data/run_latest
```

## Market selection (Phase 1)

冻结口径见：`docs/market_selection.md`（2 个 market：Liquid 主样本 + Thin 压力样本）。

运行只读选市场工具：

```bash
cargo run --bin market_select -- --config config/config.toml --probe-seconds 3600 --pool-limit 200 --prefer-strategy any
```

输出目录：`data/market_select/<run_id>/`（包含 `market_scores.csv`、`recommendation.json`、`suggest.toml`）。

## Offline / Audit Tools

> 这些工具只读输入数据（或只写建议文件），不改变 Phase 1 冻结口径，不会自动修改 `config.toml`。

Shadow ledger 参数 sweep（fill_share / dump_slippage）：

```bash
cargo run --bin shadow_sweep -- --input data/run_latest/shadow_log.csv
```

多 run 对比（找“今天为什么死”）：

```bash
cargo run --bin run_compare -- --data-dir data
```

离线 replay（用 `snapshots.csv + trades.csv` 重算 brain/shadow，验证可复现）：

```bash
cargo run --bin razor_replay -- --run-dir data/run_latest
```

Brain 阈值 sweep（离线）：

```bash
cargo run --bin brain_sweep -- --run-dir data/run_latest
```

分日评估 / walk-forward（离线，防过拟合）：

```bash
cargo run --bin dataset_split -- --run-dir data/run_latest
```
