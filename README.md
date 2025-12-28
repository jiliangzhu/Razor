# Project Razor (Phase 1 / dry-run only)

唯一真相：`docs/architecture.md`（规格冻结）。Phase 1 禁止任何交易写操作。

## Phase 1 dry-run

```bash
RAZOR_MODE=dry_run cargo run -- --config config/config.toml
```

`data/run_latest` 指向最后一次运行结果目录。

## Day 14 report

```bash
cargo run --bin day14_report -- --data-dir data/run_latest
```

## Market selection (Phase 1)

冻结口径见：`docs/market_selection.md`（2 个 market：Liquid 主样本 + Thin 压力样本）。
