# Project Razor (Phase 1 / dry-run only)

唯一真相：`docs/architecture.md`（规格冻结）。Phase 1 禁止任何交易写操作。

## Quickstart

```bash
cp config.example.toml config.toml
RAZOR_MODE=dry_run cargo run --release -- --config config.toml
```

输出日志（追加写入）：
- `data/trades.csv`
- `data/ticks.csv`
- `data/shadow_log.csv`
- `data/raw_ws.jsonl`（可选：原始 WS 录制，用于复核/回放）

## Smoke check

1) 运行 60s：`RAZOR_MODE=dry_run cargo run --release -- --config config.toml`
2) 检查文件增长：`ls -lh data/*.csv data/raw_ws.jsonl`

## Day 14 report

```bash
python3 scripts/day14_report.py data/shadow_log.csv
```

