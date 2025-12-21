#!/usr/bin/env python3

import csv
import math
import sys
from collections import defaultdict


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    if p <= 0:
        return sorted_vals[0]
    if p >= 1:
        return sorted_vals[-1]
    idx = int(math.floor(p * (len(sorted_vals) - 1)))
    return sorted_vals[idx]


def main():
    if len(sys.argv) != 2:
        print("usage: day14_report.py data/shadow_log.csv", file=sys.stderr)
        return 2

    path = sys.argv[1]

    rows = []
    with open(path, "r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)

    if not rows:
        print("rows=0")
        print("TotalShadowPnL_sum=0")
        print("SetRatio=0")
        print("GO_NO_GO=NO_GO")
        return 0

    total_pnl = 0.0
    total_q_set = 0.0
    total_q_req = 0.0
    pnl_list = []

    by_bucket = defaultdict(lambda: {"pnl": 0.0, "q_set": 0.0, "q_req": 0.0, "n": 0})
    by_strategy = defaultdict(lambda: {"pnl": 0.0, "q_set": 0.0, "q_req": 0.0, "n": 0})

    for row in rows:
        pnl = float(row["pnl_total"])
        q_set = float(row["q_set"])
        q_req = float(row["q_req"])
        bucket = row["bucket"]
        strategy = row["strategy"]

        total_pnl += pnl
        total_q_set += q_set
        total_q_req += q_req
        pnl_list.append(pnl)

        by_bucket[bucket]["pnl"] += pnl
        by_bucket[bucket]["q_set"] += q_set
        by_bucket[bucket]["q_req"] += q_req
        by_bucket[bucket]["n"] += 1

        by_strategy[strategy]["pnl"] += pnl
        by_strategy[strategy]["q_set"] += q_set
        by_strategy[strategy]["q_req"] += q_req
        by_strategy[strategy]["n"] += 1

    set_ratio = (total_q_set / total_q_req) if total_q_req > 0 else 0.0
    pnl_list.sort()
    worst_1pct = percentile(pnl_list, 0.01)

    decision = "GO" if (total_pnl > 0.0 and set_ratio >= 0.85) else "NO_GO"

    print(f"rows={len(rows)}")
    print(f"TotalShadowPnL_sum={total_pnl:.6f}")
    print(f"SetRatio={set_ratio:.4f}")
    print(f"worst1pct_pnl_total={worst_1pct:.6f}")

    for k, v in sorted(by_bucket.items(), key=lambda kv: kv[0]):
        sr = (v["q_set"] / v["q_req"]) if v["q_req"] > 0 else 0.0
        print(f"bucket[{k}].n={v['n']}")
        print(f"bucket[{k}].TotalShadowPnL_sum={v['pnl']:.6f}")
        print(f"bucket[{k}].SetRatio={sr:.4f}")

    for k, v in sorted(by_strategy.items(), key=lambda kv: kv[0]):
        sr = (v["q_set"] / v["q_req"]) if v["q_req"] > 0 else 0.0
        print(f"strategy[{k}].n={v['n']}")
        print(f"strategy[{k}].TotalShadowPnL_sum={v['pnl']:.6f}")
        print(f"strategy[{k}].SetRatio={sr:.4f}")

    print(f"GO_NO_GO={decision}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

