use std::path::PathBuf;

use anyhow::Context as _;
use tokio::sync::watch;
use tracing::warn;

use crate::recorder::CsvAppender;
use crate::schema::SNAPSHOTS_HEADER;
use crate::types::{now_ms, MarketSnapshot};

pub async fn run_snapshot_logger(
    out_path: PathBuf,
    mut snap_rx: watch::Receiver<Option<MarketSnapshot>>,
    snapshot_log_interval_ms: u64,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut out = CsvAppender::open(&out_path, &SNAPSHOTS_HEADER).context("open snapshots.csv")?;

    let mut last_logged_ms: u64 = 0;

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            changed = snap_rx.changed() => {
                if changed.is_err() {
                    break;
                }
            }
        }

        if *shutdown.borrow() {
            break;
        }

        let Some(snap) = snap_rx.borrow().clone() else {
            continue;
        };

        let ts_ms = snap
            .legs
            .iter()
            .map(|l| l.ts_recv_us / 1000)
            .max()
            .unwrap_or_else(now_ms);

        if ts_ms.saturating_sub(last_logged_ms) < snapshot_log_interval_ms {
            continue;
        }
        last_logged_ms = ts_ms;

        let legs_n = snap.legs.len();
        if !(2..=3).contains(&legs_n) {
            warn!(market_id = %snap.market_id, legs_n, "skip snapshot with unsupported legs_n");
            continue;
        }

        let mut cols: [String; 15] = Default::default();
        cols[0] = ts_ms.to_string();
        cols[1] = snap.market_id.clone();
        cols[2] = legs_n.to_string();

        for (i, leg) in snap.legs.iter().take(3).enumerate() {
            let base = 3 + i * 4;
            cols[base] = leg.token_id.clone();
            cols[base + 1] = fmt_f64(leg.best_bid);
            cols[base + 2] = fmt_f64(leg.best_ask);
            cols[base + 3] = fmt_f64(leg.ask_depth3_usdc);
        }

        out.write_record(cols)
            .with_context(|| format!("write snapshot row {}", out_path.display()))?;
    }

    out.flush_and_sync().context("flush snapshots.csv")?;
    Ok(())
}

fn fmt_f64(v: f64) -> String {
    if !v.is_finite() {
        return "NaN".to_string();
    }
    format!("{v:.6}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LegSnapshot, MarketSnapshot};

    #[test]
    fn snapshots_header_is_frozen() {
        assert_eq!(SNAPSHOTS_HEADER.join(","), "ts_ms,market_id,legs_n,leg0_token_id,leg0_best_bid,leg0_best_ask,leg0_depth3_usdc,leg1_token_id,leg1_best_bid,leg1_best_ask,leg1_depth3_usdc,leg2_token_id,leg2_best_bid,leg2_best_ask,leg2_depth3_usdc");
    }

    #[test]
    fn snapshot_row_has_fixed_columns() {
        let snap = MarketSnapshot {
            market_id: "m1".to_string(),
            legs: vec![
                LegSnapshot {
                    token_id: "t0".to_string(),
                    best_ask: 0.49,
                    best_bid: 0.48,
                    best_ask_size_best: 1.0,
                    best_bid_size_best: 1.0,
                    ask_depth3_usdc: 100.0,
                    ts_recv_us: 1_700_000_000_000_000,
                },
                LegSnapshot {
                    token_id: "t1".to_string(),
                    best_ask: 0.51,
                    best_bid: 0.50,
                    best_ask_size_best: 1.0,
                    best_bid_size_best: 1.0,
                    ask_depth3_usdc: 200.0,
                    ts_recv_us: 1_700_000_000_000_100,
                },
            ],
        };

        let ts_ms = snap.legs.iter().map(|l| l.ts_recv_us / 1000).max().unwrap();
        let mut cols: [String; 15] = Default::default();
        cols[0] = ts_ms.to_string();
        cols[1] = snap.market_id.clone();
        cols[2] = snap.legs.len().to_string();
        for (i, leg) in snap.legs.iter().take(3).enumerate() {
            let base = 3 + i * 4;
            cols[base] = leg.token_id.clone();
            cols[base + 1] = fmt_f64(leg.best_bid);
            cols[base + 2] = fmt_f64(leg.best_ask);
            cols[base + 3] = fmt_f64(leg.ask_depth3_usdc);
        }
        assert_eq!(cols.len(), 15);
    }
}
