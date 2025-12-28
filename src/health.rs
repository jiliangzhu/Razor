use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::recorder::JsonlAppender;
use crate::types::now_ms;

#[derive(Default)]
pub struct HealthCounters {
    ticks_processed: AtomicU64,
    trades_written: AtomicU64,
    trades_dropped: AtomicU64,
    trades_duplicated: AtomicU64,
    trade_poll_hit_limit: AtomicU64,
    signals_emitted: AtomicU64,
    signals_suppressed: AtomicU64,
    signals_dropped: AtomicU64,
    shadow_processed: AtomicU64,
    trade_store_size: AtomicU64,
    trade_store_evicted: AtomicU64,
    last_tick_ingest_ms: AtomicU64,
    last_trade_ingest_ms: AtomicU64,
    last_shadow_write_ms: AtomicU64,
}

impl HealthCounters {
    pub fn inc_ticks_processed(&self, n: u64) {
        self.ticks_processed.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_trades_written(&self, n: u64) {
        self.trades_written.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_trades_dropped(&self, n: u64) {
        self.trades_dropped.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_trades_duplicated(&self, n: u64) {
        self.trades_duplicated.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_trade_poll_hit_limit(&self, n: u64) {
        self.trade_poll_hit_limit.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_signals_emitted(&self, n: u64) {
        self.signals_emitted.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_signals_suppressed(&self, n: u64) {
        self.signals_suppressed.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_signals_dropped(&self, n: u64) {
        self.signals_dropped.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_shadow_processed(&self, n: u64) {
        self.shadow_processed.fetch_add(n, Ordering::Relaxed);
    }

    pub fn set_trade_store_size(&self, size: usize) {
        self.trade_store_size.store(size as u64, Ordering::Relaxed);
    }

    pub fn inc_trade_store_evicted(&self, n: u64) {
        self.trade_store_evicted.fetch_add(n, Ordering::Relaxed);
    }

    pub fn set_last_tick_ingest_ms(&self, ts_ms: u64) {
        self.last_tick_ingest_ms.store(ts_ms, Ordering::Relaxed);
    }

    pub fn set_last_trade_ingest_ms(&self, ts_ms: u64) {
        self.last_trade_ingest_ms.store(ts_ms, Ordering::Relaxed);
    }

    pub fn set_last_shadow_write_ms(&self, ts_ms: u64) {
        self.last_shadow_write_ms.store(ts_ms, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> HealthSnapshot {
        HealthSnapshot {
            ts_ms: now_ms(),
            ticks_processed: self.ticks_processed.load(Ordering::Relaxed),
            trades_written: self.trades_written.load(Ordering::Relaxed),
            trades_dropped: self.trades_dropped.load(Ordering::Relaxed),
            trades_duplicated: self.trades_duplicated.load(Ordering::Relaxed),
            trade_poll_hit_limit: self.trade_poll_hit_limit.load(Ordering::Relaxed),
            signals_emitted: self.signals_emitted.load(Ordering::Relaxed),
            signals_suppressed: self.signals_suppressed.load(Ordering::Relaxed),
            signals_dropped: self.signals_dropped.load(Ordering::Relaxed),
            shadow_processed: self.shadow_processed.load(Ordering::Relaxed),
            trade_store_size: self.trade_store_size.load(Ordering::Relaxed),
            trade_store_evicted: self.trade_store_evicted.load(Ordering::Relaxed),
            last_tick_ingest_ms: self.last_tick_ingest_ms.load(Ordering::Relaxed),
            last_trade_ingest_ms: self.last_trade_ingest_ms.load(Ordering::Relaxed),
            last_shadow_write_ms: self.last_shadow_write_ms.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HealthLine {
    Heartbeat(HealthSnapshot),
    TradePollHitLimit {
        ts_ms: u64,
        returned_count: usize,
        earliest_ts_ms: u64,
        latest_ts_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthSnapshot {
    pub ts_ms: u64,
    pub ticks_processed: u64,
    pub trades_written: u64,
    pub trades_dropped: u64,
    pub trades_duplicated: u64,
    pub trade_poll_hit_limit: u64,
    pub signals_emitted: u64,
    pub signals_suppressed: u64,
    pub signals_dropped: u64,
    pub shadow_processed: u64,
    pub trade_store_size: u64,
    pub trade_store_evicted: u64,
    pub last_tick_ingest_ms: u64,
    pub last_trade_ingest_ms: u64,
    pub last_shadow_write_ms: u64,
}

pub fn spawn_health_writer(
    path: PathBuf,
    counters: Arc<HealthCounters>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<(mpsc::Sender<HealthLine>, JoinHandle<()>)> {
    let (tx, mut rx) = mpsc::channel::<HealthLine>(10_000);

    let handle = tokio::spawn(async move {
        let mut out = match JsonlAppender::open(&path) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, path = %path.display(), "open health.jsonl failed");
                return;
            }
        };

        let mut tick = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
                _ = tick.tick() => {
                    let snap = counters.snapshot();
                    let line = HealthLine::Heartbeat(snap);
                    if let Err(e) = write_line(&mut out, &line) {
                        warn!(error = %e, "health heartbeat write failed");
                    }
                }
                maybe = rx.recv() => {
                    let Some(line) = maybe else { break; };
                    if let Err(e) = write_line(&mut out, &line) {
                        warn!(error = %e, "health event write failed");
                    }
                }
            }
        }

        if let Err(e) = out.flush_and_sync() {
            warn!(error = %e, "health.jsonl flush/sync failed");
        }
    });

    Ok((tx, handle))
}

fn write_line(out: &mut JsonlAppender, line: &HealthLine) -> anyhow::Result<()> {
    let json = serde_json::to_string(line)?;
    out.write_line(&json)?;
    Ok(())
}
