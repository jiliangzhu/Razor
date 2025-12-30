use std::collections::{HashSet, VecDeque};

use crate::types::{now_ms, TradeTick};
use tracing::warn;

/// In-memory ring buffer for Shadow volume queries (Phase 1).
///
/// Correctness first: O(n) scans are acceptable at Phase 1 scale.
pub struct TradeStore {
    retention_ms: u64,
    max_trades: usize,
    trades: VecDeque<TradeTick>,
    recent_ids: HashSet<String>,
    dedup_events: VecDeque<DedupEvent>,
    last_seen_ts_ms: u64,
    needs_full_trim: bool,
    last_out_of_order_warn_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WindowStats {
    pub trades_in_window: usize,
    #[allow(dead_code)]
    pub max_gap_ms: u64,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct DedupEvent {
    market_id: String,
    ts_ms: u64,
}

impl TradeStore {
    pub fn new_with_cap(retention_ms: u64, max_trades: usize) -> Self {
        Self {
            retention_ms,
            max_trades,
            trades: VecDeque::new(),
            recent_ids: HashSet::new(),
            dedup_events: VecDeque::new(),
            last_seen_ts_ms: 0,
            needs_full_trim: false,
            last_out_of_order_warn_ms: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.trades.len()
    }

    pub fn is_empty(&self) -> bool {
        self.trades.is_empty()
    }

    pub fn push(&mut self, t: TradeTick) -> PushResult {
        let mut t = t;
        if t.token_id.is_empty() || t.market_id.is_empty() {
            return PushResult::dropped();
        }
        if !t.price.is_finite() || !t.size.is_finite() || t.price < 0.0 || t.size <= 0.0 {
            return PushResult::dropped();
        }
        if effective_ingest_ts_ms(&t) == 0 {
            return PushResult::dropped();
        }
        if t.trade_id.trim().is_empty() {
            t.trade_id = fallback_trade_id(&t);
        }

        let now = now_ms();
        self.trim(now);

        let ts = effective_ingest_ts_ms(&t);
        if self.last_seen_ts_ms > 0 && ts < self.last_seen_ts_ms {
            self.needs_full_trim = true;
            if now.saturating_sub(self.last_out_of_order_warn_ms) >= 10_000 {
                self.last_out_of_order_warn_ms = now;
                warn!(
                    ts_ms = ts,
                    last_seen_ts_ms = self.last_seen_ts_ms,
                    "trade tick out-of-order; enabling full-trim fallback"
                );
            }
        }
        self.last_seen_ts_ms = self.last_seen_ts_ms.max(ts);

        if self.recent_ids.contains(&t.trade_id) {
            self.dedup_events.push_back(DedupEvent {
                market_id: t.market_id.clone(),
                ts_ms: effective_ingest_ts_ms(&t),
            });
            return PushResult::duplicated();
        }

        self.recent_ids.insert(t.trade_id.clone());
        self.trades.push_back(t);

        let evicted = self.enforce_cap();
        PushResult {
            inserted: true,
            duplicated: false,
            evicted,
        }
    }

    #[allow(dead_code)]
    pub fn dedup_hits_in_window(&self, market_id: &str, start_ms: u64, end_ms: u64) -> usize {
        if market_id.trim().is_empty() || start_ms > end_ms {
            return 0;
        }
        self.dedup_events
            .iter()
            .filter(|e| e.market_id == market_id)
            .filter(|e| e.ts_ms >= start_ms && e.ts_ms <= end_ms)
            .count()
    }

    pub fn volume_at_or_better_price(
        &self,
        market_id: &str,
        token_id: &str,
        start_ms: u64,
        end_ms: u64,
        price_limit: f64,
    ) -> f64 {
        if token_id.is_empty() || market_id.is_empty() {
            return 0.0;
        }
        if self.is_empty() {
            return 0.0;
        }
        if start_ms > end_ms {
            return 0.0;
        }
        if !price_limit.is_finite() {
            return 0.0;
        }

        self.trades
            .iter()
            .filter(|t| t.market_id == market_id)
            .filter(|t| t.token_id == token_id)
            .filter(|t| {
                let ts = effective_ingest_ts_ms(t);
                ts >= start_ms && ts <= end_ms
            })
            .filter(|t| t.price.is_finite() && t.size.is_finite())
            .filter(|t| t.price <= price_limit)
            .map(|t| t.size)
            .sum()
    }

    pub fn window_stats(&self, market_id: &str, start_ms: u64, end_ms: u64) -> WindowStats {
        if market_id.trim().is_empty() || start_ms > end_ms || self.is_empty() {
            return WindowStats::default();
        }

        let mut trades_in_window: usize = 0;
        let mut max_gap_ms: u64 = 0;
        let mut prev_ts: Option<u64> = None;

        for t in self.trades.iter() {
            if t.market_id != market_id {
                continue;
            }
            let ts = effective_ingest_ts_ms(t);
            if ts < start_ms || ts > end_ms {
                continue;
            }
            trades_in_window += 1;
            if let Some(prev) = prev_ts {
                max_gap_ms = max_gap_ms.max(ts.saturating_sub(prev));
            }
            prev_ts = Some(ts);
        }

        if trades_in_window == 0 {
            return WindowStats::default();
        }

        WindowStats {
            trades_in_window,
            max_gap_ms,
        }
    }

    #[allow(dead_code)]
    pub fn volume_in_window(
        &self,
        market_id: &str,
        token_id: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> f64 {
        if token_id.is_empty() || market_id.is_empty() {
            return 0.0;
        }
        if self.is_empty() {
            return 0.0;
        }
        if start_ms > end_ms {
            return 0.0;
        }

        self.trades
            .iter()
            .filter(|t| t.market_id == market_id)
            .filter(|t| t.token_id == token_id)
            .filter(|t| {
                let ts = effective_ingest_ts_ms(t);
                ts >= start_ms && ts <= end_ms
            })
            .filter(|t| t.size.is_finite())
            .map(|t| t.size)
            .sum()
    }

    fn trim(&mut self, now_ms: u64) {
        if self.retention_ms == 0 {
            self.trades.clear();
            self.recent_ids.clear();
            self.dedup_events.clear();
            self.needs_full_trim = false;
            return;
        }

        let cutoff = now_ms.saturating_sub(self.retention_ms);
        while self.dedup_events.front().is_some_and(|e| e.ts_ms < cutoff) {
            let _ = self.dedup_events.pop_front();
        }
        while self
            .trades
            .front()
            .is_some_and(|t| effective_ingest_ts_ms(t) < cutoff)
        {
            if let Some(old) = self.trades.pop_front() {
                if !old.trade_id.trim().is_empty() {
                    self.recent_ids.remove(old.trade_id.trim());
                }
            }
        }

        if self.needs_full_trim {
            self.full_trim(cutoff);
            self.needs_full_trim = false;
        }
    }

    fn full_trim(&mut self, cutoff: u64) {
        // Fallback path for out-of-order inserts: we cannot rely on popping from the front.
        let mut new_trades: VecDeque<TradeTick> = VecDeque::with_capacity(self.trades.len());
        let mut new_ids: HashSet<String> = HashSet::with_capacity(self.recent_ids.len());
        for t in self.trades.drain(..) {
            if effective_ingest_ts_ms(&t) < cutoff {
                continue;
            }
            if !t.trade_id.trim().is_empty() {
                new_ids.insert(t.trade_id.clone());
            }
            new_trades.push_back(t);
        }
        self.trades = new_trades;
        self.recent_ids = new_ids;

        let mut new_events: VecDeque<DedupEvent> = VecDeque::with_capacity(self.dedup_events.len());
        for e in self.dedup_events.drain(..) {
            if e.ts_ms < cutoff {
                continue;
            }
            new_events.push_back(e);
        }
        self.dedup_events = new_events;
    }

    fn enforce_cap(&mut self) -> usize {
        if self.max_trades == 0 {
            let evicted = self.trades.len();
            self.trades.clear();
            self.recent_ids.clear();
            self.dedup_events.clear();
            return evicted;
        }

        let mut evicted = 0usize;
        while self.trades.len() > self.max_trades {
            if let Some(old) = self.trades.pop_front() {
                if !old.trade_id.trim().is_empty() {
                    self.recent_ids.remove(old.trade_id.trim());
                }
                evicted += 1;
            } else {
                break;
            }
        }
        evicted
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushResult {
    pub inserted: bool,
    pub duplicated: bool,
    pub evicted: usize,
}

impl PushResult {
    fn dropped() -> Self {
        Self {
            inserted: false,
            duplicated: false,
            evicted: 0,
        }
    }

    fn duplicated() -> Self {
        Self {
            inserted: false,
            duplicated: true,
            evicted: 0,
        }
    }
}

fn fallback_trade_id(t: &TradeTick) -> String {
    format!(
        "weak:{}:{}:{}:{:016x}:{:016x}",
        t.market_id,
        t.token_id,
        t.ts_ms,
        t.price.to_bits(),
        t.size.to_bits()
    )
}

fn effective_ingest_ts_ms(t: &TradeTick) -> u64 {
    if t.ingest_ts_ms > 0 {
        t.ingest_ts_ms
    } else {
        t.ts_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_filter_is_strict() {
        let base = now_ms();
        let mut store = TradeStore::new_with_cap(60_000, usize::MAX);
        assert!(store.is_empty());
        let _ = store.push(TradeTick {
            ts_ms: base,
            ingest_ts_ms: base,
            exchange_ts_ms: Some(base),
            market_id: "m".to_string(),
            token_id: "A".to_string(),
            price: 0.5,
            size: 1.0,
            trade_id: "t1".to_string(),
        });
        assert!(!store.is_empty());
        let _ = store.push(TradeTick {
            ts_ms: base + 10,
            ingest_ts_ms: base + 10,
            exchange_ts_ms: Some(base + 10),
            market_id: "m".to_string(),
            token_id: "A".to_string(),
            price: 0.5,
            size: 2.0,
            trade_id: "t2".to_string(),
        });
        let _ = store.push(TradeTick {
            ts_ms: base + 20,
            ingest_ts_ms: base + 20,
            exchange_ts_ms: Some(base + 20),
            market_id: "m".to_string(),
            token_id: "B".to_string(),
            price: 0.5,
            size: 10.0,
            trade_id: "t3".to_string(),
        });

        let v = store.volume_at_or_better_price("m", "A", base, base + 100, 0.6);
        assert_eq!(v, 3.0);
    }

    #[test]
    fn window_and_price_filters_apply() {
        let base = now_ms();
        let mut store = TradeStore::new_with_cap(60_000, usize::MAX);
        // In window, price <= limit
        let _ = store.push(TradeTick {
            ts_ms: base,
            ingest_ts_ms: base,
            exchange_ts_ms: Some(base),
            market_id: "m".to_string(),
            token_id: "A".to_string(),
            price: 0.49,
            size: 1.0,
            trade_id: "t1".to_string(),
        });
        // In window, price <= limit
        let _ = store.push(TradeTick {
            ts_ms: base + 100,
            ingest_ts_ms: base + 100,
            exchange_ts_ms: Some(base + 100),
            market_id: "m".to_string(),
            token_id: "A".to_string(),
            price: 0.50,
            size: 2.0,
            trade_id: "t2".to_string(),
        });
        // In window, price > limit
        let _ = store.push(TradeTick {
            ts_ms: base + 50,
            ingest_ts_ms: base + 50,
            exchange_ts_ms: Some(base + 50),
            market_id: "m".to_string(),
            token_id: "A".to_string(),
            price: 0.51,
            size: 100.0,
            trade_id: "t3".to_string(),
        });
        // Out of window, price <= limit
        let _ = store.push(TradeTick {
            ts_ms: base.saturating_sub(1),
            ingest_ts_ms: base.saturating_sub(1),
            exchange_ts_ms: Some(base.saturating_sub(1)),
            market_id: "m".to_string(),
            token_id: "A".to_string(),
            price: 0.49,
            size: 100.0,
            trade_id: "t4".to_string(),
        });

        let v = store.volume_at_or_better_price("m", "A", base, base + 100, 0.50);
        assert_eq!(v, 3.0);
    }
}
