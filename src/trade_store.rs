use std::collections::VecDeque;

use crate::types::{now_ms, TradeTick};

/// In-memory ring buffer for Shadow volume queries (Phase 1).
///
/// Correctness first: O(n) scans are acceptable at Phase 1 scale.
pub struct TradeStore {
    retention_ms: u64,
    trades: VecDeque<TradeTick>,
}

impl TradeStore {
    pub fn new(retention_ms: u64) -> Self {
        Self {
            retention_ms,
            trades: VecDeque::new(),
        }
    }

    pub fn push(&mut self, t: TradeTick) {
        if t.token_id.is_empty() {
            return;
        }
        self.trades.push_back(t);
        self.trim(now_ms());
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
            .filter(|t| t.ts_ms >= start_ms && t.ts_ms <= end_ms)
            .filter(|t| t.price.is_finite() && t.size.is_finite())
            .filter(|t| t.price <= price_limit)
            .map(|t| t.size)
            .sum()
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
        if start_ms > end_ms {
            return 0.0;
        }

        self.trades
            .iter()
            .filter(|t| t.market_id == market_id)
            .filter(|t| t.token_id == token_id)
            .filter(|t| t.ts_ms >= start_ms && t.ts_ms <= end_ms)
            .filter(|t| t.size.is_finite())
            .map(|t| t.size)
            .sum()
    }

    fn trim(&mut self, now_ms: u64) {
        if self.retention_ms == 0 {
            self.trades.clear();
            return;
        }

        let cutoff = now_ms.saturating_sub(self.retention_ms);
        while self.trades.front().is_some_and(|t| t.ts_ms < cutoff) {
            self.trades.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_filter_is_strict() {
        let base = now_ms();
        let mut store = TradeStore::new(60_000);
        store.push(TradeTick {
            ts_ms: base,
            market_id: "m".to_string(),
            token_id: "A".to_string(),
            price: 0.5,
            size: 1.0,
        });
        store.push(TradeTick {
            ts_ms: base + 10,
            market_id: "m".to_string(),
            token_id: "A".to_string(),
            price: 0.5,
            size: 2.0,
        });
        store.push(TradeTick {
            ts_ms: base + 20,
            market_id: "m".to_string(),
            token_id: "B".to_string(),
            price: 0.5,
            size: 10.0,
        });

        let v = store.volume_at_or_better_price("m", "A", base, base + 100, 0.6);
        assert_eq!(v, 3.0);
    }

    #[test]
    fn window_and_price_filters_apply() {
        let base = now_ms();
        let mut store = TradeStore::new(60_000);
        // In window, price <= limit
        store.push(TradeTick {
            ts_ms: base,
            market_id: "m".to_string(),
            token_id: "A".to_string(),
            price: 0.49,
            size: 1.0,
        });
        // In window, price <= limit
        store.push(TradeTick {
            ts_ms: base + 100,
            market_id: "m".to_string(),
            token_id: "A".to_string(),
            price: 0.50,
            size: 2.0,
        });
        // In window, price > limit
        store.push(TradeTick {
            ts_ms: base + 50,
            market_id: "m".to_string(),
            token_id: "A".to_string(),
            price: 0.51,
            size: 100.0,
        });
        // Out of window, price <= limit
        store.push(TradeTick {
            ts_ms: base.saturating_sub(1),
            market_id: "m".to_string(),
            token_id: "A".to_string(),
            price: 0.49,
            size: 100.0,
        });

        let v = store.volume_at_or_better_price("m", "A", base, base + 100, 0.50);
        assert_eq!(v, 3.0);
    }
}
