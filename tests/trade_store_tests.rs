use assert_approx_eq::assert_approx_eq;

use razor::trade_store::TradeStore;
use razor::types::TradeTick;

#[test]
fn token_filter_is_strict() {
    let mut store = TradeStore::new(u64::MAX);

    store.push(TradeTick {
        ts_ms: 1_000,
        market_id: "m".to_string(),
        token_id: "A".to_string(),
        price: 0.5,
        size: 1.0,
    });
    store.push(TradeTick {
        ts_ms: 1_010,
        market_id: "m".to_string(),
        token_id: "A".to_string(),
        price: 0.5,
        size: 2.0,
    });
    store.push(TradeTick {
        ts_ms: 1_020,
        market_id: "m".to_string(),
        token_id: "B".to_string(),
        price: 0.5,
        size: 10.0,
    });

    let v = store.volume_at_or_better_price("m", "A", 1_000, 1_100, 0.6);
    assert_approx_eq!(v, 3.0, 1e-12);
}

#[test]
fn price_limit_and_window_filters_apply() {
    let mut store = TradeStore::new(u64::MAX);

    // In window, price <= limit
    store.push(TradeTick {
        ts_ms: 1_000,
        market_id: "m".to_string(),
        token_id: "A".to_string(),
        price: 0.49,
        size: 1.0,
    });
    // In window, price <= limit
    store.push(TradeTick {
        ts_ms: 1_100,
        market_id: "m".to_string(),
        token_id: "A".to_string(),
        price: 0.50,
        size: 2.0,
    });
    // In window, price > limit
    store.push(TradeTick {
        ts_ms: 1_050,
        market_id: "m".to_string(),
        token_id: "A".to_string(),
        price: 0.51,
        size: 100.0,
    });
    // Out of window, price <= limit
    store.push(TradeTick {
        ts_ms: 999,
        market_id: "m".to_string(),
        token_id: "A".to_string(),
        price: 0.49,
        size: 100.0,
    });

    let v = store.volume_at_or_better_price("m", "A", 1_000, 1_100, 0.50);
    assert_approx_eq!(v, 3.0, 1e-12);
}
