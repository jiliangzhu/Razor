use assert_approx_eq::assert_approx_eq;

use razor::trade_store::TradeStore;
use razor::types::TradeTick;

#[test]
fn token_filter_is_strict() {
    let mut store = TradeStore::new_with_cap(u64::MAX, usize::MAX);

    store.push(TradeTick {
        ts_ms: 1_000,
        ingest_ts_ms: 1_000,
        exchange_ts_ms: Some(1_000),
        market_id: "m".to_string(),
        token_id: "A".to_string(),
        price: 0.5,
        size: 1.0,
        trade_id: "t1".to_string(),
    });
    store.push(TradeTick {
        ts_ms: 1_010,
        ingest_ts_ms: 1_010,
        exchange_ts_ms: Some(1_010),
        market_id: "m".to_string(),
        token_id: "A".to_string(),
        price: 0.5,
        size: 2.0,
        trade_id: "t2".to_string(),
    });
    store.push(TradeTick {
        ts_ms: 1_020,
        ingest_ts_ms: 1_020,
        exchange_ts_ms: Some(1_020),
        market_id: "m".to_string(),
        token_id: "B".to_string(),
        price: 0.5,
        size: 10.0,
        trade_id: "t3".to_string(),
    });

    let v = store.volume_at_or_better_price("m", "A", 1_000, 1_100, 0.6);
    assert_approx_eq!(v, 3.0, 1e-12);
}

#[test]
fn price_limit_and_window_filters_apply() {
    let mut store = TradeStore::new_with_cap(u64::MAX, usize::MAX);

    // In window, price <= limit
    store.push(TradeTick {
        ts_ms: 1_000,
        ingest_ts_ms: 1_000,
        exchange_ts_ms: Some(1_000),
        market_id: "m".to_string(),
        token_id: "A".to_string(),
        price: 0.49,
        size: 1.0,
        trade_id: "t1".to_string(),
    });
    // In window, price <= limit
    store.push(TradeTick {
        ts_ms: 1_100,
        ingest_ts_ms: 1_100,
        exchange_ts_ms: Some(1_100),
        market_id: "m".to_string(),
        token_id: "A".to_string(),
        price: 0.50,
        size: 2.0,
        trade_id: "t2".to_string(),
    });
    // In window, price > limit
    store.push(TradeTick {
        ts_ms: 1_050,
        ingest_ts_ms: 1_050,
        exchange_ts_ms: Some(1_050),
        market_id: "m".to_string(),
        token_id: "A".to_string(),
        price: 0.51,
        size: 100.0,
        trade_id: "t3".to_string(),
    });
    // Out of window, price <= limit
    store.push(TradeTick {
        ts_ms: 999,
        ingest_ts_ms: 999,
        exchange_ts_ms: Some(999),
        market_id: "m".to_string(),
        token_id: "A".to_string(),
        price: 0.49,
        size: 100.0,
        trade_id: "t4".to_string(),
    });

    let v = store.volume_at_or_better_price("m", "A", 1_000, 1_100, 0.50);
    assert_approx_eq!(v, 3.0, 1e-12);
}
