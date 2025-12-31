use std::sync::Arc;
use std::time::Duration;

use razor::config::{
    BrainConfig, BucketConfig, CalibrationConfig, Config, LiveConfig, MarketSelectConfig,
    PolymarketConfig, ReportConfig, RunConfig, ShadowConfig, SimConfig,
};
use razor::health::HealthCounters;
use razor::schema::SHADOW_HEADER;
use razor::shadow;
use razor::types::{now_ms, Bps, Bucket, BucketMetrics, Leg, Side, Signal, Strategy};
use tokio::sync::{mpsc, watch};

#[tokio::test]
async fn dedup_hits_same_second_key() {
    let cfg = Config {
        polymarket: PolymarketConfig::default(),
        run: RunConfig {
            data_dir: "data".into(),
            market_ids: vec![],
        },
        schema_version: razor::schema::SCHEMA_VERSION.to_string(),
        brain: BrainConfig::default(),
        buckets: BucketConfig::default(),
        shadow: ShadowConfig {
            window_start_ms: 0,
            window_end_ms: 1,
            ..ShadowConfig::default()
        },
        market_select: MarketSelectConfig::default(),
        report: ReportConfig::default(),
        live: LiveConfig::default(),
        calibration: CalibrationConfig::default(),
        sim: SimConfig::default(),
    };

    let tmp = std::env::temp_dir().join(format!("razor_shadow_dedup_{}.csv", std::process::id()));
    let _ = std::fs::remove_file(&tmp);

    let (trade_tx, trade_rx) = mpsc::channel(16);
    let (signal_tx, signal_rx) = mpsc::channel(16);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let health = Arc::new(HealthCounters::default());
    let handle = tokio::spawn(shadow::run(
        cfg.clone(),
        vec![],
        trade_rx,
        signal_rx,
        tmp.clone(),
        health,
        shutdown_rx,
    ));

    let base_ms = now_ms().saturating_sub(5);
    let base_signal = Signal {
        run_id: "run_test".to_string(),
        signal_id: 1,
        signal_ts_ms: base_ms,
        market_id: "mkt".to_string(),
        cycle_id: "run_test:mkt:binary:1".to_string(),
        market_slug: None,
        market_type: None,
        strategy: Strategy::Binary,
        bucket: Bucket::Liquid,
        reasons: Vec::new(),
        q_req: 10.0,
        raw_cost_bps: Bps::from_price_cost(0.97),
        raw_edge_bps: Bps::new(300),
        hard_fees_bps: Bps::FEE_POLY + Bps::FEE_MERGE,
        risk_premium_bps: Bps::new(80),
        expected_net_bps: Bps::new(10),
        bucket_metrics: BucketMetrics {
            worst_leg_index: 0,
            worst_spread_bps: 0,
            worst_depth3_usdc: 1000.0,
            is_depth3_degraded: false,
        },
        legs: vec![
            Leg {
                leg_index: 0,
                token_id: "A".to_string(),
                side: Side::Buy,
                limit_price: 0.49,
                qty: 10.0,
                best_bid_at_signal: 0.48,
                best_ask_at_signal: 0.49,
            },
            Leg {
                leg_index: 1,
                token_id: "B".to_string(),
                side: Side::Buy,
                limit_price: 0.48,
                qty: 10.0,
                best_bid_at_signal: 0.47,
                best_ask_at_signal: 0.48,
            },
        ],
    };
    let mut second_signal = base_signal.clone();
    second_signal.signal_id = 2;

    signal_tx.send(base_signal).await.expect("send signal");
    signal_tx.send(second_signal).await.expect("send signal");

    tokio::time::sleep(Duration::from_millis(120)).await;
    shutdown_tx.send(true).expect("shutdown");
    drop(signal_tx);
    drop(trade_tx);

    handle.await.expect("shadow task").expect("shadow run");

    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(&tmp)
        .expect("open csv");
    let header = rdr.headers().expect("header").clone();
    let idx_notes = header
        .iter()
        .position(|h| h.eq_ignore_ascii_case("notes"))
        .expect("notes col");
    let idx_total_pnl = header
        .iter()
        .position(|h| h.eq_ignore_ascii_case("total_pnl"))
        .expect("total_pnl col");

    let mut saw_dedup = false;
    for record in rdr.records() {
        let record = record.expect("record");
        if record.len() != SHADOW_HEADER.len() {
            continue;
        }
        let notes = record.get(idx_notes).unwrap_or("");
        if notes.contains("reason=DEDUP_HIT") {
            let total_pnl = record
                .get(idx_total_pnl)
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(1.0);
            assert_eq!(total_pnl, 0.0);
            saw_dedup = true;
        }
    }

    assert!(saw_dedup, "expected DEDUP_HIT row");
}
