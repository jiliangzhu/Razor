use std::fs;
use std::path::PathBuf;

use razor::reasons::{compute_reason_agg, parse_notes_reasons, ShadowReason};

fn tmp_csv(name: &str, contents: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "razor_reasons_{name}_{}_{}.csv",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    fs::write(&p, contents).expect("write tmp csv");
    p
}

#[test]
fn reason_code_display_is_strict() {
    assert_eq!(ShadowReason::NoTrades.to_string(), "NO_TRADES");
    assert_eq!(ShadowReason::WindowEmpty.to_string(), "WINDOW_EMPTY");
    assert_eq!(ShadowReason::DedupHit.to_string(), "DEDUP_HIT");
    assert_eq!(ShadowReason::BucketNan.to_string(), "BUCKET_NAN");
    assert_eq!(ShadowReason::LegsPadded.to_string(), "LEGS_PADDED");
    assert_eq!(ShadowReason::MissingBid.to_string(), "MISSING_BID");
    assert_eq!(ShadowReason::InvalidPrice.to_string(), "INVALID_PRICE");
    assert_eq!(ShadowReason::InvalidQty.to_string(), "INVALID_QTY");
}

#[test]
fn parse_notes_reasons_strips_diag_kv() {
    let got = parse_notes_reasons("NO_TRADES|MISSING_BID|TS_SRC=local|LAT_MS=100");
    assert_eq!(
        got,
        vec!["NO_TRADES".to_string(), "MISSING_BID".to_string()]
    );
}

#[test]
fn reason_agg_groups_count_and_pnl() {
    let csv = concat!(
        "run_id,total_pnl,notes\n",
        "r1,-1.0,NO_TRADES|MISSING_BID|BID=0\n",
        "r1,2.0,NO_TRADES\n",
        "r2,100.0,NO_TRADES\n",
    );
    let path = tmp_csv("agg", csv);

    let agg = compute_reason_agg(&path, "r1").expect("agg");

    let no_trades = agg.get("NO_TRADES").expect("NO_TRADES");
    assert_eq!(no_trades.count, 2);
    assert!((no_trades.sum_pnl - 1.0).abs() < 1e-12);
    assert!((no_trades.worst_pnl - (-1.0)).abs() < 1e-12);

    let missing_bid = agg.get("MISSING_BID").expect("MISSING_BID");
    assert_eq!(missing_bid.count, 1);
    assert!((missing_bid.sum_pnl - (-1.0)).abs() < 1e-12);
    assert!((missing_bid.worst_pnl - (-1.0)).abs() < 1e-12);
}
