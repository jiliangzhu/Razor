use std::fs;
use std::path::PathBuf;

use razor::reasons::{compute_reason_agg, parse_notes_reasons, ShadowNoteReason};

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
    assert_eq!(ShadowNoteReason::NoTrades.to_string(), "NO_TRADES");
    assert_eq!(ShadowNoteReason::WindowEmpty.to_string(), "WINDOW_EMPTY");
    assert_eq!(
        ShadowNoteReason::TradeSizeSuspect.to_string(),
        "TRADE_SIZE_SUSPECT"
    );
    assert_eq!(ShadowNoteReason::DedupHit.to_string(), "DEDUP_HIT");
    assert_eq!(
        ShadowNoteReason::BucketThinNan.to_string(),
        "BUCKET_THIN_NAN"
    );
    assert_eq!(ShadowNoteReason::MissingBid.to_string(), "MISSING_BID");
    assert_eq!(ShadowNoteReason::InvalidPrice.to_string(), "INVALID_PRICE");
    assert_eq!(ShadowNoteReason::InvalidQty.to_string(), "INVALID_QTY");
}

#[test]
fn parse_notes_reasons_splits_csv_notes() {
    let got = parse_notes_reasons("NO_TRADES,MISSING_BID");
    assert_eq!(
        got,
        vec!["NO_TRADES".to_string(), "MISSING_BID".to_string()]
    );
}

#[test]
fn reason_agg_groups_count_and_pnl() {
    let csv = concat!(
        "run_id,total_pnl,notes\n",
        "r1,-1.0,\"NO_TRADES,MISSING_BID\"\n",
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
