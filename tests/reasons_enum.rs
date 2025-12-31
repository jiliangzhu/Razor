use std::fs;
use std::path::PathBuf;

use razor::reasons::{compute_reason_agg, format_notes, parse_notes_reasons, Reason};

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
    assert_eq!(Reason::Ok.to_string(), "OK");
    assert_eq!(Reason::NoTrades.to_string(), "NO_TRADES");
    assert_eq!(Reason::WindowEmpty.to_string(), "WINDOW_EMPTY");
    assert_eq!(Reason::DedupHit.to_string(), "DEDUP_HIT");
    assert_eq!(Reason::BucketThinNan.to_string(), "BUCKET_THIN_NAN");
    assert_eq!(Reason::MissingBid.to_string(), "MISSING_BID");
    assert_eq!(Reason::InvalidPrice.to_string(), "INVALID_PRICE");
    assert_eq!(Reason::InvalidQty.to_string(), "INVALID_QTY");
}

#[test]
fn format_notes_emits_reason_and_cycle_id() {
    let notes = format_notes(Reason::NoTrades, "run:market:binary:1");
    assert_eq!(notes, "reason=NO_TRADES,cycle_id=run:market:binary:1");
}

#[test]
fn parse_notes_reasons_reads_reason_pairs() {
    let got = parse_notes_reasons("reason=NO_TRADES,cycle_id=abc");
    assert_eq!(got, vec!["NO_TRADES".to_string()]);
}

#[test]
fn reason_agg_groups_count_and_pnl() {
    let csv = concat!(
        "run_id,total_pnl,notes\n",
        "r1,-1.0,\"reason=NO_TRADES,cycle_id=abc\"\n",
        "r1,2.0,reason=NO_TRADES,cycle_id=def\n",
        "r2,100.0,reason=NO_TRADES,cycle_id=ghi\n",
    );
    let path = tmp_csv("agg", csv);

    let agg = compute_reason_agg(&path, "r1").expect("agg");

    let no_trades = agg.get("NO_TRADES").expect("NO_TRADES");
    assert_eq!(no_trades.count, 2);
    assert!((no_trades.sum_pnl - 1.0).abs() < 1e-12);
    assert!((no_trades.worst_pnl - (-1.0)).abs() < 1e-12);
}
