use std::fs;
use std::path::PathBuf;

use razor::report::{compute_day14_metrics, ReportCfg};

fn tmp_csv(name: &str, contents: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "razor_day14_{name}_{}_{}.csv",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    fs::write(&p, contents).expect("write tmp csv");
    p
}

fn cfg() -> ReportCfg {
    ReportCfg {
        data_dir: "data".to_string(),
        legging_ratio_threshold: 0.15,
        pnl_threshold: 0.0,
    }
}

#[test]
fn go_case() {
    let csv = "\
market_id,bucket,q_req,q_set,q_fill1,q_fill2,q_fill3,token1,token2,token3,pnl_total\n\
m1,Liquid,10,9,10,10,0,A,B,,1.0\n\
m2,Thin,10,9,10,10,0,C,D,,0.5\n\
";
    let path = tmp_csv("go", csv);
    let metrics = compute_day14_metrics(&path, &cfg()).expect("metrics");
    assert_eq!(metrics.rows_total, 2);
    assert_eq!(metrics.rows_ok, 2);
    assert_eq!(metrics.rows_bad, 0);
    assert_eq!(metrics.decision, "GO");
    assert!(metrics.total_shadow_pnl > 0.0);
    assert!(metrics.legging_ratio < metrics.legging_ratio_threshold);
}

#[test]
fn no_go_pnl_negative() {
    let csv = "\
market_id,bucket,q_req,q_set,q_fill1,q_fill2,token1,token2,pnl_total\n\
m1,Liquid,10,9,10,10,A,B,-0.1\n\
";
    let path = tmp_csv("no_go_pnl", csv);
    let metrics = compute_day14_metrics(&path, &cfg()).expect("metrics");
    assert_eq!(metrics.decision, "NO_GO");
    assert!(metrics.total_shadow_pnl <= metrics.pnl_threshold);
}

#[test]
fn no_go_legging_too_high() {
    let csv = "\
market_id,bucket,q_req,q_set,q_fill1,q_fill2,token1,token2,pnl_total\n\
m1,Liquid,10,5,10,10,A,B,1.0\n\
";
    let path = tmp_csv("no_go_leg", csv);
    let metrics = compute_day14_metrics(&path, &cfg()).expect("metrics");
    assert_eq!(metrics.decision, "NO_GO");
    assert!(metrics.total_shadow_pnl > 0.0);
    assert!(metrics.legging_ratio >= metrics.legging_ratio_threshold);
}

#[test]
fn empty_file_header_only() {
    let csv = "market_id,bucket,q_req,q_set,q_fill1,q_fill2,token1,token2,pnl_total\n";
    let path = tmp_csv("empty", csv);
    let metrics = compute_day14_metrics(&path, &cfg()).expect("metrics");
    assert_eq!(metrics.rows_total, 0);
    assert_eq!(metrics.rows_ok, 0);
    assert_eq!(metrics.rows_bad, 0);
    assert_eq!(metrics.decision, "NO_GO");
}

#[test]
fn bad_row_is_counted_and_ignored() {
    let csv = "\
market_id,bucket,q_req,q_set,q_fill1,q_fill2,token1,token2,pnl_total\n\
m1,Liquid,10,9,10,10,A,B,1.0\n\
m2,Liquid,10,9,10,10,A,B,not_a_number\n\
m3,Thin,10,9,10,10,A,B,0.5\n\
";
    let path = tmp_csv("bad_row", csv);
    let metrics = compute_day14_metrics(&path, &cfg()).expect("metrics");
    assert_eq!(metrics.rows_total, 3);
    assert_eq!(metrics.rows_ok, 2);
    assert_eq!(metrics.rows_bad, 1);
    assert!((metrics.total_shadow_pnl - 1.5).abs() < 1e-12);
}

#[test]
fn missing_required_column_fails() {
    let csv = "\
market_id,bucket,q_req,q_set,q_fill1,q_fill2,token1,token2\n\
m1,Liquid,10,9,10,10,A,B\n\
";
    let path = tmp_csv("missing_col", csv);
    let err = compute_day14_metrics(&path, &cfg()).unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.to_ascii_lowercase().contains("pnl_total"));
}
