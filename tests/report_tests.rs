use std::fs;
use std::path::PathBuf;

use razor::report::{compute_report, ReportThresholds};
use razor::schema::{SCHEMA_VERSION, SHADOW_HEADER};

fn tmp_csv(name: &str, contents: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "razor_report_{name}_{}_{}.csv",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    fs::write(&p, contents).expect("write tmp csv");
    p
}

fn header_line() -> String {
    let mut s = SHADOW_HEADER.join(",");
    s.push('\n');
    s
}

fn idx(name: &str) -> usize {
    SHADOW_HEADER
        .iter()
        .position(|h| h.trim().eq_ignore_ascii_case(name))
        .unwrap_or_else(|| panic!("missing column {name} in SHADOW_HEADER"))
}

#[allow(clippy::too_many_arguments)]
fn row(
    run_id: &str,
    signal_id: u64,
    ts_ms: u64,
    market_id: &str,
    strategy: &str,
    bucket: &str,
    total_pnl: &str,
    set_ratio: &str,
) -> String {
    let mut cols: Vec<String> = vec![String::new(); SHADOW_HEADER.len()];
    cols[idx("run_id")] = run_id.to_string();
    cols[idx("schema_version")] = SCHEMA_VERSION.to_string();
    cols[idx("signal_id")] = signal_id.to_string();
    cols[idx("signal_ts_unix_ms")] = ts_ms.to_string();
    cols[idx("window_start_ms")] = "100".to_string();
    cols[idx("window_end_ms")] = "1100".to_string();
    cols[idx("market_id")] = market_id.to_string();
    cols[idx("strategy")] = strategy.to_string();
    cols[idx("bucket")] = bucket.to_string();
    cols[idx("total_pnl")] = total_pnl.to_string();
    cols[idx("set_ratio")] = set_ratio.to_string();

    let mut s = cols.join(",");
    s.push('\n');
    s
}

#[test]
fn go_case() {
    let run_id = "run_1";
    let csv = format!(
        "{}{}{}",
        header_line(),
        row(run_id, 1, 1_000, "m1", "binary", "liquid", "1.0", "0.90"),
        row(run_id, 2, 2_000, "m2", "triangle", "thin", "0.5", "0.90"),
    );
    let path = tmp_csv("go", &csv);

    let report = compute_report(&path, run_id, ReportThresholds::default()).expect("report");
    assert!(report.verdict.go);
    assert_eq!(report.totals.signals, 2);
    assert!((report.totals.total_shadow_pnl - 1.5).abs() < 1e-12);
    assert!((report.totals.avg_set_ratio - 0.9).abs() < 1e-12);
    assert_eq!(report.worst_20.len(), 2);
    assert!(report.worst_20[0].total_pnl <= report.worst_20[1].total_pnl);
}

#[test]
fn no_go_pnl_negative() {
    let run_id = "run_2";
    let csv = format!(
        "{}{}",
        header_line(),
        row(run_id, 1, 1_000, "m1", "binary", "liquid", "-0.1", "1.0"),
    );
    let path = tmp_csv("no_go_pnl", &csv);

    let report = compute_report(&path, run_id, ReportThresholds::default()).expect("report");
    assert!(!report.verdict.go);
    assert!(report.totals.total_shadow_pnl < 0.0);
}

#[test]
fn no_go_set_ratio_too_low() {
    let run_id = "run_3";
    let csv = format!(
        "{}{}",
        header_line(),
        row(run_id, 1, 1_000, "m1", "binary", "liquid", "1.0", "0.50"),
    );
    let path = tmp_csv("no_go_ratio", &csv);

    let report = compute_report(&path, run_id, ReportThresholds::default()).expect("report");
    assert!(!report.verdict.go);
    assert!(report.totals.total_shadow_pnl > 0.0);
    assert!(report.totals.avg_set_ratio < 0.85);
}

#[test]
fn empty_file_header_only() {
    let run_id = "run_4";
    let csv = header_line();
    let path = tmp_csv("empty", &csv);

    let report = compute_report(&path, run_id, ReportThresholds::default()).expect("report");
    assert_eq!(report.totals.signals, 0);
    assert!(!report.verdict.go);
}

#[test]
fn bad_row_is_counted_and_ignored() {
    let run_id = "run_5";
    let csv = format!(
        "{}{}{}{}",
        header_line(),
        row(run_id, 1, 1_000, "m1", "binary", "liquid", "1.0", "0.90"),
        row(
            run_id,
            2,
            2_000,
            "m2",
            "binary",
            "liquid",
            "not_a_number",
            "0.90"
        ),
        row(run_id, 3, 3_000, "m3", "triangle", "thin", "0.5", "0.90"),
    );
    let path = tmp_csv("bad_row", &csv);

    let report = compute_report(&path, run_id, ReportThresholds::default()).expect("report");
    assert_eq!(report.rows_total, 3);
    assert_eq!(report.rows_bad, 1);
    assert_eq!(report.totals.signals, 2);
    assert!((report.totals.total_shadow_pnl - 1.5).abs() < 1e-12);
}
