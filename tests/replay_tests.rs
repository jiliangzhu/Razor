use std::path::PathBuf;

use assert_approx_eq::assert_approx_eq;

#[test]
fn replay_fixture_produces_expected_totals() -> anyhow::Result<()> {
    let run_dir = PathBuf::from("tests/fixtures/replay_small");
    assert!(run_dir.exists());

    let out_dir = std::env::temp_dir().join(format!(
        "razor_replay_test_{}_{}",
        std::process::id(),
        razor::types::now_ms()
    ));
    let _ = std::fs::remove_dir_all(&out_dir);

    let replay_run_id = "replay_test".to_string();
    let res = razor::replay::run_replay(
        &run_dir,
        razor::replay::ReplayOptions {
            out_dir: out_dir.clone(),
            replay_run_id: replay_run_id.clone(),
        },
    )?;

    assert_eq!(res.signals, 1);
    assert!(out_dir.join(razor::replay::FILE_REPLAY_SHADOW_LOG).exists());
    assert!(out_dir
        .join(razor::replay::FILE_REPLAY_REPORT_JSON)
        .exists());
    assert!(out_dir.join(razor::replay::FILE_REPLAY_REPORT_MD).exists());

    let replay_shadow = out_dir.join(razor::replay::FILE_REPLAY_SHADOW_LOG);
    let mut rdr = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(&replay_shadow)?;
    let header = rdr.headers()?.clone();

    let expected_header: Vec<&str> = razor::schema::SHADOW_HEADER.to_vec();
    assert_eq!(header.iter().collect::<Vec<_>>(), expected_header);

    let run_id_idx = header
        .iter()
        .position(|h| h.eq_ignore_ascii_case("run_id"))
        .unwrap();
    let total_pnl_idx = header
        .iter()
        .position(|h| h.eq_ignore_ascii_case("total_pnl"))
        .unwrap();

    let row = rdr.records().next().unwrap()?;
    assert_eq!(row.get(run_id_idx).unwrap(), replay_run_id);

    let total_pnl: f64 = row.get(total_pnl_idx).unwrap().parse()?;
    assert_approx_eq!(total_pnl, -0.1660605, 1e-5);

    let thresholds = razor::report::ReportThresholds {
        min_total_shadow_pnl: 0.0,
        min_avg_set_ratio: 0.85,
    };
    let report =
        razor::report::compute_report(&out_dir.join("shadow_log.csv"), &replay_run_id, thresholds)?;
    assert_eq!(report.totals.signals, 1);
    assert_approx_eq!(report.totals.total_shadow_pnl, total_pnl, 1e-9);

    let _ = std::fs::remove_dir_all(&out_dir);
    Ok(())
}
