use std::path::PathBuf;

use anyhow::Context as _;
use clap::Parser;

use razor::report::{compute_day14_metrics, ReportCfg};

#[derive(Parser, Debug)]
#[command(name = "day14_report", about = "Project Razor Day14 report (GO/NO_GO)")]
struct Args {
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,
    #[arg(long, default_value = "shadow_log.csv")]
    shadow_file: String,
    #[arg(long, default_value_t = 0.15)]
    legging_threshold: f64,
    #[arg(long, default_value_t = 0.0)]
    pnl_threshold: f64,
    #[arg(long, default_value = "day14_report.json")]
    json_out: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir).context("create data_dir")?;

    let shadow_path = args.data_dir.join(&args.shadow_file);
    let out_path = args.data_dir.join(&args.json_out);

    let cfg = ReportCfg {
        data_dir: args.data_dir.display().to_string(),
        legging_ratio_threshold: args.legging_threshold,
        pnl_threshold: args.pnl_threshold,
    };

    let metrics = compute_day14_metrics(&shadow_path, &cfg)?;

    let json = serde_json::to_vec_pretty(&metrics).context("serialize json")?;
    std::fs::write(&out_path, json).with_context(|| format!("write {}", out_path.display()))?;

    let pnl_pass = metrics.total_shadow_pnl > metrics.pnl_threshold;
    let legging_pass =
        metrics.q_fill_avg_sum > 0.0 && metrics.legging_ratio < metrics.legging_ratio_threshold;

    println!("rows_total={}", metrics.rows_total);
    println!("rows_ok={}", metrics.rows_ok);
    println!("rows_bad={}", metrics.rows_bad);
    println!("total_shadow_pnl={:.6}", metrics.total_shadow_pnl);
    println!("pnl_by_bucket.Liquid={:.6}", metrics.pnl_by_bucket.liquid);
    println!("pnl_by_bucket.Thin={:.6}", metrics.pnl_by_bucket.thin);
    println!("pnl_by_bucket.Unknown={:.6}", metrics.pnl_by_bucket.unknown);
    println!("q_req_sum={:.6}", metrics.q_req_sum);
    println!("q_set_sum={:.6}", metrics.q_set_sum);
    println!("q_fill_avg_sum={:.6}", metrics.q_fill_avg_sum);
    println!("legging_ratio={:.6}", metrics.legging_ratio);
    println!(
        "legging_ratio_threshold={:.6}",
        metrics.legging_ratio_threshold
    );
    println!("pnl_threshold={:.6}", metrics.pnl_threshold);
    println!("pass_total_shadow_pnl={pnl_pass}");
    println!("pass_legging_ratio={legging_pass}");
    println!("decision={}", metrics.decision);
    println!("reasons={}", metrics.reasons.join("; "));
    println!("json_out={}", out_path.display());

    Ok(())
}
