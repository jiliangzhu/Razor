use std::path::{Path, PathBuf};

use anyhow::Context as _;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::Config;
use crate::recorder::CsvAppender;
use crate::schema::CALIBRATION_LOG_HEADER;
use crate::types::{now_ms, Bucket, Side};

#[derive(Debug, Clone)]
pub struct CalibrationEvent {
    pub ts_ms: u64,
    pub bucket: Bucket,
    pub market_id: String,
    pub token_id: String,
    pub side: Side,
    pub req_qty: f64,
    pub filled_qty: f64,
    pub market_ask_size_best: f64,
    pub market_bid_size_best: f64,
    pub sim_fill_share_used: f64,
    pub mode: String, // "SIM"
}

pub async fn run(
    cfg: Config,
    mut rx: mpsc::Receiver<CalibrationEvent>,
    calibration_log_path: PathBuf,
) -> anyhow::Result<()> {
    let mut out = CsvAppender::open(calibration_log_path, &CALIBRATION_LOG_HEADER)
        .context("open calibration_log.csv")?;

    let q = cfg.calibration.quantile;
    if (q - 0.25).abs() > 1e-9 {
        warn!(
            quantile = q,
            "config calibration.quantile is ignored; Frozen Spec uses p25 (0.25)"
        );
    }

    let mut samples_liquid: Vec<f64> = Vec::new();
    let mut samples_thin: Vec<f64> = Vec::new();

    let mut last_written_liquid = 0usize;
    let mut last_written_thin = 0usize;

    let min_n = cfg.calibration.min_samples_per_bucket.max(1);

    loop {
        let Some(ev) = rx.recv().await else {
            return Err(anyhow::anyhow!("calibration channel closed"));
        };

        out.write_record([
            ev.ts_ms.to_string(),
            ev.bucket.as_str().to_string(),
            ev.market_id.clone(),
            ev.token_id.clone(),
            ev.side.as_str().to_string(),
            ev.req_qty.to_string(),
            ev.filled_qty.to_string(),
            ev.market_ask_size_best.to_string(),
            ev.market_bid_size_best.to_string(),
            ev.sim_fill_share_used.to_string(),
            ev.mode.clone(),
        ])?;

        if let Some(sample) = real_share_sample(ev.req_qty, ev.filled_qty) {
            match ev.bucket {
                Bucket::Liquid => samples_liquid.push(sample),
                Bucket::Thin => samples_thin.push(sample),
            }
        }

        let liquid_n = samples_liquid.len();
        let thin_n = samples_thin.len();
        let should_write = (liquid_n >= min_n || thin_n >= min_n)
            && (liquid_n != last_written_liquid || thin_n != last_written_thin);

        if !should_write {
            continue;
        }

        let liquid_p25 = if liquid_n >= min_n {
            p25(&samples_liquid)
        } else {
            0.0
        };
        let thin_p25 = if thin_n >= min_n {
            p25(&samples_thin)
        } else {
            0.0
        };

        write_suggest_toml(
            &cfg.run.data_dir,
            &cfg.calibration.suggest_filename,
            now_ms(),
            liquid_n,
            liquid_p25,
            thin_n,
            thin_p25,
        )
        .context("write calibration_suggest.toml")?;

        info!(
            liquid_samples = liquid_n,
            liquid_p25,
            thin_samples = thin_n,
            thin_p25,
            "calibration suggest written"
        );

        last_written_liquid = liquid_n;
        last_written_thin = thin_n;
    }
}

fn real_share_sample(req_qty: f64, filled_qty: f64) -> Option<f64> {
    if !req_qty.is_finite() || !filled_qty.is_finite() {
        return None;
    }
    if req_qty <= 0.0 || filled_qty < 0.0 {
        return None;
    }
    let v = filled_qty / req_qty;
    if !v.is_finite() {
        return None;
    }
    Some(v.clamp(0.0, 1.0))
}

fn p25(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut v: Vec<f64> = samples.iter().copied().filter(|x| x.is_finite()).collect();
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = (((v.len() - 1) as f64) * 0.25).floor() as usize;
    v[idx.min(v.len() - 1)]
}

fn write_suggest_toml(
    data_dir: &Path,
    filename: &str,
    generated_at_ms: u64,
    liquid_samples: usize,
    liquid_p25: f64,
    thin_samples: usize,
    thin_p25: f64,
) -> anyhow::Result<()> {
    let mut out = String::new();
    out.push_str("[calibration_suggest]\n");
    out.push_str(&format!("generated_at_ms = {generated_at_ms}\n\n"));

    out.push_str("[calibration_suggest.liquid]\n");
    out.push_str(&format!("samples = {liquid_samples}\n"));
    out.push_str(&format!("p25 = {liquid_p25:.6}\n\n"));

    out.push_str("[calibration_suggest.thin]\n");
    out.push_str(&format!("samples = {thin_samples}\n"));
    out.push_str(&format!("p25 = {thin_p25:.6}\n"));

    let path = data_dir.join(filename);
    if let Err(e) = std::fs::write(&path, out.as_bytes()) {
        warn!(error = %e, path = %path.display(), "write calibration_suggest.toml failed");
        return Err(anyhow::anyhow!(e));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p25_index_matches_spec() {
        let samples: Vec<f64> = (0..40).map(|i| i as f64 / 100.0).collect();
        // 40 samples => idx = floor(0.25*(39)) = 9
        assert_eq!(p25(&samples), 0.09);
    }

    #[test]
    fn real_share_sample_is_clamped_and_validated() {
        assert_eq!(real_share_sample(10.0, 3.0).unwrap(), 0.3);
        assert_eq!(real_share_sample(10.0, 30.0).unwrap(), 1.0);
        assert!(real_share_sample(0.0, 1.0).is_none());
        assert!(real_share_sample(10.0, f64::NAN).is_none());
    }
}
