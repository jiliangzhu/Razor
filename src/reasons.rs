use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use anyhow::Context as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShadowReason {
    NoTrades,
    WindowEmpty,
    #[allow(dead_code)]
    DedupHit,
    BucketNan,
    LegsPadded,
    MissingBid,
    InvalidPrice,
    InvalidQty,
}

impl ShadowReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            ShadowReason::NoTrades => "NO_TRADES",
            ShadowReason::WindowEmpty => "WINDOW_EMPTY",
            ShadowReason::DedupHit => "DEDUP_HIT",
            ShadowReason::BucketNan => "BUCKET_NAN",
            ShadowReason::LegsPadded => "LEGS_PADDED",
            ShadowReason::MissingBid => "MISSING_BID",
            ShadowReason::InvalidPrice => "INVALID_PRICE",
            ShadowReason::InvalidQty => "INVALID_QTY",
        }
    }
}

impl fmt::Display for ShadowReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn format_notes(reasons: &[ShadowReason], kv_tokens: &[String]) -> String {
    let mut uniq: BTreeSet<&'static str> = BTreeSet::new();
    for r in reasons {
        uniq.insert(r.as_str());
    }

    let mut out: Vec<String> = Vec::new();
    if uniq.is_empty() {
        out.push("OK".to_string());
    } else {
        out.extend(uniq.into_iter().map(|s| s.to_string()));
    }

    for kv in kv_tokens {
        let kv = kv.trim();
        if kv.is_empty() {
            continue;
        }
        out.push(kv.to_string());
    }

    out.join("|")
}

#[allow(dead_code)]
pub fn parse_notes_reasons(notes: &str) -> Vec<String> {
    let left = notes.split(';').next().unwrap_or("");
    left.split('|')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .filter(|s| !s.contains('='))
        .map(|s| s.to_string())
        .collect()
}

#[derive(Debug, Default, Clone)]
#[allow(dead_code)]
pub struct ReasonAgg {
    pub count: u64,
    pub sum_pnl: f64,
    pub worst_pnl: f64,
}

#[allow(dead_code)]
pub fn compute_reason_agg(
    shadow_log_path: &Path,
    run_id: &str,
) -> anyhow::Result<BTreeMap<String, ReasonAgg>> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(shadow_log_path)
        .with_context(|| format!("open {}", shadow_log_path.display()))?;

    let header = rdr
        .headers()
        .with_context(|| format!("read header {}", shadow_log_path.display()))?
        .clone();

    let run_id_idx = find_col(&header, "run_id").context("missing column: run_id")?;
    let total_pnl_idx = find_col(&header, "total_pnl").context("missing column: total_pnl")?;
    let notes_idx = find_col(&header, "notes").context("missing column: notes")?;

    let mut out: BTreeMap<String, ReasonAgg> = BTreeMap::new();

    for record in rdr.records() {
        let record = match record {
            Ok(r) => r,
            Err(_) => continue,
        };

        let row_run = record.get(run_id_idx).unwrap_or("").trim();
        if row_run != run_id {
            continue;
        }

        let pnl = match record.get(total_pnl_idx).and_then(parse_f64) {
            Some(v) => v,
            None => continue,
        };

        let notes = record.get(notes_idx).unwrap_or("");
        let reasons = parse_notes_reasons(notes);
        for r in reasons {
            let e = out.entry(r).or_insert_with(|| ReasonAgg {
                count: 0,
                sum_pnl: 0.0,
                worst_pnl: 0.0,
            });
            e.count += 1;
            e.sum_pnl += pnl;
            if e.count == 1 || pnl < e.worst_pnl {
                e.worst_pnl = pnl;
            }
        }
    }

    Ok(out)
}

#[allow(dead_code)]
fn find_col(header: &csv::StringRecord, name: &str) -> Option<usize> {
    header
        .iter()
        .position(|h| h.trim().eq_ignore_ascii_case(name))
}

#[allow(dead_code)]
fn parse_f64(s: &str) -> Option<f64> {
    let v = s.trim().parse::<f64>().ok()?;
    if v.is_finite() {
        Some(v)
    } else {
        None
    }
}
