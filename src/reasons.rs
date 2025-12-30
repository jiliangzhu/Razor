use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use anyhow::Context as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShadowNoteReason {
    NoTrades,
    WindowEmpty,
    WindowDataGap,
    MissingBid,
    MissingBook,
    BucketThinNan,
    BucketLiquidNan,
    DepthUnitSuspect,
    FillShareP25Zero,
    DedupHit,
    SignalTooOld,
    LegsMismatch,
    InternalError,
    InvalidPrice,
    InvalidQty,
}

impl ShadowNoteReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            ShadowNoteReason::NoTrades => "NO_TRADES",
            ShadowNoteReason::WindowEmpty => "WINDOW_EMPTY",
            ShadowNoteReason::WindowDataGap => "WINDOW_DATA_GAP",
            ShadowNoteReason::MissingBid => "MISSING_BID",
            ShadowNoteReason::MissingBook => "MISSING_BOOK",
            ShadowNoteReason::BucketThinNan => "BUCKET_THIN_NAN",
            ShadowNoteReason::BucketLiquidNan => "BUCKET_LIQUID_NAN",
            ShadowNoteReason::DepthUnitSuspect => "DEPTH_UNIT_SUSPECT",
            ShadowNoteReason::FillShareP25Zero => "FILL_SHARE_P25_ZERO",
            ShadowNoteReason::DedupHit => "DEDUP_HIT",
            ShadowNoteReason::SignalTooOld => "SIGNAL_TOO_OLD",
            ShadowNoteReason::LegsMismatch => "LEGS_MISMATCH",
            ShadowNoteReason::InternalError => "INTERNAL_ERROR",
            ShadowNoteReason::InvalidPrice => "INVALID_PRICE",
            ShadowNoteReason::InvalidQty => "INVALID_QTY",
        }
    }
}

impl fmt::Display for ShadowNoteReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn format_notes(reasons: &[ShadowNoteReason]) -> String {
    let mut uniq: BTreeSet<&'static str> = BTreeSet::new();
    for r in reasons {
        uniq.insert(r.as_str());
    }

    uniq.into_iter().collect::<Vec<_>>().join(",")
}

#[allow(dead_code)]
pub fn parse_notes_reasons(notes: &str) -> Vec<String> {
    notes
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
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
