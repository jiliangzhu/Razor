use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use anyhow::Context as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reason {
    Ok,
    NoTrades,
    MissingBid,
    BucketThinNan,
    BucketLiquidNan,
    DedupHit,
    WindowEmpty,
    InvalidSignal,
    RoundGateBlocked,
    WindowDataGap,
    MissingBook,
    DepthUnitSuspect,
    FillShareP25Zero,
    SignalTooOld,
    LegsMismatch,
    InternalError,
    InvalidPrice,
    InvalidQty,
}

impl Reason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Reason::Ok => "OK",
            Reason::NoTrades => "NO_TRADES",
            Reason::MissingBid => "MISSING_BID",
            Reason::BucketThinNan => "BUCKET_THIN_NAN",
            Reason::BucketLiquidNan => "BUCKET_LIQUID_NAN",
            Reason::DedupHit => "DEDUP_HIT",
            Reason::WindowEmpty => "WINDOW_EMPTY",
            Reason::InvalidSignal => "INVALID_SIGNAL",
            Reason::RoundGateBlocked => "ROUND_GATE_BLOCKED",
            Reason::WindowDataGap => "WINDOW_DATA_GAP",
            Reason::MissingBook => "MISSING_BOOK",
            Reason::DepthUnitSuspect => "DEPTH_UNIT_SUSPECT",
            Reason::FillShareP25Zero => "FILL_SHARE_P25_ZERO",
            Reason::SignalTooOld => "SIGNAL_TOO_OLD",
            Reason::LegsMismatch => "LEGS_MISMATCH",
            Reason::InternalError => "INTERNAL_ERROR",
            Reason::InvalidPrice => "INVALID_PRICE",
            Reason::InvalidQty => "INVALID_QTY",
        }
    }
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn format_notes(reason: Reason, cycle_id: &str) -> String {
    format!("reason={},cycle_id={}", reason.as_str(), cycle_id)
}

pub fn parse_notes_reasons(notes: &str) -> Vec<String> {
    let mut out = Vec::new();
    for part in notes.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((k, v)) = part.split_once('=') {
            if k.trim().eq_ignore_ascii_case("reason") {
                let v = v.trim();
                if !v.is_empty() {
                    out.push(v.to_string());
                }
            }
        } else {
            out.push(part.to_string());
        }
    }
    out
}

#[allow(dead_code)]
pub fn extract_note_value(notes: &str, key: &str) -> Option<String> {
    let key = key.trim();
    if key.is_empty() {
        return None;
    }
    for part in notes.split(',') {
        let part = part.trim();
        if let Some((k, v)) = part.split_once('=') {
            if k.trim().eq_ignore_ascii_case(key) {
                let v = v.trim();
                if !v.is_empty() {
                    return Some(v.to_string());
                }
            }
        }
    }
    None
}

#[allow(dead_code)]
pub fn format_reason_list(reasons: &[Reason]) -> String {
    let mut uniq: BTreeSet<&'static str> = BTreeSet::new();
    for r in reasons {
        uniq.insert(r.as_str());
    }

    uniq.into_iter().collect::<Vec<_>>().join(",")
}

#[allow(dead_code)]
pub fn parse_notes_reasons_legacy(notes: &str) -> Vec<String> {
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
