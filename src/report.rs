use std::path::Path;

use anyhow::Context as _;
use serde::Serialize;

const VERSION: &str = "day14_report_v1";

#[derive(Clone, Debug)]
pub struct ReportCfg {
    pub data_dir: String,
    pub legging_ratio_threshold: f64,
    pub pnl_threshold: f64,
}

#[derive(Debug, Default, Serialize)]
pub struct PnlByBucket {
    #[serde(rename = "Liquid")]
    pub liquid: f64,
    #[serde(rename = "Thin")]
    pub thin: f64,
    #[serde(rename = "Unknown")]
    pub unknown: f64,
}

#[derive(Debug, Serialize)]
pub struct Day14Metrics {
    pub version: String,
    pub data_dir: String,
    pub rows_total: u64,
    pub rows_ok: u64,
    pub rows_bad: u64,

    pub total_shadow_pnl: f64,
    pub pnl_by_bucket: PnlByBucket,

    pub q_req_sum: f64,
    pub q_set_sum: f64,
    pub q_fill_avg_sum: f64,

    pub legging_ratio: f64,
    pub legging_ratio_threshold: f64,
    pub pnl_threshold: f64,

    pub decision: String,
    pub reasons: Vec<String>,
}

pub fn compute_day14_metrics(csv_path: &Path, cfg: &ReportCfg) -> anyhow::Result<Day14Metrics> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(csv_path)
        .with_context(|| format!("open {}", csv_path.display()))?;

    let header = rdr
        .headers()
        .with_context(|| format!("read header {}", csv_path.display()))?
        .clone();
    let meta = HeaderMeta::new(&header)?;

    let mut rows_total: u64 = 0;
    let mut rows_ok: u64 = 0;
    let mut rows_bad: u64 = 0;

    let mut total_shadow_pnl = 0.0f64;
    let mut pnl_by_bucket = PnlByBucket::default();

    let mut q_req_sum = 0.0f64;
    let mut q_set_sum = 0.0f64;
    let mut q_fill_avg_sum = 0.0f64;

    for record in rdr.records() {
        rows_total += 1;
        let record = match record {
            Ok(r) => r,
            Err(_) => {
                rows_bad += 1;
                continue;
            }
        };

        let Some(row) = parse_row(&record, &meta) else {
            rows_bad += 1;
            continue;
        };

        rows_ok += 1;

        total_shadow_pnl += row.pnl_total;
        match bucket_class(&row.bucket_raw) {
            BucketClass::Liquid => pnl_by_bucket.liquid += row.pnl_total,
            BucketClass::Thin => pnl_by_bucket.thin += row.pnl_total,
            BucketClass::Unknown => pnl_by_bucket.unknown += row.pnl_total,
        }

        q_req_sum += row.q_req;
        q_set_sum += row.q_set;
        q_fill_avg_sum += row.q_fill_avg;
    }

    let legging_ratio = if q_fill_avg_sum > 0.0 {
        1.0 - (q_set_sum / q_fill_avg_sum)
    } else {
        1.0
    };

    let mut metrics = Day14Metrics {
        version: VERSION.to_string(),
        data_dir: cfg.data_dir.clone(),
        rows_total,
        rows_ok,
        rows_bad,
        total_shadow_pnl,
        pnl_by_bucket,
        q_req_sum,
        q_set_sum,
        q_fill_avg_sum,
        legging_ratio,
        legging_ratio_threshold: cfg.legging_ratio_threshold,
        pnl_threshold: cfg.pnl_threshold,
        decision: String::new(),
        reasons: Vec::new(),
    };

    let (decision_str, reasons) = decision(&metrics);
    metrics.decision = decision_str;
    metrics.reasons = reasons;

    Ok(metrics)
}

pub fn decision(metrics: &Day14Metrics) -> (String, Vec<String>) {
    let pnl_pass = metrics.total_shadow_pnl > metrics.pnl_threshold;
    let legging_pass =
        metrics.q_fill_avg_sum > 0.0 && metrics.legging_ratio < metrics.legging_ratio_threshold;

    if pnl_pass && legging_pass {
        return (
            "GO".to_string(),
            vec![
                format!("TotalShadowPnL > {}", metrics.pnl_threshold),
                format!("LeggingRatio < {}", metrics.legging_ratio_threshold),
            ],
        );
    }

    let mut reasons: Vec<String> = Vec::new();
    if !pnl_pass {
        reasons.push(format!("TotalShadowPnL <= {}", metrics.pnl_threshold));
    }
    if metrics.q_fill_avg_sum == 0.0 {
        reasons.push("q_fill_avg_sum == 0".to_string());
    } else if !legging_pass {
        reasons.push(format!(
            "LeggingRatio >= {}",
            metrics.legging_ratio_threshold
        ));
    }

    ("NO_GO".to_string(), reasons)
}

#[derive(Clone, Debug)]
struct HeaderMeta {
    market_id: usize,
    bucket: usize,
    q_req: usize,
    q_set: usize,
    pnl_total: usize,

    legs: Option<usize>,
    q_fill_sum: Option<usize>,
    q_fill_cols: Vec<usize>,
    token_cols: Vec<usize>,
}

impl HeaderMeta {
    fn new(header: &csv::StringRecord) -> anyhow::Result<Self> {
        let mut market_id: Option<usize> = None;
        let mut bucket: Option<usize> = None;
        let mut q_req: Option<usize> = None;
        let mut q_set: Option<usize> = None;
        let mut pnl_total: Option<usize> = None;

        let mut legs: Option<usize> = None;
        let mut q_fill_sum: Option<usize> = None;
        let mut q_fill_cols: Vec<usize> = Vec::new();
        let mut token_cols: Vec<usize> = Vec::new();

        for (idx, name) in header.iter().enumerate() {
            let n = norm(name);
            match n.as_str() {
                "marketid" | "market" => {
                    market_id.get_or_insert(idx);
                }
                "bucket" => {
                    bucket.get_or_insert(idx);
                }
                "qreq" => {
                    q_req.get_or_insert(idx);
                }
                "qset" => {
                    q_set.get_or_insert(idx);
                }
                "pnltotal" => {
                    pnl_total.get_or_insert(idx);
                }
                "legs" | "legscount" => {
                    legs.get_or_insert(idx);
                }
                "qfillsum" => {
                    q_fill_sum.get_or_insert(idx);
                }
                _ => {}
            }

            if let Some(suffix) = n.strip_prefix("qfill") {
                if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
                    q_fill_cols.push(idx);
                }
            }
            if let Some(suffix) = n.strip_prefix("token") {
                if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
                    token_cols.push(idx);
                }
            }
        }

        let Some(market_id) = market_id else {
            anyhow::bail!("missing required column: market_id");
        };
        let Some(bucket) = bucket else {
            anyhow::bail!("missing required column: bucket");
        };
        let Some(q_req) = q_req else {
            anyhow::bail!("missing required column: q_req");
        };
        let Some(q_set) = q_set else {
            anyhow::bail!("missing required column: q_set");
        };
        let Some(pnl_total) = pnl_total else {
            anyhow::bail!("missing required column: pnl_total");
        };

        if q_fill_sum.is_none() && q_fill_cols.is_empty() {
            anyhow::bail!("missing required column(s): q_fill_sum or q_fill_*");
        }

        Ok(Self {
            market_id,
            bucket,
            q_req,
            q_set,
            pnl_total,
            legs,
            q_fill_sum,
            q_fill_cols,
            token_cols,
        })
    }
}

#[derive(Clone, Debug)]
struct ParsedRow {
    #[allow(dead_code)]
    market_id: String,
    bucket_raw: String,
    q_req: f64,
    q_set: f64,
    q_fill_avg: f64,
    pnl_total: f64,
}

fn parse_row(record: &csv::StringRecord, meta: &HeaderMeta) -> Option<ParsedRow> {
    let market_id = record.get(meta.market_id)?.trim();
    if market_id.is_empty() {
        return None;
    }
    let bucket_raw = record.get(meta.bucket)?.trim();
    if bucket_raw.is_empty() {
        return None;
    }

    let q_req = parse_f64(record.get(meta.q_req)?)?;
    let q_set = parse_f64(record.get(meta.q_set)?)?;
    let pnl_total = parse_f64(record.get(meta.pnl_total)?)?;

    let q_fill_sum = if let Some(idx) = meta.q_fill_sum {
        parse_f64(record.get(idx)?)?
    } else {
        let mut sum = 0.0f64;
        for idx in &meta.q_fill_cols {
            sum += parse_f64(record.get(*idx)?)?;
        }
        sum
    };

    let legs_count = if let Some(idx) = meta.legs {
        let n = parse_usize(record.get(idx)?)?;
        if n == 0 {
            return None;
        }
        n
    } else if !meta.token_cols.is_empty() {
        let mut n = 0usize;
        for idx in &meta.token_cols {
            if record.get(*idx).unwrap_or("").trim().is_empty() {
                continue;
            }
            n += 1;
        }
        if n > 0 {
            n
        } else if !meta.q_fill_cols.is_empty() {
            meta.q_fill_cols.len()
        } else {
            return None;
        }
    } else if !meta.q_fill_cols.is_empty() {
        meta.q_fill_cols.len()
    } else {
        return None;
    };

    let q_fill_avg = q_fill_sum / (legs_count as f64);

    Some(ParsedRow {
        market_id: market_id.to_string(),
        bucket_raw: bucket_raw.to_string(),
        q_req,
        q_set,
        q_fill_avg,
        pnl_total,
    })
}

fn norm(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

fn parse_f64(s: &str) -> Option<f64> {
    let v = s.trim().parse::<f64>().ok()?;
    if v.is_finite() {
        Some(v)
    } else {
        None
    }
}

fn parse_usize(s: &str) -> Option<usize> {
    s.trim().parse::<usize>().ok()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BucketClass {
    Liquid,
    Thin,
    Unknown,
}

fn bucket_class(s: &str) -> BucketClass {
    match s.trim().to_ascii_lowercase().as_str() {
        "liquid" => BucketClass::Liquid,
        "thin" => BucketClass::Thin,
        _ => BucketClass::Unknown,
    }
}
