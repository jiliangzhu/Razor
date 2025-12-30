use anyhow::Context as _;
use serde_json::Value;
use std::time::Duration;

use crate::config::Config;
use crate::market_select::metrics::ProbePhase;

#[derive(Clone, Debug)]
pub struct GammaMarket {
    pub gamma_id: String,
    pub condition_id: String,
    pub token_ids: Vec<String>,
    pub strategy: String, // "binary" | "triangle"
    pub volume24hr: f64,
    pub liquidity: f64,
    pub end_date_rfc3339: Option<String>,
    #[allow(dead_code)]
    pub question: Option<String>,
    pub market_phase: ProbePhase,
}

pub async fn fetch_candidate_pool(cfg: &Config, limit: usize) -> anyhow::Result<Vec<GammaMarket>> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("razor/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_millis(
            cfg.polymarket.http_connect_timeout_ms,
        ))
        .timeout(Duration::from_millis(cfg.polymarket.http_timeout_ms))
        .build()
        .context("build http client")?;

    let url = format!(
        "{}/markets",
        cfg.polymarket.gamma_base.trim_end_matches('/')
    );

    let resp = client
        .get(url)
        .query(&[
            ("active", "true"),
            ("closed", "false"),
            ("limit", &limit.to_string()),
        ])
        .send()
        .await
        .context("gamma markets request")?;

    let list: Vec<Value> = resp.json().await.context("decode gamma response")?;

    let mut out: Vec<GammaMarket> = Vec::new();
    for v in list {
        let Some(gamma_id) = get_str(&v, "id") else {
            continue;
        };
        let Some(condition_id) = get_str(&v, "conditionId") else {
            continue;
        };
        if condition_id.trim().is_empty() {
            continue;
        }

        let Some(clob_token_ids) = get_str(&v, "clobTokenIds") else {
            continue;
        };
        let token_ids: Vec<String> = match serde_json::from_str(&clob_token_ids) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let legs_n = token_ids.len();
        if legs_n != 2 && legs_n != 3 {
            continue;
        }

        let strategy = if legs_n == 2 { "binary" } else { "triangle" }.to_string();

        let volume24hr = get_f64(&v, "volume24hr").unwrap_or(0.0);
        let liquidity = get_f64(&v, "liquidityNum")
            .or_else(|| get_f64(&v, "liquidity"))
            .unwrap_or(0.0);
        let end_date = get_str(&v, "endDate");
        let question = get_str(&v, "question");

        let market_phase = end_date
            .as_deref()
            .and_then(parse_market_phase)
            .unwrap_or(ProbePhase::Unknown);

        out.push(GammaMarket {
            gamma_id,
            condition_id,
            token_ids,
            strategy,
            volume24hr,
            liquidity,
            end_date_rfc3339: end_date,
            question,
            market_phase,
        });
    }

    // Deterministic candidate ordering: by volume24hr desc, then gamma_id asc.
    out.sort_by(|a, b| {
        crate::market_select::metrics::cmp_f64_desc(a.volume24hr, b.volume24hr)
            .then_with(|| a.gamma_id.cmp(&b.gamma_id))
    });

    Ok(out)
}

fn get_str(v: &Value, key: &str) -> Option<String> {
    let obj = v.as_object()?;
    let val = obj.get(key)?;
    if let Some(s) = val.as_str() {
        return Some(s.to_string());
    }
    if val.is_number() {
        return Some(val.to_string());
    }
    None
}

fn get_f64(v: &Value, key: &str) -> Option<f64> {
    let obj = v.as_object()?;
    let val = obj.get(key)?;
    if let Some(n) = val.as_f64() {
        return Some(n);
    }
    if let Some(s) = val.as_str() {
        return s.parse::<f64>().ok();
    }
    None
}

fn parse_market_phase(end_date_rfc3339: &str) -> Option<ProbePhase> {
    let end_secs = parse_rfc3339_to_unix_secs(end_date_rfc3339)?;
    let now_secs = crate::types::now_ms() / 1000;
    if end_secs <= now_secs {
        return Some(ProbePhase::Unknown);
    }
    let diff = end_secs - now_secs;
    if diff >= 7 * 86_400 {
        Some(ProbePhase::Gt7d)
    } else if diff >= 86_400 {
        Some(ProbePhase::D1ToD7)
    } else {
        Some(ProbePhase::Lt24h)
    }
}

fn parse_rfc3339_to_unix_secs(s: &str) -> Option<u64> {
    // Accept formats like:
    // - "2026-02-28T12:00:00Z"
    // - "2025-01-08T01:33:54.924Z"
    let (date, time) = s.split_once('T')?;
    let (y, m, d) = parse_ymd(date)?;

    let time = time.strip_suffix('Z')?;
    let (hms, _frac) = time.split_once('.').unwrap_or((time, ""));
    let mut it = hms.split(':');
    let hh = it.next()?.parse::<u32>().ok()?;
    let mm = it.next()?.parse::<u32>().ok()?;
    let ss = it.next()?.parse::<u32>().ok()?;

    let days = days_from_civil(y, m, d)?;
    let secs = days
        .checked_mul(86_400)?
        .checked_add((hh as u64).checked_mul(3600)?)?
        .checked_add((mm as u64).checked_mul(60)?)?
        .checked_add(ss as u64)?;
    Some(secs)
}

fn parse_ymd(s: &str) -> Option<(i32, u32, u32)> {
    let mut it = s.split('-');
    let y = it.next()?.parse::<i32>().ok()?;
    let m = it.next()?.parse::<u32>().ok()?;
    let d = it.next()?.parse::<u32>().ok()?;
    Some((y, m, d))
}

// Inverse of civil_from_days (Howard Hinnant's algorithm).
// Returns days since 1970-01-01.
fn days_from_civil(year: i32, month: u32, day: u32) -> Option<u64> {
    if month == 0 || month > 12 || day == 0 || day > 31 {
        return None;
    }
    let y = i64::from(year) - i64::from(month <= 2);
    let m = i64::from(month) + if month <= 2 { 9 } else { -3 };
    let d = i64::from(day);
    let era = if y >= 0 { y } else { y - 399 }.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * m + 2).div_euclid(5) + d - 1;
    let doe = yoe * 365 + yoe.div_euclid(4) - yoe.div_euclid(100) + doy;
    let days = era * 146_097 + doe - 719_468;
    if days < 0 {
        None
    } else {
        Some(days as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_end_date_with_fractional_seconds() {
        let secs = parse_rfc3339_to_unix_secs("2025-01-08T01:33:54.924Z").unwrap();
        let secs2 = parse_rfc3339_to_unix_secs("2025-01-08T01:33:54Z").unwrap();
        assert_eq!(secs, secs2);
    }
}
