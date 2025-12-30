use crate::config::BucketConfig;
use crate::reasons::ShadowNoteReason;
use crate::types::{Bps, Bucket, BucketMetrics, MarketSnapshot};

const INVALID_SPREAD_BPS: Bps = Bps(i32::MAX);
const MAX_DEPTH3_USDC: f64 = 10_000_000.0;

pub fn fill_share_p25(bucket: Bucket, cfg: &BucketConfig) -> f64 {
    match bucket {
        Bucket::Liquid => cfg.fill_share_liquid_p25,
        Bucket::Thin => cfg.fill_share_thin_p25,
    }
}

#[derive(Debug, Clone)]
pub struct BucketDecision {
    pub bucket: Bucket,
    pub worst_leg_token_id: String,
    pub metrics: BucketMetrics,
    pub reasons: Vec<ShadowNoteReason>,
}

pub fn classify_bucket(snapshot: &MarketSnapshot) -> BucketDecision {
    if snapshot.legs.is_empty() {
        return BucketDecision {
            bucket: Bucket::Thin,
            worst_leg_token_id: String::new(),
            metrics: BucketMetrics {
                worst_leg_index: 0,
                worst_spread_bps: i32::MAX,
                worst_depth3_usdc: f64::NAN,
                is_depth3_degraded: true,
            },
            reasons: vec![ShadowNoteReason::BucketThinNan],
        };
    }

    let mut is_depth3_degraded = false;
    let mut depth_unit_suspect = false;
    let mut worst_leg_index = 0usize;
    let mut worst_depth = f64::INFINITY;

    for (idx, leg) in snapshot.legs.iter().enumerate() {
        let d = depth_sanitize(leg.ask_depth3_usdc);
        if !leg.ask_depth3_usdc.is_finite()
            || leg.ask_depth3_usdc <= 0.0
            || leg.ask_depth3_usdc > MAX_DEPTH3_USDC
        {
            is_depth3_degraded = true;
            if leg.ask_depth3_usdc.is_finite() && leg.ask_depth3_usdc > MAX_DEPTH3_USDC {
                depth_unit_suspect = true;
            }
        }
        if d < worst_depth {
            worst_depth = d;
            worst_leg_index = idx;
        }
    }

    let worst = &snapshot.legs[worst_leg_index];
    let spread = spread_bps(worst.best_bid, worst.best_ask).raw();
    let worst_depth3 = if is_depth3_degraded {
        f64::NAN
    } else {
        worst_depth
    };

    let bucket = if !is_depth3_degraded && spread < 20 && worst_depth3 > 500.0 {
        Bucket::Liquid
    } else {
        Bucket::Thin
    };

    let mut reasons: Vec<ShadowNoteReason> = Vec::new();
    if depth_unit_suspect {
        reasons.push(ShadowNoteReason::DepthUnitSuspect);
    }
    if bucket == Bucket::Thin && (is_depth3_degraded || spread == INVALID_SPREAD_BPS.raw()) {
        reasons.push(ShadowNoteReason::BucketThinNan);
    }

    BucketDecision {
        bucket,
        worst_leg_token_id: if is_depth3_degraded || spread == INVALID_SPREAD_BPS.raw() {
            String::new()
        } else {
            worst.token_id.clone()
        },
        metrics: BucketMetrics {
            worst_leg_index,
            worst_spread_bps: spread,
            worst_depth3_usdc: worst_depth3,
            is_depth3_degraded,
        },
        reasons,
    }
}

fn depth_sanitize(depth3_usdc: f64) -> f64 {
    if !depth3_usdc.is_finite() || !(0.0..=MAX_DEPTH3_USDC).contains(&depth3_usdc) {
        f64::INFINITY
    } else {
        depth3_usdc
    }
}

fn spread_bps(best_bid: f64, best_ask: f64) -> Bps {
    if !best_bid.is_finite() || !best_ask.is_finite() {
        return INVALID_SPREAD_BPS;
    }
    if best_bid <= 0.0 || best_ask <= 0.0 {
        return INVALID_SPREAD_BPS;
    }
    if best_ask < best_bid {
        return INVALID_SPREAD_BPS;
    }
    let mid = (best_ask + best_bid) / 2.0;
    if !mid.is_finite() || mid <= 0.0 {
        return INVALID_SPREAD_BPS;
    }
    let ratio = (best_ask - best_bid) / mid;
    if !ratio.is_finite() || ratio < 0.0 {
        return INVALID_SPREAD_BPS;
    }
    Bps::from_cost_ratio(ratio)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LegSnapshot, MarketSnapshot};

    #[test]
    fn bucket_thin_when_worst_depth_is_low() {
        let snap = MarketSnapshot {
            market_id: "m".to_string(),
            legs: vec![
                LegSnapshot {
                    token_id: "a".to_string(),
                    best_bid: 0.4991,
                    best_ask: 0.5,
                    best_ask_size_best: 0.0,
                    best_bid_size_best: 0.0,
                    ask_depth3_usdc: 400.0,
                    ts_recv_us: 0,
                },
                LegSnapshot {
                    token_id: "b".to_string(),
                    best_bid: 0.4995,
                    best_ask: 0.5,
                    best_ask_size_best: 0.0,
                    best_bid_size_best: 0.0,
                    ask_depth3_usdc: 10_000.0,
                    ts_recv_us: 0,
                },
            ],
        };
        let d = classify_bucket(&snap);
        assert_eq!(d.bucket, Bucket::Thin);
        assert_eq!(d.metrics.worst_leg_index, 0);
    }

    #[test]
    fn bucket_liquid_when_worst_leg_is_tight_and_deep() {
        let snap = MarketSnapshot {
            market_id: "m".to_string(),
            legs: vec![
                // worst depth = 600 (>500), spread ~= 18.0 bps (<20)
                LegSnapshot {
                    token_id: "a".to_string(),
                    best_bid: 0.4991,
                    best_ask: 0.5,
                    best_ask_size_best: 0.0,
                    best_bid_size_best: 0.0,
                    ask_depth3_usdc: 600.0,
                    ts_recv_us: 0,
                },
                LegSnapshot {
                    token_id: "b".to_string(),
                    best_bid: 0.4995,
                    best_ask: 0.5,
                    best_ask_size_best: 0.0,
                    best_bid_size_best: 0.0,
                    ask_depth3_usdc: 10_000.0,
                    ts_recv_us: 0,
                },
            ],
        };
        let d = classify_bucket(&snap);
        assert_eq!(d.bucket, Bucket::Liquid);
        assert_eq!(d.metrics.worst_leg_index, 0);
    }
}
