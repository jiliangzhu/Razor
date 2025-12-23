use crate::config::BucketConfig;
use crate::types::{Bps, Bucket, MarketSnapshot};

const INVALID_SPREAD_BPS: Bps = Bps(i32::MAX);

pub fn fill_share_p25(bucket: Bucket, cfg: &BucketConfig) -> f64 {
    match bucket {
        Bucket::Liquid => cfg.fill_share_liquid_p25,
        Bucket::Thin => cfg.fill_share_thin_p25,
    }
}

pub fn bucket_for_snapshot(snapshot: &MarketSnapshot) -> Bucket {
    let Some((worst, worst_depth)) = worst_leg(snapshot) else {
        return Bucket::Thin;
    };

    let spread_bps = spread_bps(worst.best_bid, worst.best_ask);

    if spread_bps < Bps::new(20) && worst_depth > 500.0 {
        Bucket::Liquid
    } else {
        Bucket::Thin
    }
}

pub fn worst_leg(snapshot: &MarketSnapshot) -> Option<(&crate::types::LegSnapshot, f64)> {
    let mut worst = snapshot.legs.first()?;
    let mut worst_depth = depth_sanitize(worst.ask_depth3_usdc);
    for leg in &snapshot.legs[1..] {
        let d = depth_sanitize(leg.ask_depth3_usdc);
        if d < worst_depth {
            worst_depth = d;
            worst = leg;
        }
    }
    Some((worst, worst_depth))
}

fn depth_sanitize(depth3_usdc: f64) -> f64 {
    if !depth3_usdc.is_finite() || depth3_usdc < 0.0 {
        0.0
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
                    ask_depth3_usdc: 400.0,
                    ts_recv_us: 0,
                },
                LegSnapshot {
                    token_id: "b".to_string(),
                    best_bid: 0.4995,
                    best_ask: 0.5,
                    ask_depth3_usdc: 10_000.0,
                    ts_recv_us: 0,
                },
            ],
        };
        assert_eq!(bucket_for_snapshot(&snap), Bucket::Thin);
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
                    ask_depth3_usdc: 600.0,
                    ts_recv_us: 0,
                },
                LegSnapshot {
                    token_id: "b".to_string(),
                    best_bid: 0.4995,
                    best_ask: 0.5,
                    ask_depth3_usdc: 10_000.0,
                    ts_recv_us: 0,
                },
            ],
        };
        assert_eq!(bucket_for_snapshot(&snap), Bucket::Liquid);
    }
}
