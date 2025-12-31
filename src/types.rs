//! Unit system (frozen):
//! - All **fees / edges / thresholds / budgets** are expressed in **basis points** (`Bps`).
//! - Only at the final step (multiplying a fee into a `price`) do we convert to `f64`.
//! - Converting ratios to `Bps` must be **directional**:
//!   - For **cost / gating** (avoid false-positive edge): use `from_cost_ratio` (ceil).
//!   - For **proceeds / display**: use `from_proceeds_ratio` (floor).
//! - Do **not** introduce float fee constants like `0.02` outside this module.

use std::fmt;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::reasons::Reason;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bps(pub i32);

impl Bps {
    #[allow(dead_code)]
    pub const ZERO: Bps = Bps(0);
    pub const ONE_HUNDRED_PERCENT: Bps = Bps(10_000);
    pub const FEE_POLY: Bps = Bps(200);
    pub const FEE_MERGE: Bps = Bps(10);
    pub const BASIS: f64 = 10_000.0;
    const FROM_RATIO_EPS: f64 = 1e-9;

    pub const fn new(raw: i32) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> i32 {
        self.0
    }

    pub fn to_f64(self) -> f64 {
        (self.0 as f64) / Self::BASIS
    }

    /// Convert a ratio-like value into basis points using **floor** (e.g. `0.985 -> 9850`).
    ///
    /// Rounding strategy: **floor** after scaling by `BASIS`.
    /// We add a tiny epsilon to avoid cases like `0.1 * 10000` becoming `999.999...`.
    #[allow(dead_code)]
    pub fn from_ratio_floor(x: f64) -> Bps {
        assert!(x.is_finite(), "ratio must be finite");
        assert!(x >= 0.0, "ratio must be >= 0");

        let scaled = (x * Self::BASIS) + Self::FROM_RATIO_EPS;
        let raw = scaled.floor() as i64;
        assert!(
            raw >= i32::MIN as i64 && raw <= i32::MAX as i64,
            "Bps::from_ratio_floor overflow"
        );
        Bps(raw as i32)
    }

    /// Convert a ratio-like value into basis points using **ceil** (e.g. `0.98509 -> 9851`).
    ///
    /// This is the correct direction for **cost / gating**: it avoids systematically
    /// under-estimating costs which would inflate computed edge.
    ///
    /// We subtract a tiny epsilon to avoid cases like `1.0 * 10000` becoming `10000.0000...2`
    /// and rounding up to 10001.
    #[allow(dead_code)]
    pub fn from_ratio_ceil(x: f64) -> Bps {
        assert!(x.is_finite(), "ratio must be finite");
        assert!(x >= 0.0, "ratio must be >= 0");

        let scaled = (x * Self::BASIS) - Self::FROM_RATIO_EPS;
        let raw = scaled.ceil() as i64;
        assert!(
            raw >= i32::MIN as i64 && raw <= i32::MAX as i64,
            "Bps::from_ratio_ceil overflow"
        );
        Bps(raw as i32)
    }

    /// Alias for **cost / gating** conversion (ceil).
    #[allow(dead_code)]
    pub fn from_cost_ratio(x: f64) -> Bps {
        Self::from_ratio_ceil(x)
    }

    /// Alias for **proceeds / display** conversion (floor).
    #[allow(dead_code)]
    pub fn from_proceeds_ratio(x: f64) -> Bps {
        Self::from_ratio_floor(x)
    }

    /// Convert a probability-style price `p` in `[0, 1]` into basis points.
    ///
    /// Use this when you want strict probability-domain validation.
    #[allow(dead_code)]
    pub fn from_prob(p: f64) -> Bps {
        assert!(p.is_finite(), "price must be finite");
        assert!(p >= 0.0, "price must be >= 0");
        assert!(p <= 1.0, "price must be <= 1");
        Self::from_proceeds_ratio(p)
    }

    /// Convert a price/ratio to basis points.
    ///
    /// Note: this function intentionally allows values > 1.0 (e.g. multi-leg sums like
    /// `sum(best_ask_i)`), but it uses **floor** and therefore must not be used for
    /// cost/gating. Use `from_cost_ratio` for that.
    #[allow(dead_code)]
    pub fn from_price(p: f64) -> Bps {
        Self::from_proceeds_ratio(p)
    }

    /// Convert a price/ratio to basis points for **cost / gating** (ceil).
    ///
    /// This is the safe default when the result will be subtracted from 10_000 to compute edge.
    #[allow(dead_code)]
    pub fn from_price_cost(p: f64) -> Bps {
        if !p.is_finite() || p < 0.0 {
            return Bps::ONE_HUNDRED_PERCENT;
        }
        Self::from_cost_ratio(p)
    }

    /// Convert a price/ratio to basis points for **proceeds / display** (floor).
    #[allow(dead_code)]
    pub fn from_price_proceeds(p: f64) -> Bps {
        if !p.is_finite() || p < 0.0 {
            return Bps::ZERO;
        }
        Self::from_proceeds_ratio(p)
    }

    pub fn apply_cost(self, price: f64) -> f64 {
        price * (1.0 + self.to_f64())
    }

    pub fn apply_proceeds(self, price: f64) -> f64 {
        price * (1.0 - self.to_f64())
    }

    #[allow(dead_code)]
    pub fn clamp(self, min: Bps, max: Bps) -> Bps {
        Bps(self.0.clamp(min.0, max.0))
    }
}

impl fmt::Display for Bps {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}bps", self.0)
    }
}

impl Add for Bps {
    type Output = Bps;

    fn add(self, rhs: Bps) -> Self::Output {
        Bps(self.0.checked_add(rhs.0).expect("Bps add overflow"))
    }
}

impl Sub for Bps {
    type Output = Bps;

    fn sub(self, rhs: Bps) -> Self::Output {
        Bps(self.0.checked_sub(rhs.0).expect("Bps sub overflow"))
    }
}

impl AddAssign for Bps {
    fn add_assign(&mut self, rhs: Bps) {
        self.0 = self.0.checked_add(rhs.0).expect("Bps add_assign overflow");
    }
}

impl SubAssign for Bps {
    fn sub_assign(&mut self, rhs: Bps) {
        self.0 = self.0.checked_sub(rhs.0).expect("Bps sub_assign overflow");
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Strategy {
    Binary,
    Triangle,
}

impl Strategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Strategy::Binary => "binary",
            Strategy::Triangle => "triangle",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LiquidityBucket {
    Liquid,
    Thin,
}

impl LiquidityBucket {
    pub fn as_str(self) -> &'static str {
        match self {
            LiquidityBucket::Liquid => "Liquid",
            LiquidityBucket::Thin => "Thin",
        }
    }
}

pub type Bucket = LiquidityBucket;

#[derive(Clone, Debug)]
pub struct LegSnapshot {
    pub token_id: String,
    pub best_ask: f64,
    #[allow(dead_code)]
    pub best_ask_size_best: f64,
    pub best_bid: f64,
    #[allow(dead_code)]
    pub best_bid_size_best: f64,
    pub ask_depth3_usdc: f64,
    #[allow(dead_code)]
    pub ts_recv_us: u64,
}

#[derive(Clone, Debug)]
pub struct MarketSnapshot {
    pub market_id: String,
    pub legs: Vec<LegSnapshot>,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    #[allow(dead_code)]
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }
}

#[derive(Clone, Debug)]
pub struct SignalLeg {
    pub leg_index: usize,
    pub token_id: String,
    #[allow(dead_code)]
    pub side: Side,
    pub limit_price: f64,
    pub qty: f64,
    pub best_bid_at_signal: f64,
    #[allow(dead_code)]
    pub best_ask_at_signal: f64,
}

pub type Leg = SignalLeg;

#[derive(Clone, Debug)]
pub struct Signal {
    pub run_id: String,
    pub signal_id: u64,
    pub signal_ts_ms: u64,
    pub market_id: String,
    pub cycle_id: String,
    pub market_slug: Option<String>,
    pub market_type: Option<String>,
    pub strategy: Strategy,
    pub bucket: Bucket,
    pub reasons: Vec<Reason>,
    pub q_req: f64,
    pub raw_cost_bps: Bps,
    pub raw_edge_bps: Bps,
    pub hard_fees_bps: Bps,
    pub risk_premium_bps: Bps,
    pub expected_net_bps: Bps,
    pub bucket_metrics: BucketMetrics,
    pub legs: Vec<SignalLeg>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillStatus {
    None,
    Partial,
    Full,
}

impl FillStatus {
    #[allow(dead_code)]
    pub fn as_str(self) -> &'static str {
        match self {
            FillStatus::None => "NONE",
            FillStatus::Partial => "PARTIAL",
            FillStatus::Full => "FULL",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct FillReport {
    pub requested_qty: f64,
    pub filled_qty: f64,
    pub avg_price: f64,
    pub status: FillStatus,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TradeTick {
    /// Phase 1 canonical timestamp (unix ms) used for shadow windows.
    ///
    /// Current policy: `ts_ms` is **local ingest time** (TS_SRC=local).
    pub ts_ms: u64,
    /// Local ingest timestamp (unix ms). Redundant with `ts_ms` when TS_SRC=local.
    #[serde(default)]
    pub ingest_ts_ms: u64,
    /// Exchange timestamp (unix ms) if available; None when missing/unknown.
    #[serde(default)]
    pub exchange_ts_ms: Option<u64>,
    pub market_id: String,
    pub token_id: String,
    pub price: f64,
    pub size: f64,
    pub trade_id: String,
}

#[derive(Clone, Debug)]
pub struct BucketMetrics {
    pub worst_leg_index: usize,
    #[allow(dead_code)]
    pub worst_spread_bps: i32,
    #[allow(dead_code)]
    pub worst_depth3_usdc: f64,
    #[allow(dead_code)]
    pub is_depth3_degraded: bool,
}

#[derive(Clone, Debug)]
pub struct MarketDef {
    pub market_id: String,
    pub token_ids: Vec<String>,
    pub market_slug: Option<String>,
    pub market_type: Option<String>,
    pub round_start_ms: Option<u64>,
}

impl MarketDef {
    pub fn strategy(&self) -> anyhow::Result<Strategy> {
        match self.token_ids.len() {
            2 => Ok(Strategy::Binary),
            3 => Ok(Strategy::Triangle),
            n => anyhow::bail!("unsupported leg count {n} (Phase 1 supports 2 or 3)"),
        }
    }
}

pub fn now_us() -> u64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    d.as_micros() as u64
}

pub fn now_ms() -> u64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    d.as_millis() as u64
}

#[cfg(test)]
mod tests {
    use assert_approx_eq::assert_approx_eq;

    use super::Bps;

    #[test]
    fn bps_apply_cost_and_proceeds() {
        let fee = Bps::FEE_POLY; // 2%
        let expected_delta = (fee.raw() as f64) / Bps::BASIS;
        assert_approx_eq!(fee.apply_cost(1.0), 1.0 + expected_delta);
        assert_approx_eq!(fee.apply_proceeds(1.0), 1.0 - expected_delta);
    }

    #[test]
    fn bps_add_sub() {
        assert_eq!((Bps::FEE_POLY + Bps::FEE_MERGE).raw(), 210);
        assert_eq!(
            (Bps::ONE_HUNDRED_PERCENT - Bps::from_price(0.985)).raw(),
            150
        );
    }

    #[test]
    fn from_price_units() {
        assert_eq!(Bps::from_price(0.1).raw(), 1000);
        assert_eq!(Bps::from_price(1.0).raw(), 10_000);
        assert_eq!(Bps::from_price(0.985).raw(), 9850);
    }

    #[test]
    fn from_ratio_allows_gt_one() {
        let sum_prices = 1.23456; // scaled=12345.6
        assert_eq!(Bps::from_proceeds_ratio(sum_prices).raw(), 12_345);
        assert_eq!(Bps::from_cost_ratio(sum_prices).raw(), 12_346);
        assert_eq!(Bps::from_price(sum_prices).raw(), 12_345);
    }

    #[test]
    #[should_panic(expected = "price must be <= 1")]
    fn from_prob_rejects_gt_one() {
        let _ = Bps::from_prob(1.0001);
    }

    #[test]
    fn edge_example_uses_bps_domain() {
        // Example: sum_prices = 0.985 => raw_cost_bps = 9850 => raw_edge_bps = 150
        let sum_prices = 0.985;
        let raw_cost_bps = Bps::from_price(sum_prices);
        let raw_edge_bps = Bps::ONE_HUNDRED_PERCENT - raw_cost_bps;
        assert_eq!(raw_edge_bps.raw(), 150);
    }

    #[test]
    fn cost_rounding_is_not_optimistic() {
        // scaled = 9850.9: floor would under-estimate cost and inflate edge by 0.9 bps.
        let sum_prices = 0.98509;
        let cost_floor = Bps::from_proceeds_ratio(sum_prices);
        let cost_ceil = Bps::from_cost_ratio(sum_prices);
        assert_eq!(cost_floor.raw(), 9850);
        assert_eq!(cost_ceil.raw(), 9851);

        let edge_floor = Bps::ONE_HUNDRED_PERCENT - cost_floor;
        let edge_ceil = Bps::ONE_HUNDRED_PERCENT - cost_ceil;
        assert_eq!(edge_floor.raw(), 150);
        assert_eq!(edge_ceil.raw(), 149);
        assert!(edge_ceil <= edge_floor);
    }

    #[test]
    fn from_price_cost_rounds_up_near_one() {
        // scaled = 9999.1 => ceil => 10000 (must not be 9999).
        assert_eq!(Bps::from_price_cost(0.99991).raw(), 10_000);
        assert_eq!(Bps::from_price_proceeds(0.99991).raw(), 9_999);
    }

    #[test]
    fn constants_are_exact() {
        assert_eq!(Bps::ZERO.raw(), 0);
        assert_eq!(Bps::ONE_HUNDRED_PERCENT.raw(), 10_000);
        assert_eq!(Bps::FEE_POLY.raw(), 200);
        assert_eq!(Bps::FEE_MERGE.raw(), 10);
        assert_approx_eq!(Bps::BASIS, 10_000.0);
    }
}
