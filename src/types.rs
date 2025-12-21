use std::fmt;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bps(i32);

impl Bps {
    #[allow(dead_code)]
    pub const ZERO: Bps = Bps(0);
    pub const ONE_HUNDRED_PERCENT: Bps = Bps(10_000);
    pub const FEE_POLY: Bps = Bps(200);
    pub const FEE_MERGE: Bps = Bps(10);

    pub const fn new(raw: i32) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> i32 {
        self.0
    }

    pub fn apply_cost(self, amount: f64) -> f64 {
        let scalar = 1.0 + (self.0 as f64) / (Self::ONE_HUNDRED_PERCENT.0 as f64);
        amount * scalar
    }

    pub fn apply_proceeds(self, amount: f64) -> f64 {
        let scalar = 1.0 - (self.0 as f64) / (Self::ONE_HUNDRED_PERCENT.0 as f64);
        amount * scalar
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
        Bps(self.0 + rhs.0)
    }
}

impl Sub for Bps {
    type Output = Bps;

    fn sub(self, rhs: Bps) -> Self::Output {
        Bps(self.0 - rhs.0)
    }
}

impl AddAssign for Bps {
    fn add_assign(&mut self, rhs: Bps) {
        self.0 += rhs.0;
    }
}

impl SubAssign for Bps {
    fn sub_assign(&mut self, rhs: Bps) {
        self.0 -= rhs.0;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bucket {
    Liquid,
    Thin,
}

impl Bucket {
    pub fn as_str(self) -> &'static str {
        match self {
            Bucket::Liquid => "Liquid",
            Bucket::Thin => "Thin",
        }
    }
}

#[derive(Clone, Debug)]
pub struct LegSnapshot {
    pub token_id: String,
    pub best_ask: f64,
    pub best_bid: f64,
    pub ask_depth3_usdc: f64,
    #[allow(dead_code)]
    pub ts_recv_us: u64,
}

#[derive(Clone, Debug)]
pub struct MarketSnapshot {
    pub market_id: String,
    pub legs: Vec<LegSnapshot>,
}

#[derive(Clone, Debug)]
pub struct SignalLeg {
    pub token_id: String,
    pub p_limit: f64,
    pub best_bid_at_t0: f64,
}

#[derive(Clone, Debug)]
pub struct Signal {
    pub signal_id: u64,
    pub ts_signal_us: u64,
    pub market_id: String,
    pub strategy: Strategy,
    pub bucket: Bucket,
    pub q_req: f64,
    pub expected_net_bps: Bps,
    pub legs: Vec<SignalLeg>,
}

#[derive(Clone, Debug)]
pub struct TradeTick {
    pub ts_recv_us: u64,
    pub market_id: String,
    pub token_id: String,
    pub price: f64,
    pub size: f64,
}

#[derive(Clone, Debug)]
pub struct MarketDef {
    pub market_id: String,
    pub token_ids: Vec<String>,
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

#[cfg(test)]
mod tests {
    use assert_approx_eq::assert_approx_eq;

    use super::Bps;

    #[test]
    fn bps_apply_cost_and_proceeds() {
        let fee = Bps::new(200); // 2%
        assert_approx_eq!(fee.apply_cost(1.0), 1.02);
        assert_approx_eq!(fee.apply_proceeds(1.0), 0.98);
    }

    #[test]
    fn bps_add_sub() {
        let a = Bps::new(10);
        let b = Bps::new(5);
        assert_eq!((a + b).raw(), 15);
        assert_eq!((a - b).raw(), 5);
    }
}
