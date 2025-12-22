use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write as _};
use std::path::Path;

use anyhow::Context as _;

pub const TRADES_HEADER: [&str; 5] = ["ts_ms", "market_id", "token_id", "price", "size"];

pub const TICKS_HEADER: [&str; 6] = [
    "ts_recv_us",
    "market_id",
    "token_id",
    "best_bid",
    "best_ask",
    "ask_depth3_usdc",
];

pub const SHADOW_HEADER: [&str; 35] = [
    "ts_signal_us",
    "signal_id",
    "market_id",
    "strategy",
    "bucket",
    "q_req",
    "token1",
    "p1",
    "v_mkt1",
    "q_fill1",
    "best_bid1",
    "exit1",
    "token2",
    "p2",
    "v_mkt2",
    "q_fill2",
    "best_bid2",
    "exit2",
    "token3",
    "p3",
    "v_mkt3",
    "q_fill3",
    "best_bid3",
    "exit3",
    "q_set",
    "q_left1",
    "q_left2",
    "q_left3",
    "set_ratio",
    "pnl_set",
    "pnl_left",
    "pnl_total",
    "fill_share_used",
    "risk_premium_bps",
    "expected_net_bps",
];

pub struct CsvAppender {
    writer: csv::Writer<BufWriter<File>>,
}

impl CsvAppender {
    pub fn open(path: impl AsRef<Path>, header: &[&str]) -> anyhow::Result<Self> {
        let path = path.as_ref();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open {}", path.display()))?;

        let is_empty = file
            .metadata()
            .with_context(|| format!("stat {}", path.display()))?
            .len()
            == 0;

        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(BufWriter::new(file));

        if is_empty {
            writer
                .write_record(header)
                .with_context(|| format!("write header {}", path.display()))?;
            writer
                .flush()
                .with_context(|| format!("flush {}", path.display()))?;
        }

        Ok(Self { writer })
    }

    pub fn write_record<I, S>(&mut self, record: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        self.writer.write_record(record)?;
        self.writer.flush()?;
        Ok(())
    }
}

pub struct JsonlAppender {
    out: BufWriter<File>,
}

impl JsonlAppender {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open {}", path.display()))?;
        Ok(Self {
            out: BufWriter::new(file),
        })
    }

    pub fn write_line(&mut self, line: &str) -> anyhow::Result<()> {
        self.out.write_all(line.as_bytes())?;
        self.out.write_all(b"\n")?;
        self.out.flush()?;
        Ok(())
    }
}
