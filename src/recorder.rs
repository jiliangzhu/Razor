use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use tracing::warn;

pub const TRADES_HEADER: [&str; 5] = ["ts_ms", "market_id", "token_id", "price", "size"];

pub const TICKS_HEADER: [&str; 6] = [
    "ts_recv_us",
    "market_id",
    "token_id",
    "best_bid",
    "best_ask",
    "ask_depth3_usdc",
];

pub const SHADOW_HEADER: [&str; 38] = crate::schema::SHADOW_HEADER;

pub struct CsvAppender {
    writer: csv::Writer<BufWriter<File>>,
}

impl CsvAppender {
    pub fn open(path: impl AsRef<Path>, header: &[&str]) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let expected = header.join(",");

        if let Ok(meta) = std::fs::metadata(path) {
            if meta.len() > 0 {
                let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
                let mut reader = BufReader::new(f);
                let mut first = String::new();
                reader
                    .read_line(&mut first)
                    .with_context(|| format!("read header {}", path.display()))?;

                let got = first.trim_end();
                if got != expected {
                    let backup = schema_mismatch_backup_path(path)?;
                    std::fs::rename(path, &backup).with_context(|| {
                        format!(
                            "rotate schema-mismatched csv {} -> {}",
                            path.display(),
                            backup.display()
                        )
                    })?;
                    warn!(
                        path = %path.display(),
                        backup = %backup.display(),
                        "csv schema mismatch; rotated file"
                    );
                }
            }
        }

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

fn schema_mismatch_backup_path(path: &Path) -> anyhow::Result<PathBuf> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let base_name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown.csv".to_string());
    let backup_name = format!("{base_name}.schema_mismatch_{now_ms}");
    Ok(path.with_file_name(backup_name))
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
