use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use serde::Serialize;
use tracing::warn;

use crate::types::now_ms;

pub const TRADES_HEADER: [&str; 8] = crate::schema::TRADES_HEADER;

pub const TICKS_HEADER: [&str; 6] = [
    "ts_recv_us",
    "market_id",
    "token_id",
    "best_bid",
    "best_ask",
    "ask_depth3_usdc",
];

pub const SHADOW_HEADER: [&str; 38] = crate::schema::SHADOW_HEADER;

const CSV_FLUSH_EVERY_RECORDS: usize = 200;
const CSV_FLUSH_EVERY_MS: u64 = 1_000;

pub struct CsvAppender {
    writer: csv::Writer<BufWriter<File>>,
    pending_records: usize,
    last_flush_ms: u64,
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

        Ok(Self {
            writer,
            pending_records: 0,
            last_flush_ms: now_ms(),
        })
    }

    pub fn write_record<I, S>(&mut self, record: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        self.writer.write_record(record)?;
        self.pending_records = self.pending_records.saturating_add(1);
        self.maybe_flush()?;
        Ok(())
    }

    pub fn flush_and_sync(&mut self) -> anyhow::Result<()> {
        self.writer.flush()?;
        self.pending_records = 0;
        self.last_flush_ms = now_ms();
        self.writer
            .get_ref()
            .get_ref()
            .sync_all()
            .context("sync csv file")?;
        Ok(())
    }

    fn maybe_flush(&mut self) -> anyhow::Result<()> {
        let now = now_ms();
        let due = self.pending_records >= CSV_FLUSH_EVERY_RECORDS
            || now.saturating_sub(self.last_flush_ms) >= CSV_FLUSH_EVERY_MS;
        if due {
            self.writer.flush()?;
            self.pending_records = 0;
            self.last_flush_ms = now;
        }
        Ok(())
    }
}

pub struct JsonlAppender {
    path: PathBuf,
    out: BufWriter<File>,
    pending_lines: usize,
    last_flush_ms: u64,
    rotate_max_bytes: Option<u64>,
    rotate_keep_files: Option<usize>,
}

pub struct RecorderGuard {
    run_dir: PathBuf,
}

impl RecorderGuard {
    pub fn new(run_dir: PathBuf) -> Self {
        Self { run_dir }
    }

    pub fn flush_all(&self) -> anyhow::Result<()> {
        let files = [
            crate::schema::FILE_TICKS,
            crate::schema::FILE_TRADES,
            crate::schema::FILE_SNAPSHOTS,
            crate::schema::FILE_SHADOW_LOG,
            crate::schema::FILE_RAW_WS_JSONL,
            crate::schema::FILE_HEALTH_JSONL,
            crate::schema::FILE_TRADE_LOG,
            crate::schema::FILE_CALIBRATION_LOG,
            crate::schema::FILE_CALIBRATION_SUGGEST,
            crate::schema::FILE_REPORT_JSON,
            crate::schema::FILE_REPORT_MD,
            crate::schema::FILE_SCHEMA_VERSION,
            crate::schema::FILE_RUN_CONFIG,
            crate::schema::FILE_RUN_META_JSON,
        ];

        for f in files {
            let path = self.run_dir.join(f);
            if !path.exists() {
                continue;
            }
            let file = OpenOptions::new()
                .read(true)
                .open(&path)
                .with_context(|| format!("open {}", path.display()))?;
            file.sync_all()
                .with_context(|| format!("sync {}", path.display()))?;
        }

        Ok(())
    }
}

impl Drop for RecorderGuard {
    fn drop(&mut self) {
        let _ = self.flush_all();
    }
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
        Self::open_with_rotation(path, None, None)
    }

    pub fn open_with_rotation(
        path: impl AsRef<Path>,
        rotate_max_bytes: Option<u64>,
        rotate_keep_files: Option<usize>,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open {}", path.display()))?;
        Ok(Self {
            path: path.to_path_buf(),
            out: BufWriter::new(file),
            pending_lines: 0,
            last_flush_ms: now_ms(),
            rotate_max_bytes,
            rotate_keep_files,
        })
    }

    pub fn write_line(&mut self, line: &str) -> anyhow::Result<()> {
        self.out.write_all(line.as_bytes())?;
        self.out.write_all(b"\n")?;
        self.pending_lines = self.pending_lines.saturating_add(1);
        self.maybe_flush()?;
        Ok(())
    }

    pub fn flush_and_sync(&mut self) -> anyhow::Result<()> {
        self.flush_internal()?;
        self.out.get_ref().sync_all().context("sync jsonl file")?;
        Ok(())
    }

    fn maybe_flush(&mut self) -> anyhow::Result<()> {
        // JSONL can be high-volume (raw WS); flush in batches to avoid IO becoming the bottleneck.
        const JSONL_FLUSH_EVERY_LINES: usize = 500;
        const JSONL_FLUSH_EVERY_MS: u64 = 1_000;

        let now = now_ms();
        let due = self.pending_lines >= JSONL_FLUSH_EVERY_LINES
            || now.saturating_sub(self.last_flush_ms) >= JSONL_FLUSH_EVERY_MS;
        if due {
            self.flush_internal()?;
        }
        Ok(())
    }

    fn flush_internal(&mut self) -> anyhow::Result<()> {
        self.out.flush()?;
        self.pending_lines = 0;
        self.last_flush_ms = now_ms();

        if let Some(max_bytes) = self.rotate_max_bytes {
            self.rotate_if_needed(max_bytes)?;
        }
        Ok(())
    }

    fn rotate_if_needed(&mut self, max_bytes: u64) -> anyhow::Result<()> {
        if max_bytes == 0 {
            return Ok(());
        }

        let meta = match std::fs::metadata(&self.path) {
            Ok(m) => m,
            Err(_) => return Ok(()),
        };
        if meta.len() < max_bytes {
            return Ok(());
        }

        let rotated = rotated_path(&self.path)?;
        warn!(
            path = %self.path.display(),
            rotated = %rotated.display(),
            max_bytes,
            "jsonl reached size cap; rotating"
        );

        // Ensure durability for the rotated segment (best-effort).
        self.out
            .get_ref()
            .sync_all()
            .context("sync before rotate")?;

        // Best-effort rotation: on Unix renaming an open file works; we then reopen a fresh file.
        let _ = std::fs::rename(&self.path, &rotated);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("reopen {}", self.path.display()))?;
        self.out = BufWriter::new(file);

        if let Some(keep) = self.rotate_keep_files {
            if keep > 0 {
                cleanup_rotated_files(&self.path, keep)?;
            }
        }
        Ok(())
    }
}

fn cleanup_rotated_files(path: &Path, keep: usize) -> anyhow::Result<()> {
    if keep == 0 {
        return Ok(());
    }

    let Some(dir) = path.parent() else {
        return Ok(());
    };
    let base = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    if base.is_empty() {
        return Ok(());
    }
    let prefix = format!("{base}.rotated_");

    let mut rotated: Vec<(u128, PathBuf)> = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with(&prefix) {
            continue;
        }
        let ts = name
            .strip_prefix(&prefix)
            .and_then(|s| s.parse::<u128>().ok())
            .unwrap_or(0);
        rotated.push((ts, entry.path()));
    }

    if rotated.len() <= keep {
        return Ok(());
    }
    rotated.sort_by_key(|(ts, _)| *ts);

    let remove_n = rotated.len().saturating_sub(keep);
    for (_ts, p) in rotated.into_iter().take(remove_n) {
        let _ = std::fs::remove_file(&p);
        warn!(path = %p.display(), keep, "removed old rotated jsonl segment");
    }
    Ok(())
}

fn rotated_path(path: &Path) -> anyhow::Result<PathBuf> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let base_name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown.jsonl".to_string());
    let rotated_name = format!("{base_name}.rotated_{now_ms}");
    Ok(path.with_file_name(rotated_name))
}

pub fn write_run_config_snapshot(run_dir: &Path, cfg_raw: &str) -> anyhow::Result<()> {
    let out_path = run_dir.join(crate::schema::FILE_RUN_CONFIG);
    std::fs::write(&out_path, cfg_raw.as_bytes())
        .with_context(|| format!("write {}", out_path.display()))?;
    Ok(())
}

#[derive(Debug, Serialize)]
struct RunMeta {
    run_id: String,
    start_ts_ms: u64,
    schema_version: String,
    binary_version: String,
    mode: String,
    pid: u32,
    host: String,
    os: String,
    arch: String,
    git_commit: String,
}

pub fn write_run_meta_json(
    run_dir: &Path,
    run_id: &str,
    start_ts_ms: u64,
    mode: &impl std::fmt::Display,
) -> anyhow::Result<()> {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());

    let meta = RunMeta {
        run_id: run_id.to_string(),
        start_ts_ms,
        schema_version: crate::schema::SCHEMA_VERSION.to_string(),
        binary_version: env!("CARGO_PKG_VERSION").to_string(),
        mode: mode.to_string(),
        pid: std::process::id(),
        host,
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        git_commit: read_git_commit().unwrap_or_else(|| "unknown".to_string()),
    };

    let out_path = run_dir.join(crate::schema::FILE_META_JSON);
    let json = serde_json::to_vec_pretty(&meta).context("serialize meta.json")?;
    std::fs::write(&out_path, json).with_context(|| format!("write {}", out_path.display()))?;
    Ok(())
}

fn read_git_commit() -> Option<String> {
    let head = std::fs::read_to_string(".git/HEAD").ok()?;
    let head = head.trim();

    if let Some(rest) = head.strip_prefix("ref:") {
        let reference = rest.trim();
        let ref_path = Path::new(".git").join(reference);
        if let Ok(commit) = std::fs::read_to_string(&ref_path) {
            return Some(commit.trim().to_string());
        }
        // Fallback: packed-refs (best-effort).
        let packed = std::fs::read_to_string(".git/packed-refs").ok()?;
        for line in packed.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with('^') {
                continue;
            }
            let mut parts = line.split_whitespace();
            let commit = parts.next()?;
            let r = parts.next()?;
            if r == reference {
                return Some(commit.to_string());
            }
        }
        None
    } else {
        Some(head.to_string())
    }
}
