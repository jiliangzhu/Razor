use std::path::{Path, PathBuf};

use crate::types::now_ms;

#[derive(Clone, Debug)]
pub struct RunContext {
    pub run_id: String,
    pub run_dir: PathBuf,
    pub start_ts_ms: u64,
}

pub fn create_run_context(base_data_dir: &Path) -> anyhow::Result<RunContext> {
    std::fs::create_dir_all(base_data_dir)?;

    let start_ts_ms = now_ms();
    let pid = std::process::id();

    for attempt in 0..1000u32 {
        let run_id = format_run_id(start_ts_ms, pid, attempt);
        let run_dir = base_data_dir.join(&run_id);
        if run_dir.exists() {
            continue;
        }
        std::fs::create_dir_all(&run_dir)?;
        update_run_latest_symlink(base_data_dir, &run_dir)?;
        write_latest_marker(&run_dir)?;
        return Ok(RunContext {
            run_id,
            run_dir,
            start_ts_ms,
        });
    }

    anyhow::bail!("failed to allocate unique run_dir after many attempts")
}

fn update_run_latest_symlink(base_data_dir: &Path, run_dir: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let link_path = base_data_dir.join("run_latest");
        let target = run_dir.strip_prefix(base_data_dir).unwrap_or(run_dir);
        if let Ok(meta) = std::fs::symlink_metadata(&link_path) {
            if !meta.file_type().is_symlink() {
                anyhow::bail!("refusing to overwrite non-symlink {}", link_path.display());
            }
            std::fs::remove_file(&link_path)?;
        }

        symlink(target, link_path)?;
        Ok(())
    }

    #[cfg(not(unix))]
    {
        let _ = (base_data_dir, run_dir);
        Ok(())
    }
}

fn format_run_id(start_ts_ms: u64, pid: u32, attempt: u32) -> String {
    let secs = (start_ts_ms / 1000) as i64;
    let days = secs.div_euclid(86_400);
    let sec_of_day = secs.rem_euclid(86_400);
    let hour = (sec_of_day / 3600) as u32;
    let minute = ((sec_of_day % 3600) / 60) as u32;
    let second = (sec_of_day % 60) as u32;

    let (year, month, day) = civil_from_days(days);
    let rand6 = ((start_ts_ms as u32) ^ pid ^ attempt) % 1_000_000;

    format!("run_{year:04}{month:02}{day:02}_{hour:02}{minute:02}{second:02}_{rand6:06}")
}

// UTC date conversion (Howard Hinnant's algorithm).
// Input: days since 1970-01-01.
fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 }.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096).div_euclid(365);
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2).div_euclid(153);
    let d = doy - (153 * mp + 2).div_euclid(5) + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + i64::from(m <= 2);
    (year as i32, m as u32, d as u32)
}

fn write_latest_marker(run_dir: &Path) -> anyhow::Result<()> {
    let marker = run_dir.join("LATEST");
    std::fs::write(&marker, format!("{}\n", run_dir.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_id_format_is_stable() {
        let id = format_run_id(1_700_000_000_000, 1234, 0);
        assert!(id.starts_with("run_"));
        assert!(id.contains('_'));
        assert_eq!(id.len(), "run_YYYYMMDD_HHMMSS_000000".len());
    }
}
