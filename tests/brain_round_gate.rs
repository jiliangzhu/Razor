use razor::brain::{RoundGate, RoundGateStatus};

#[test]
fn round_gate_skips_when_missing_round_info() {
    let gate = RoundGate::new(true, 10);
    let status = gate.status(1_000_000, None);
    assert_eq!(status, RoundGateStatus::SkipNoRoundInfo);
}

#[test]
fn round_gate_blocks_when_elapsed_exceeds_window() {
    let gate = RoundGate::new(true, 10);
    let start_ms = 1_000_000;
    let now_ms = start_ms + (11 * 60_000);
    let status = gate.status(now_ms, Some(start_ms));
    assert_eq!(status, RoundGateStatus::Blocked);
}

#[test]
fn round_gate_passes_inside_window() {
    let gate = RoundGate::new(true, 10);
    let start_ms = 1_000_000;
    let now_ms = start_ms + (5 * 60_000);
    let status = gate.status(now_ms, Some(start_ms));
    assert_eq!(status, RoundGateStatus::Pass);
}
