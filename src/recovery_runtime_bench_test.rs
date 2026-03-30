use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::recovery::{RecoveryRegistry, RecoveryState, ServerRecoveryOptions};
use crate::recovery_protocol::RecoveryToken;

/// Build a minimal RecoveryState with default negotiated options,
/// mirroring Go's benchRecoveryState().
fn bench_recovery_state() -> Arc<RecoveryState> {
    let registry: RecoveryRegistry = Arc::new(Mutex::new(std::collections::HashMap::new()));
    RecoveryState::new_server(
        ServerRecoveryOptions {
            enable: true,
            ..ServerRecoveryOptions::default()
        },
        RecoveryToken::default(),
        RecoveryToken::default(),
        registry,
    )
}

fn bench_iterations() -> usize {
    std::env::var("AM_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1_000_000)
}

#[tokio::test]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_next_ack_wait_no_pending() {
    let r = bench_recovery_state();
    let n = bench_iterations();
    let start = Instant::now();
    for _ in 0..n {
        let _ = r.next_ack_wait();
    }
    let ns_per_op = start.elapsed().as_nanos() as f64 / n as f64;
    println!("BenchmarkNextAckWait_NoPending\t{ns_per_op:.2} ns/op  ({n} iters)");
}

#[tokio::test]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_next_ack_wait_with_delay() {
    let r = bench_recovery_state();
    // Simulate a pending ACK with a future deadline by noting a received seq.
    // With ack_every=64 (default), one note_received sets ack_due_at ~20ms ahead.
    r.note_received(1);
    let n = bench_iterations();
    let start = Instant::now();
    for _ in 0..n {
        let _ = r.next_ack_wait();
    }
    let ns_per_op = start.elapsed().as_nanos() as f64 / n as f64;
    println!("BenchmarkNextAckWait_WithDelay\t{ns_per_op:.2} ns/op  ({n} iters)");
}

#[tokio::test]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_take_pending_ack_empty() {
    let r = bench_recovery_state();
    let n = bench_iterations();
    let start = Instant::now();
    for _ in 0..n {
        let _ = r.take_pending_ack();
    }
    let ns_per_op = start.elapsed().as_nanos() as f64 / n as f64;
    println!("BenchmarkTakePendingControl_Empty\t{ns_per_op:.2} ns/op  ({n} iters)");
}

#[tokio::test]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_take_pending_ack_ready() {
    let r = bench_recovery_state();
    let n = bench_iterations();
    let start = Instant::now();
    for i in 0..n {
        // Set ack_due by noting a new seq each iteration (triggers ack_every threshold).
        r.note_received((i + 1) as u64);
        // Every ack_every (64) calls, ack_due becomes true and take_pending_ack returns Some.
        let _ = r.take_pending_ack();
    }
    let ns_per_op = start.elapsed().as_nanos() as f64 / n as f64;
    println!("BenchmarkTakePendingControl_AckReady\t{ns_per_op:.2} ns/op  ({n} iters)");
}

#[tokio::test]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_wait_calc() {
    let r = bench_recovery_state();
    let n = bench_iterations();
    let start = Instant::now();
    for _ in 0..n {
        let ack_wait = r.next_ack_wait();
        let heartbeat_wait = r.heartbeat_interval();
        let wait = if !heartbeat_wait.is_zero() && (ack_wait.is_zero() || heartbeat_wait < ack_wait)
        {
            heartbeat_wait
        } else {
            ack_wait
        };
        let _ = wait;
    }
    let ns_per_op = start.elapsed().as_nanos() as f64 / n as f64;
    println!("BenchmarkWaitCalc\t{ns_per_op:.2} ns/op  ({n} iters)");
}

#[tokio::test]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_wait_calc_ack_beats_heartbeat() {
    let r = bench_recovery_state();
    // Set a pending ACK with a short deadline (via note_received).
    r.note_received(1);
    let n = bench_iterations();
    let start = Instant::now();
    for _ in 0..n {
        let ack_wait = r.next_ack_wait();
        let heartbeat_wait = r.heartbeat_interval();
        let wait = if !heartbeat_wait.is_zero() && (ack_wait.is_zero() || heartbeat_wait < ack_wait)
        {
            heartbeat_wait
        } else {
            ack_wait
        };
        let _ = wait;
    }
    let ns_per_op = start.elapsed().as_nanos() as f64 / n as f64;
    println!("BenchmarkWaitCalc_AckBeatsHeartbeat\t{ns_per_op:.2} ns/op  ({n} iters)");
}

#[tokio::test]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_wait_calc_v2_old_style() {
    let r = bench_recovery_state();
    let n = bench_iterations();
    let start = Instant::now();
    for _ in 0..n {
        let wait = r.next_ack_wait();
        let heartbeat_wait = r.heartbeat_interval();
        let wait_for_heartbeat =
            !heartbeat_wait.is_zero() && (wait.is_zero() || heartbeat_wait < wait);
        let mut w = wait;
        if wait_for_heartbeat {
            w = heartbeat_wait;
        }
        // v2: check bool at second location
        if wait_for_heartbeat {
            let _ = w;
        }
    }
    let ns_per_op = start.elapsed().as_nanos() as f64 / n as f64;
    println!("BenchmarkWaitCalc_V2OldStyle\t{ns_per_op:.2} ns/op  ({n} iters)");
}

#[tokio::test]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_wait_calc_v3_optimized() {
    let r = bench_recovery_state();
    let n = bench_iterations();
    let start = Instant::now();
    for _ in 0..n {
        let ack_wait = r.next_ack_wait();
        let heartbeat_wait = r.heartbeat_interval();
        let wait = if !heartbeat_wait.is_zero() && (ack_wait.is_zero() || heartbeat_wait < ack_wait)
        {
            heartbeat_wait
        } else {
            ack_wait
        };
        // v3: direct comparison, no bool variable
        if wait == heartbeat_wait {
            let _ = wait;
        }
    }
    let ns_per_op = start.elapsed().as_nanos() as f64 / n as f64;
    println!("BenchmarkWaitCalc_V3Optimized\t{ns_per_op:.2} ns/op  ({n} iters)");
}
