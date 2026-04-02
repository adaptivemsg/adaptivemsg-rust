use std::net::TcpListener;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;

use crate::message::{Message, MessageHandler};
use crate::recovery::ServerRecoveryOptions;
use crate::Server;

#[crate::message(register)]
struct BenchEchoRequest {
    text: String,
}

#[crate::message(register)]
struct BenchEchoReply {
    text: String,
}

#[crate::message_handler]
impl MessageHandler for BenchEchoRequest {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        Ok(Some(Box::new(BenchEchoReply {
            text: self.text.clone(),
        })))
    }
}

fn ephemeral_tcp_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral tcp addr");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr.to_string()
}

async fn wait_server_ready(addr: &str) {
    for _ in 0..50u32 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server did not become ready");
}

async fn run_protocol_send_recv_perf_case(recovery_enabled: bool, iterations: usize) -> f64 {
    run_protocol_send_recv_perf_case_with_codec(recovery_enabled, iterations, None).await
}

async fn run_protocol_send_recv_perf_case_with_codec(
    recovery_enabled: bool,
    iterations: usize,
    codecs: Option<Vec<crate::CodecID>>,
) -> f64 {
    let addr = ephemeral_tcp_addr();
    let serve_addr = addr.clone();
    let server_codecs = codecs.clone();
    let server = tokio::spawn(async move {
        let mut server = Server::new();
        if let Some(codecs) = server_codecs {
            server = server.with_codecs(&codecs);
        }
        if recovery_enabled {
            server = server.with_recovery(ServerRecoveryOptions {
                enable: true,
                detached_ttl: Duration::from_secs(5),
                ack_every: 64,
                ack_delay: Duration::from_millis(20),
                heartbeat_interval: Duration::from_secs(30),
                heartbeat_timeout: Duration::from_secs(90),
                ..ServerRecoveryOptions::default()
            });
        }
        let _ = server.serve(&serve_addr).await;
    });
    wait_server_ready(&addr).await;

    let mut client = crate::Client::new().with_timeout(Duration::from_secs(2));
    if let Some(codecs) = codecs {
        client = client.with_codecs(&codecs);
    }
    if recovery_enabled {
        client = client.with_recovery(crate::ClientRecoveryOptions {
            enable: true,
            reconnect_min_backoff: Duration::from_millis(100),
            reconnect_max_backoff: Duration::from_secs(2),
            max_replay_bytes: 8 << 20,
        });
    }
    let conn = client
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("client connect");
    conn.set_recv_timeout(Duration::from_secs(2));

    let warmup: BenchEchoReply = conn
        .send_recv(BenchEchoRequest {
            text: "warmup".to_string(),
        })
        .await
        .expect("warmup send_recv");
    assert_eq!(warmup.text, "warmup");

    // Run the hot loop in a spawned task so it runs on a tokio worker thread.
    // The #[tokio::test] main task has higher scheduling overhead (~2.5x) because
    // it runs outside the normal work-stealing pool. Using tokio::spawn ensures
    // the same runtime characteristics as the scaling benchmark and real-world
    // server workloads.
    let stream = conn.new_stream();
    stream.set_recv_timeout(Duration::from_secs(2));
    let handle = tokio::spawn(async move {
        let start = Instant::now();
        for _ in 0..iterations {
            let reply: BenchEchoReply = stream
                .send_recv(BenchEchoRequest {
                    text: "x".to_string(),
                })
                .await
                .expect("bench send_recv");
            assert_eq!(reply.text, "x");
        }
        start.elapsed()
    });
    let elapsed = handle.await.unwrap();
    let ns_per_op = elapsed.as_nanos() as f64 / iterations as f64;

    conn.close();
    server.abort();

    ns_per_op
}

fn bench_iterations() -> usize {
    std::env::var("AM_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1000)
}

fn bench_runs() -> usize {
    std::env::var("AM_BENCH_RUNS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(5)
}

async fn run_bench_median(name: &str, recovery: bool, codecs: Option<Vec<crate::CodecID>>) {
    let iterations = bench_iterations();
    let runs = bench_runs();
    let mut results: Vec<f64> = Vec::with_capacity(runs);
    for _ in 0..runs {
        let ns =
            run_protocol_send_recv_perf_case_with_codec(recovery, iterations, codecs.clone()).await;
        results.push(ns);
    }
    results.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = results[runs / 2];
    println!("{name}\t{median:.2} ns/op  (median of {runs} runs x {iterations} ops)");
}

// All protocol benchmarks use multi_thread to match Go's default multi-goroutine
// runtime. Using current_thread would serialize the server and client onto one OS
// thread, doubling measured latency and making the numbers incomparable with Go.

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_protocol_v2_send_recv() {
    run_bench_median("BenchmarkProtocolV2SendRecv", false, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_protocol_v3_recovery_send_recv() {
    run_bench_median("BenchmarkProtocolV3RecoverySendRecv", true, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_protocol_v2_send_recv_msgpack() {
    let codecs = vec![crate::codec_msgpack::CodecMsgpackCompact];
    run_bench_median("BenchmarkProtocolV2SendRecv_Msgpack", false, Some(codecs)).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_protocol_v3_recovery_send_recv_msgpack() {
    let codecs = vec![crate::codec_msgpack::CodecMsgpackCompact];
    run_bench_median(
        "BenchmarkProtocolV3RecoverySendRecv_Msgpack",
        true,
        Some(codecs),
    )
    .await;
}
