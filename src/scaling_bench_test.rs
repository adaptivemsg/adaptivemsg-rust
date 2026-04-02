use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::sync::Barrier;

use crate::message::{Message, MessageHandler};
use crate::recovery::ServerRecoveryOptions;
use crate::stream::Stream;
use crate::Server;

#[crate::message(register)]
struct ScaleEchoRequest {
    text: String,
}

#[crate::message(register)]
struct ScaleEchoReply {
    text: String,
}

#[crate::message_handler]
impl MessageHandler for ScaleEchoRequest {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        Ok(Some(Box::new(ScaleEchoReply {
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

async fn run_scaling_perf_case(
    conns: usize,
    streams_per_conn: usize,
    iterations: usize,
    recovery_enabled: bool,
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

    // Create all connections and streams
    let mut all_streams: Vec<Stream> = Vec::with_capacity(conns * streams_per_conn);
    let mut all_conns = Vec::with_capacity(conns);

    for _ in 0..conns {
        let mut client = crate::Client::new().with_timeout(Duration::from_secs(5));
        if let Some(ref codecs) = codecs {
            client = client.with_codecs(codecs);
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
        conn.set_recv_timeout(Duration::from_secs(5));

        // Warm up
        let _warmup: ScaleEchoReply = conn
            .send_recv(ScaleEchoRequest {
                text: "warmup".to_string(),
            })
            .await
            .expect("warmup send_recv");

        // Create streams
        for _ in 0..streams_per_conn {
            let stream = conn.new_stream();
            stream.set_recv_timeout(Duration::from_secs(5));
            all_streams.push(stream);
        }
        all_conns.push(conn);
    }

    let total_streams = all_streams.len();
    let ops_per_stream = iterations / total_streams;
    let ops_per_stream = if ops_per_stream < 1 {
        1
    } else {
        ops_per_stream
    };

    let errors = Arc::new(AtomicU64::new(0));
    let first_error = Arc::new(Mutex::new(None::<String>));
    let barrier = Arc::new(Barrier::new(total_streams + 1));

    let mut handles = Vec::with_capacity(total_streams);

    for stream in all_streams {
        let errors = Arc::clone(&errors);
        let first_error = Arc::clone(&first_error);
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..ops_per_stream {
                let result: Result<ScaleEchoReply, _> = stream
                    .send_recv(ScaleEchoRequest {
                        text: "x".to_string(),
                    })
                    .await;
                match result {
                    Ok(reply) if reply.text == "x" => {}
                    Ok(reply) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        let mut slot = first_error.lock().expect("first_error lock poisoned");
                        if slot.is_none() {
                            *slot = Some(format!("unexpected reply payload: {:?}", reply.text));
                        }
                        return;
                    }
                    Err(err) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        let mut slot = first_error.lock().expect("first_error lock poisoned");
                        if slot.is_none() {
                            *slot = Some(err.to_string());
                        }
                        return;
                    }
                }
            }
        }));
    }

    // Synchronize all tasks to start at the same time
    barrier.wait().await;
    let start = Instant::now();

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();

    // Cleanup
    for conn in all_conns {
        conn.close();
    }
    server.abort();

    if errors.load(Ordering::Relaxed) > 0 {
        let detail = first_error
            .lock()
            .expect("first_error lock poisoned")
            .clone()
            .unwrap_or_else(|| "unknown benchmark error".to_string());
        panic!("errors during benchmark: {detail}");
    }

    elapsed.as_nanos() as f64
}

async fn run_scaling_bench_median(
    name: &str,
    conns: usize,
    streams_per_conn: usize,
    recovery: bool,
    codecs: Option<Vec<crate::CodecID>>,
) {
    let iterations = bench_iterations();
    let runs = bench_runs();
    let mut results: Vec<f64> = Vec::with_capacity(runs);

    for _ in 0..runs {
        let ns = run_scaling_perf_case(
            conns,
            streams_per_conn,
            iterations,
            recovery,
            codecs.clone(),
        )
        .await;
        results.push(ns);
    }
    results.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median_ns = results[runs / 2];
    let ops_per_sec = iterations as f64 / (median_ns / 1e9);

    println!(
        "{name}\t{ops_per_sec:.0} ops/sec  ({median_ns:.0} ns total, median of {runs} runs x {iterations} ops)"
    );
}

// V2 scaling tests

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v2_1conn_1stream() {
    run_scaling_bench_median("ScalingV2_1Conn1Stream", 1, 1, false, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v2_1conn_4stream() {
    run_scaling_bench_median("ScalingV2_1Conn4Stream", 1, 4, false, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v2_1conn_16stream() {
    run_scaling_bench_median("ScalingV2_1Conn16Stream", 1, 16, false, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v2_1conn_64stream() {
    run_scaling_bench_median("ScalingV2_1Conn64Stream", 1, 64, false, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v2_4conn_1stream() {
    run_scaling_bench_median("ScalingV2_4Conn1Stream", 4, 1, false, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v2_4conn_4stream() {
    run_scaling_bench_median("ScalingV2_4Conn4Stream", 4, 4, false, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v2_4conn_16stream() {
    run_scaling_bench_median("ScalingV2_4Conn16Stream", 4, 16, false, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v2_4conn_64stream() {
    run_scaling_bench_median("ScalingV2_4Conn64Stream", 4, 64, false, None).await;
}

// V3 scaling tests

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v3_1conn_1stream() {
    run_scaling_bench_median("ScalingV3_1Conn1Stream", 1, 1, true, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v3_4conn_1stream() {
    run_scaling_bench_median("ScalingV3_4Conn1Stream", 4, 1, true, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v3_1conn_4stream() {
    run_scaling_bench_median("ScalingV3_1Conn4Stream", 1, 4, true, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v3_1conn_16stream() {
    run_scaling_bench_median("ScalingV3_1Conn16Stream", 1, 16, true, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v3_1conn_64stream() {
    run_scaling_bench_median("ScalingV3_1Conn64Stream", 1, 64, true, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v3_4conn_4stream() {
    run_scaling_bench_median("ScalingV3_4Conn4Stream", 4, 4, true, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v3_4conn_16stream() {
    run_scaling_bench_median("ScalingV3_4Conn16Stream", 4, 16, true, None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v3_4conn_64stream() {
    run_scaling_bench_median("ScalingV3_4Conn64Stream", 4, 64, true, None).await;
}

// Msgpack variants for Go comparison

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v2_4conn_64stream_msgpack() {
    let codecs = vec![crate::codec_msgpack::CodecMsgpackCompact];
    run_scaling_bench_median("ScalingV2_4Conn64Stream_Msgpack", 4, 64, false, Some(codecs)).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_v3_4conn_1stream_msgpack() {
    let codecs = vec![crate::codec_msgpack::CodecMsgpackCompact];
    run_scaling_bench_median("ScalingV3_4Conn1Stream_Msgpack", 4, 1, true, Some(codecs)).await;
}

// All scaling benchmarks runner
#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual benchmark test; run with -- --ignored --nocapture"]
async fn benchmark_scaling_all() {
    let configs = [
        (1, 1),
        (1, 4),
        (1, 16),
        (1, 64),
        (4, 1),
        (4, 4),
        (4, 16),
        (4, 64),
    ];

    println!("\nRust Scaling Benchmark (Postcard codec)");
    println!("{:-<70}", "");

    for (conns, streams) in configs.iter() {
        let name = format!("V2_{conns}Conn{streams}Stream");
        run_scaling_bench_median(&name, *conns, *streams, false, None).await;
    }

    println!();

    for (conns, streams) in configs.iter() {
        let name = format!("V3_{conns}Conn{streams}Stream");
        run_scaling_bench_median(&name, *conns, *streams, true, None).await;
    }

    println!("\nRust Scaling Benchmark (Msgpack codec, for Go comparison)");
    println!("{:-<70}", "");

    let codecs = vec![crate::codec_msgpack::CodecMsgpackCompact];
    for (conns, streams) in configs.iter() {
        let name = format!("V2_{conns}Conn{streams}Stream_Msgpack");
        run_scaling_bench_median(&name, *conns, *streams, false, Some(codecs.clone())).await;
    }

    println!();

    for (conns, streams) in configs.iter() {
        let name = format!("V3_{conns}Conn{streams}Stream_Msgpack");
        run_scaling_bench_median(&name, *conns, *streams, true, Some(codecs.clone())).await;
    }
}
