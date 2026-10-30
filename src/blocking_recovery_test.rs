use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::blocking::Client;
use crate::blocking_test::{start_server, start_server_until, wait_for};
use crate::debug::{FAILURE_RECOVERY_READ, FAILURE_RECOVERY_RECONNECT_TERMINAL};
use crate::message::{Message, MessageHandler};
use crate::protocol::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};
use crate::{ClientRecoveryOptions, Error, OkReply, Server, ServerRecoveryOptions};

#[crate::message(register)]
struct ResumeEchoRequest {
    text: String,
    delay_ms: u64,
}

#[crate::message(register)]
struct ResumeEchoReply {
    text: String,
}

/// Breaks the server's transport before replying, so that the reply reaches
/// the client only by replay after it resumes.
#[crate::message(register)]
struct ResumeBreakRequest {
    text: String,
}

/// Recorded on the server, per stream.
#[crate::message(register)]
struct ResumeRecord {
    seq: u32,
}

/// Asks for the records of the stream.
#[crate::message(register)]
struct ResumeRecords {}

#[crate::message(register)]
struct ResumeRecordsReply {
    seqs: Vec<u32>,
}

#[crate::message_handler]
impl MessageHandler for ResumeEchoRequest {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        Ok(Some(Box::new(ResumeEchoReply {
            text: self.text.clone(),
        })))
    }
}

#[crate::message_handler]
impl MessageHandler for ResumeBreakRequest {
    async fn handle(
        self: Box<Self>,
        stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        stream_ctx.stream.connection.close_transport_for_test();
        Ok(Some(Box::new(ResumeEchoReply {
            text: self.text.clone(),
        })))
    }
}

type Records = Mutex<Vec<u32>>;

#[crate::message_handler]
impl MessageHandler for ResumeRecord {
    async fn handle(
        self: Box<Self>,
        stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        let records = stream_ctx.get_context::<Records>().unwrap_or_else(|| {
            let records = Arc::new(Records::default());
            stream_ctx.set_context(Arc::clone(&records));
            records
        });
        records.lock().unwrap().push(self.seq);
        Ok(None)
    }
}

#[crate::message_handler]
impl MessageHandler for ResumeRecords {
    async fn handle(
        self: Box<Self>,
        stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        let seqs = stream_ctx
            .get_context::<Records>()
            .map(|records| records.lock().unwrap().clone())
            .unwrap_or_default();
        Ok(Some(Box::new(ResumeRecordsReply { seqs })))
    }
}

fn echo(text: &str) -> ResumeEchoRequest {
    ResumeEchoRequest {
        text: text.to_string(),
        delay_ms: 0,
    }
}

fn server_options() -> ServerRecoveryOptions {
    ServerRecoveryOptions {
        enable: true,
        detached_ttl: Duration::from_secs(5),
        ack_every: 1,
        ack_delay: Duration::from_millis(5),
        heartbeat_interval: Duration::from_millis(50),
        heartbeat_timeout: Duration::from_millis(250),
        ..ServerRecoveryOptions::default()
    }
}

fn client_options() -> ClientRecoveryOptions {
    ClientRecoveryOptions {
        enable: true,
        reconnect_min_backoff: Duration::from_millis(20),
        reconnect_max_backoff: Duration::from_millis(100),
        ..ClientRecoveryOptions::default()
    }
}

fn recovery_client() -> Client {
    Client::new()
        .with_connect_timeout(Duration::from_secs(5))
        .with_recv_timeout(Duration::from_secs(10))
        .with_recovery(client_options())
}

fn start_recovery_server() -> String {
    start_server(Server::new().with_recovery(server_options()))
}

/// Forwards TCP connections to a server. Tests freeze, cut or refuse the
/// connections to imitate network failures.
struct Proxy {
    addr: String,
    links: Arc<Mutex<Vec<ProxyLink>>>,
    refuse: Arc<AtomicBool>,
}

struct ProxyLink {
    client: TcpStream,
    server: TcpStream,
    frozen: Arc<AtomicBool>,
}

impl Proxy {
    fn start(target: &str) -> Self {
        let target = target.strip_prefix("tcp://").unwrap_or(target).to_string();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind proxy");
        let addr = format!("tcp://{}", listener.local_addr().expect("proxy addr"));
        let links = Arc::new(Mutex::new(Vec::new()));
        let refuse = Arc::new(AtomicBool::new(false));
        let (accepted, refusing) = (Arc::clone(&links), Arc::clone(&refuse));
        thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { continue };
                if refusing.load(Ordering::Acquire) {
                    continue;
                }
                let Ok(server) = TcpStream::connect(&target) else {
                    continue;
                };
                let frozen = Arc::new(AtomicBool::new(false));
                for (from, to) in [(&client, &server), (&server, &client)] {
                    let (from, to) = (from.try_clone().unwrap(), to.try_clone().unwrap());
                    let frozen = Arc::clone(&frozen);
                    thread::spawn(move || pump(from, to, frozen));
                }
                accepted.lock().unwrap().push(ProxyLink {
                    client,
                    server,
                    frozen,
                });
            }
        });
        Self {
            addr,
            links,
            refuse,
        }
    }

    /// Stops carrying bytes on the current connections without closing them,
    /// as a network that went silent would.
    fn freeze(&self) {
        for link in self.links.lock().unwrap().iter() {
            link.frozen.store(true, Ordering::Release);
        }
    }

    /// Closes the current connections.
    fn cut(&self) {
        for link in self.links.lock().unwrap().drain(..) {
            let _ = link.client.shutdown(Shutdown::Both);
            let _ = link.server.shutdown(Shutdown::Both);
        }
    }

    /// Closes new connections at once while `refuse` is set.
    fn refuse(&self, refuse: bool) {
        self.refuse.store(refuse, Ordering::Release);
    }
}

fn pump(mut from: TcpStream, mut to: TcpStream, frozen: Arc<AtomicBool>) {
    let mut buf = [0u8; 16 << 10];
    loop {
        let read = match from.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        if frozen.load(Ordering::Acquire) {
            continue;
        }
        if to.write_all(&buf[..read]).is_err() {
            break;
        }
    }
    if !frozen.load(Ordering::Acquire) {
        let _ = to.shutdown(Shutdown::Write);
    }
}

#[test]
fn recovery_negotiates_v3_and_close_releases_server() {
    let disconnects = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&disconnects);
    let server = Server::new()
        .with_recovery(ServerRecoveryOptions {
            detached_ttl: Duration::from_secs(60),
            ..server_options()
        })
        .on_disconnect(move |_| {
            seen.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });
    let addr = start_server(server);
    let conn = recovery_client().connect(&addr).expect("connect");
    let reply: ResumeEchoReply = conn.send_recv(echo("hi")).expect("send_recv");
    assert_eq!(reply.text, "hi");

    let state = conn.debug_state();
    assert_eq!(state.protocol, PROTOCOL_VERSION_V3);
    assert_eq!(state.next_send_seq, 1);
    let recovery = state.recovery.expect("recovery state");
    assert_eq!(recovery.role, "client");
    assert!(recovery.transport_attached);
    wait_for("the server to acknowledge the request", || {
        conn.debug_state().recovery.unwrap().last_acked_seq == 1
    });

    let started = Instant::now();
    conn.close();
    assert!(started.elapsed() < Duration::from_secs(2));
    // CLOSE releases the connection at once instead of after the TTL.
    wait_for("on_disconnect", || disconnects.load(Ordering::Relaxed) == 1);
    assert!(matches!(conn.send(echo("late")), Err(Error::Closed)));
}

#[test]
fn recovery_falls_back_to_v2() {
    let addr = start_server(Server::new());
    let conn = recovery_client().connect(&addr).expect("connect");
    assert_eq!(conn.debug_state().protocol, PROTOCOL_VERSION_V2);
    assert!(conn.debug_state().recovery.is_none());
    let reply: ResumeEchoReply = conn.send_recv(echo("v2")).expect("send_recv");
    assert_eq!(reply.text, "v2");
    conn.close();
}

#[test]
fn recovery_resumes_after_transport_break() {
    let addr = start_recovery_server();
    let conn = recovery_client().connect(&addr).expect("connect");
    let reply: ResumeEchoReply = conn.send_recv(echo("one")).expect("send_recv");
    assert_eq!(reply.text, "one");
    let gen = conn.transport_generation_for_test();

    conn.break_transport_for_test();
    let reply: ResumeEchoReply = conn.send_recv(echo("two")).expect("send_recv after break");
    assert_eq!(reply.text, "two");

    assert!(conn.transport_generation_for_test() > gen);
    let counters = conn.debug_state().counters;
    assert!(counters.reconnect_successes >= 1);
    assert!(counters.transport_attaches >= 2);
    assert!(counters.transport_detaches >= 1);
    conn.close();
}

#[test]
fn recovery_replays_reply_lost_with_server_transport() {
    let addr = start_recovery_server();
    let conn = recovery_client().connect(&addr).expect("connect");
    let stream = conn.new_stream();
    let reply: ResumeEchoReply = stream
        .send_recv(ResumeBreakRequest {
            text: "replayed".into(),
        })
        .expect("send_recv");
    assert_eq!(reply.text, "replayed");
    assert!(conn.debug_state().counters.reconnect_successes >= 1);
    // The stream keeps working on the new transport.
    let reply: ResumeEchoReply = stream.send_recv(echo("after")).expect("send_recv");
    assert_eq!(reply.text, "after");
    conn.close();
}

#[test]
fn recovery_delivers_sends_across_break_once_in_order() {
    let addr = start_recovery_server();
    let conn = recovery_client().connect(&addr).expect("connect");
    let stream = conn.new_stream();
    for seq in 0..20 {
        stream.send(ResumeRecord { seq }).expect("send");
    }
    conn.break_transport_for_test();
    for seq in 20..100 {
        stream.send(ResumeRecord { seq }).expect("send");
    }
    for _ in 0..100 {
        let _: OkReply = stream.recv().expect("record reply");
    }
    let records: ResumeRecordsReply = stream.send_recv(ResumeRecords {}).expect("records");
    assert_eq!(records.seqs, (0..100).collect::<Vec<_>>());
    conn.close();
}

#[test]
fn recovery_heartbeat_detects_silent_transport() {
    let addr = start_recovery_server();
    let proxy = Proxy::start(&addr);
    let conn = recovery_client().connect(&proxy.addr).expect("connect");
    let reply: ResumeEchoReply = conn.send_recv(echo("before")).expect("send_recv");
    assert_eq!(reply.text, "before");

    proxy.freeze();
    let started = Instant::now();
    // The request goes into the silent transport; the client notices within
    // the heartbeat timeout, resumes, and replays it.
    let reply: ResumeEchoReply = conn.send_recv(echo("after")).expect("send_recv");
    assert_eq!(reply.text, "after");
    assert!(started.elapsed() < Duration::from_secs(3));

    let state = conn.debug_state();
    assert!(state.counters.reconnect_successes >= 1);
    assert_eq!(state.last_failure_code, FAILURE_RECOVERY_READ);
    assert_eq!(state.last_failure, "recv timeout");
    conn.close();
}

#[test]
fn recovery_rejected_resume_closes_connection() {
    let addr = start_server(Server::new().with_recovery(ServerRecoveryOptions {
        detached_ttl: Duration::from_millis(100),
        ..server_options()
    }));
    let proxy = Proxy::start(&addr);
    let conn = recovery_client().connect(&proxy.addr).expect("connect");
    let _: ResumeEchoReply = conn.send_recv(echo("x")).expect("send_recv");

    // The server drops the detached connection before the client gets back.
    proxy.refuse(true);
    proxy.cut();
    thread::sleep(Duration::from_millis(500));
    proxy.refuse(false);

    wait_for("the rejected resume to close the connection", || {
        conn.is_closed()
    });
    let state = conn.debug_state();
    assert_eq!(state.last_failure_code, FAILURE_RECOVERY_RECONNECT_TERMINAL);
    assert!(state.counters.reconnect_failures >= 1);
    assert!(matches!(conn.recv::<ResumeEchoReply>(), Err(Error::Closed)));
}

#[test]
fn recovery_server_shutdown_closes_client() {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let addr = start_server_until(Server::new().with_recovery(server_options()), async move {
        let _ = stopped.await;
    });
    let conn = recovery_client().connect(&addr).expect("connect");
    let _: ResumeEchoReply = conn.send_recv(echo("x")).expect("send_recv");

    stop.send(()).expect("stop server");
    // The server sends CLOSE, so the client does not try to resume.
    wait_for("the client to close", || conn.is_closed());
    assert_eq!(conn.debug_state().counters.reconnect_attempts, 0);
}

#[test]
fn recovery_send_waits_for_replay_room() {
    let addr = start_recovery_server();
    let conn = recovery_client()
        .with_recovery(ClientRecoveryOptions {
            max_replay_bytes: 600,
            ..client_options()
        })
        .connect(&addr)
        .expect("connect");
    let stream = conn.new_stream();

    // A frame larger than the whole buffer never fits.
    let too_large = ResumeEchoRequest {
        text: "x".repeat(1000),
        delay_ms: 0,
    };
    assert!(matches!(
        stream.send(too_large),
        Err(Error::FrameTooLarge(_))
    ));

    // About ten frames fit at once, so most sends wait for an ACK.
    for seq in 0..300 {
        stream.send(ResumeRecord { seq }).expect("send");
    }
    for _ in 0..300 {
        let _: OkReply = stream.recv().expect("record reply");
    }
    let records: ResumeRecordsReply = stream.send_recv(ResumeRecords {}).expect("records");
    assert_eq!(records.seqs, (0..300).collect::<Vec<_>>());
    let recovery = conn.debug_state().recovery.expect("recovery state");
    assert!(recovery.replay_bytes <= 600);
    conn.close();
}

#[test]
fn recovery_close_while_detached_is_prompt() {
    let addr = start_recovery_server();
    let proxy = Proxy::start(&addr);
    let conn = recovery_client().connect(&proxy.addr).expect("connect");
    let _: ResumeEchoReply = conn.send_recv(echo("x")).expect("send_recv");

    proxy.refuse(true);
    proxy.cut();
    wait_for("the client to detach", || {
        !conn.debug_state().recovery.unwrap().transport_attached
    });
    let started = Instant::now();
    conn.close();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(conn.is_closed());
}

#[test]
fn recovery_concurrent_streams_survive_repeated_breaks() {
    let addr = start_recovery_server();
    let conn = recovery_client().connect(&addr).expect("connect");
    let done = AtomicBool::new(false);
    thread::scope(|scope| {
        scope.spawn(|| {
            while !done.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(25));
                conn.break_transport_for_test();
            }
        });
        let workers: Vec<_> = (0..4)
            .map(|worker| {
                let conn = &conn;
                scope.spawn(move || {
                    let stream = conn.new_stream();
                    for i in 0..100 {
                        let text = format!("{worker}-{i}");
                        let reply = stream.send_recv::<ResumeEchoReply>(ResumeEchoRequest {
                            text: text.clone(),
                            delay_ms: 1,
                        });
                        let reply = reply.unwrap_or_else(|err| {
                            panic!(
                                "request {text}: {err}\nstream: {:#?}\nconnection: {:#?}",
                                stream.debug_state(),
                                conn.debug_state()
                            )
                        });
                        assert_eq!(reply.text, text);
                    }
                    stream.close();
                })
            })
            .collect();
        let results: Vec<_> = workers.into_iter().map(|worker| worker.join()).collect();
        done.store(true, Ordering::Release);
        for result in results {
            result.expect("worker");
        }
    });
    assert!(conn.debug_state().counters.reconnect_successes >= 2);
    conn.close();
}
