use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use crate::codec_postcard::CodecPostcard;
use crate::message::{Message, MessageHandler};
use crate::protocol::{handshake_client, DEFAULT_MAX_FRAME, PROTOCOL_VERSION_V3};
use crate::recovery::ServerRecoveryOptions;
use crate::recovery_protocol::{
    new_recovery_token, parse_control_payload, read_attach_request, read_attach_response,
    write_attach_request, write_attach_response, AttachRequest, AttachResponse, ATTACH_MODE_NEW,
    ATTACH_MODE_RESUME, ATTACH_STATUS_OK, ATTACH_STATUS_REJECTED, CONTROL_STREAM_ID,
    CONTROL_TYPE_PING, RECOVERY_TOKEN_LEN,
};
use crate::Server;

#[crate::message(register)]
struct RecoveryEchoRequest {
    text: String,
}

#[crate::message(register)]
struct RecoveryEchoReply {
    text: String,
}

#[crate::message(register)]
struct RecoveryDropReplyRequest {
    text: String,
}

#[crate::message_handler]
impl MessageHandler for RecoveryEchoRequest {
    async fn handle(
        self: Box<Self>,
        _stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        Ok(Some(Box::new(RecoveryEchoReply {
            text: self.text.clone(),
        })))
    }
}

static DROP_REPLY_ONCE: AtomicBool = AtomicBool::new(false);

#[crate::message_handler]
impl MessageHandler for RecoveryDropReplyRequest {
    async fn handle(
        self: Box<Self>,
        stream_ctx: crate::StreamContext,
    ) -> crate::Result<Option<Box<dyn Message>>> {
        if !DROP_REPLY_ONCE.swap(true, Ordering::AcqRel) {
            stream_ctx.stream.connection.close_transport_for_test();
        }
        Ok(Some(Box::new(RecoveryEchoReply {
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

async fn start_recovery_server(addr: &str) -> JoinHandle<()> {
    let serve_addr = addr.to_string();
    tokio::spawn(async move {
        let recovery = ServerRecoveryOptions {
            enable: true,
            detached_ttl: Duration::from_secs(2),
            ack_every: 1,
            ack_delay: Duration::from_millis(5),
            heartbeat_interval: Duration::from_millis(50),
            heartbeat_timeout: Duration::from_millis(200),
            ..ServerRecoveryOptions::default()
        };
        let server = Server::new().with_recovery(recovery);
        let _ = server.serve(&serve_addr).await;
    })
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

async fn connect_v3(
    addr: &str,
) -> (
    tokio::net::tcp::OwnedReadHalf,
    tokio::net::tcp::OwnedWriteHalf,
) {
    let stream = TcpStream::connect(addr).await.expect("connect");
    let (mut reader, mut writer) = stream.into_split();
    let config = handshake_client(
        &mut reader,
        &mut writer,
        &[CodecPostcard],
        DEFAULT_MAX_FRAME,
        PROTOCOL_VERSION_V3,
    )
    .await
    .expect("handshake v3");
    assert_eq!(config.version, PROTOCOL_VERSION_V3);
    (reader, writer)
}

async fn start_message_recovery_server(addr: &str) -> JoinHandle<()> {
    let serve_addr = addr.to_string();
    tokio::spawn(async move {
        let recovery = ServerRecoveryOptions {
            enable: true,
            detached_ttl: Duration::from_secs(2),
            ack_every: 1,
            ack_delay: Duration::from_millis(5),
            heartbeat_interval: Duration::from_millis(20),
            heartbeat_timeout: Duration::from_millis(80),
            ..ServerRecoveryOptions::default()
        };
        let server = Server::new().with_recovery(recovery);
        let _ = server.serve(&serve_addr).await;
    })
}

async fn wait_for_generation_change(connection: &crate::Connection, previous: u64) {
    for _ in 0..200u32 {
        if connection.transport_generation_for_test() != previous {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("transport generation did not change");
}

async fn start_heartbeat_blackhole_server(
    addr: &str,
    opts: ServerRecoveryOptions,
) -> (JoinHandle<()>, tokio::sync::oneshot::Receiver<()>) {
    let listener = TokioTcpListener::bind(addr)
        .await
        .expect("bind blackhole listener");
    let (resumed_tx, resumed_rx) = tokio::sync::oneshot::channel();
    let resumed_tx = Arc::new(std::sync::Mutex::new(Some(resumed_tx)));
    let handle = tokio::spawn(async move {
        let options = opts.normalized();
        let codecs = [CodecPostcard];

        let (first_stream, _) = listener.accept().await.expect("accept first stream");
        let (mut first_reader, mut first_writer) = first_stream.into_split();
        let first_cfg = crate::protocol::handshake_server(
            &mut first_reader,
            &mut first_writer,
            &codecs,
            DEFAULT_MAX_FRAME,
            true,
        )
        .await
        .expect("first handshake");
        assert_eq!(first_cfg.version, PROTOCOL_VERSION_V3);
        let first_req = read_attach_request(&mut first_reader)
            .await
            .expect("read first attach request");
        assert_eq!(first_req.mode, ATTACH_MODE_NEW);

        let connection_id = new_recovery_token().expect("new connection id token");
        let resume_secret = new_recovery_token().expect("new resume secret token");
        let first_resp = AttachResponse {
            status: ATTACH_STATUS_OK,
            connection_id,
            resume_secret,
            last_recv_seq: 0,
            negotiated: options.negotiated(),
        };
        write_attach_response(&mut first_writer, &first_resp)
            .await
            .expect("write first attach response");

        let first_reader_task = tokio::spawn(async move {
            let header_len = crate::frame::FRAME_HEADER_LEN_V3;
            loop {
                let mut header = vec![0u8; header_len];
                if first_reader.read_exact(&mut header).await.is_err() {
                    return;
                }
                let (_, _, payload_len) = crate::frame::parse_header(&header, PROTOCOL_VERSION_V3)
                    .expect("parse frame header");
                let mut payload = vec![0u8; payload_len];
                if first_reader.read_exact(&mut payload).await.is_err() {
                    return;
                }
            }
        });

        let (second_stream, _) = listener.accept().await.expect("accept second stream");
        let (mut second_reader, mut second_writer) = second_stream.into_split();
        let second_cfg = crate::protocol::handshake_server(
            &mut second_reader,
            &mut second_writer,
            &codecs,
            DEFAULT_MAX_FRAME,
            true,
        )
        .await
        .expect("second handshake");
        assert_eq!(second_cfg.version, PROTOCOL_VERSION_V3);
        let second_req = read_attach_request(&mut second_reader)
            .await
            .expect("read second attach request");
        if second_req.mode != ATTACH_MODE_RESUME
            || second_req.connection_id != connection_id
            || second_req.resume_secret != resume_secret
        {
            let _ = write_attach_response(
                &mut second_writer,
                &AttachResponse {
                    status: ATTACH_STATUS_REJECTED,
                    ..AttachResponse::default()
                },
            )
            .await;
            return;
        }
        let second_resp = AttachResponse {
            status: ATTACH_STATUS_OK,
            connection_id,
            resume_secret,
            last_recv_seq: 0,
            negotiated: options.negotiated(),
        };
        write_attach_response(&mut second_writer, &second_resp)
            .await
            .expect("write second attach response");
        if let Some(tx) = resumed_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }

        let header_len = crate::frame::FRAME_HEADER_LEN_V3;
        loop {
            let mut header = vec![0u8; header_len];
            if second_reader.read_exact(&mut header).await.is_err() {
                break;
            }
            let (stream_id, _, payload_len) =
                match crate::frame::parse_header(&header, PROTOCOL_VERSION_V3) {
                    Ok(parsed) => parsed,
                    Err(_) => break,
                };
            let mut payload = vec![0u8; payload_len];
            if second_reader.read_exact(&mut payload).await.is_err() {
                break;
            }
            if stream_id != CONTROL_STREAM_ID {
                continue;
            }
            let Ok((control_type, _)) = parse_control_payload(&payload) else {
                break;
            };
            if control_type == CONTROL_TYPE_PING {
                // Intentionally blackhole heartbeat pings.
                continue;
            }
        }

        first_reader_task.abort();
    });
    (handle, resumed_rx)
}

#[tokio::test]
async fn recovery_attach_new_ok() {
    let addr = ephemeral_tcp_addr();
    let server = start_recovery_server(&addr).await;
    wait_server_ready(&addr).await;

    let (mut reader, mut writer) = connect_v3(&addr).await;
    let request = AttachRequest {
        mode: ATTACH_MODE_NEW,
        ..AttachRequest::default()
    };
    write_attach_request(&mut writer, &request)
        .await
        .expect("write attach new");
    let response = read_attach_response(&mut reader)
        .await
        .expect("read attach response");

    assert_eq!(response.status, ATTACH_STATUS_OK);
    assert_ne!(response.connection_id, [0; RECOVERY_TOKEN_LEN]);
    assert_ne!(response.resume_secret, [0; RECOVERY_TOKEN_LEN]);
    assert_eq!(response.last_recv_seq, 0);
    assert!(response.negotiated.ack_every > 0);

    server.abort();
}

#[tokio::test]
async fn recovery_resume_unknown_connection_rejected() {
    let addr = ephemeral_tcp_addr();
    let server = start_recovery_server(&addr).await;
    wait_server_ready(&addr).await;

    let (mut reader, mut writer) = connect_v3(&addr).await;
    let request = AttachRequest {
        mode: ATTACH_MODE_RESUME,
        connection_id: [7; RECOVERY_TOKEN_LEN],
        resume_secret: [9; RECOVERY_TOKEN_LEN],
        last_recv_seq: 0,
    };
    write_attach_request(&mut writer, &request)
        .await
        .expect("write resume request");
    let response = read_attach_response(&mut reader)
        .await
        .expect("read attach response");

    assert_eq!(response.status, ATTACH_STATUS_REJECTED);

    server.abort();
}

#[tokio::test]
async fn recovery_resume_bad_secret_rejected() {
    let addr = ephemeral_tcp_addr();
    let server = start_recovery_server(&addr).await;
    wait_server_ready(&addr).await;

    let (mut first_reader, mut first_writer) = connect_v3(&addr).await;
    write_attach_request(
        &mut first_writer,
        &AttachRequest {
            mode: ATTACH_MODE_NEW,
            ..AttachRequest::default()
        },
    )
    .await
    .expect("write attach new");
    let first_response = read_attach_response(&mut first_reader)
        .await
        .expect("read first attach response");
    assert_eq!(first_response.status, ATTACH_STATUS_OK);

    let (mut resume_reader, mut resume_writer) = connect_v3(&addr).await;
    let mut bad_secret = first_response.resume_secret;
    bad_secret[0] ^= 0xFF;
    let request = AttachRequest {
        mode: ATTACH_MODE_RESUME,
        connection_id: first_response.connection_id,
        resume_secret: bad_secret,
        last_recv_seq: 0,
    };
    write_attach_request(&mut resume_writer, &request)
        .await
        .expect("write resume request");
    let response = read_attach_response(&mut resume_reader)
        .await
        .expect("read attach response");

    assert_eq!(response.status, ATTACH_STATUS_REJECTED);

    server.abort();
}

#[tokio::test]
async fn recovery_resume_ok_with_valid_tokens() {
    let addr = ephemeral_tcp_addr();
    let server = start_recovery_server(&addr).await;
    wait_server_ready(&addr).await;

    let (mut first_reader, mut first_writer) = connect_v3(&addr).await;
    write_attach_request(
        &mut first_writer,
        &AttachRequest {
            mode: ATTACH_MODE_NEW,
            ..AttachRequest::default()
        },
    )
    .await
    .expect("write attach new");
    let first_response = read_attach_response(&mut first_reader)
        .await
        .expect("read first attach response");
    assert_eq!(first_response.status, ATTACH_STATUS_OK);

    let (mut resume_reader, mut resume_writer) = connect_v3(&addr).await;
    let request = AttachRequest {
        mode: ATTACH_MODE_RESUME,
        connection_id: first_response.connection_id,
        resume_secret: first_response.resume_secret,
        last_recv_seq: 0,
    };
    write_attach_request(&mut resume_writer, &request)
        .await
        .expect("write resume request");
    let response = read_attach_response(&mut resume_reader)
        .await
        .expect("read attach response");

    assert_eq!(response.status, ATTACH_STATUS_OK);
    assert_eq!(response.connection_id, first_response.connection_id);

    server.abort();
}

#[tokio::test]
async fn recovery_queues_send_while_detached() {
    let addr = ephemeral_tcp_addr();
    let server = start_message_recovery_server(&addr).await;
    wait_server_ready(&addr).await;

    let client = crate::Client::new()
        .with_timeout(Duration::from_millis(500))
        .with_recovery(crate::ClientRecoveryOptions {
            enable: true,
            reconnect_min_backoff: Duration::from_millis(5),
            reconnect_max_backoff: Duration::from_millis(20),
            max_replay_bytes: 1 << 20,
        });
    let conn = client
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("client connect");
    conn.set_recv_timeout(Duration::from_secs(2));

    let first: RecoveryEchoReply = conn
        .send_recv(RecoveryEchoRequest {
            text: "first".to_string(),
        })
        .await
        .expect("first send_recv");
    assert_eq!(first.text, "first");

    let before = conn.transport_generation_for_test();
    conn.close_transport_for_test();

    let queued: RecoveryEchoReply = conn
        .send_recv(RecoveryEchoRequest {
            text: "queued".to_string(),
        })
        .await
        .expect("queued send_recv");
    assert_eq!(queued.text, "queued");
    wait_for_generation_change(&conn, before).await;

    server.abort();
}

#[tokio::test]
async fn recovery_replays_reply_after_transport_break() {
    DROP_REPLY_ONCE.store(false, Ordering::Release);

    let addr = ephemeral_tcp_addr();
    let server = start_message_recovery_server(&addr).await;
    wait_server_ready(&addr).await;

    let client = crate::Client::new()
        .with_timeout(Duration::from_millis(500))
        .with_recovery(crate::ClientRecoveryOptions {
            enable: true,
            reconnect_min_backoff: Duration::from_millis(5),
            reconnect_max_backoff: Duration::from_millis(20),
            max_replay_bytes: 1 << 20,
        });
    let conn = client
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("client connect");
    conn.set_recv_timeout(Duration::from_secs(2));

    let replayed: RecoveryEchoReply = conn
        .send_recv(RecoveryDropReplyRequest {
            text: "replayed".to_string(),
        })
        .await
        .expect("send_recv with dropped reply");
    assert_eq!(replayed.text, "replayed");

    let follow_up: RecoveryEchoReply = conn
        .send_recv(RecoveryEchoRequest {
            text: "after".to_string(),
        })
        .await
        .expect("follow-up send_recv");
    assert_eq!(follow_up.text, "after");

    server.abort();
}

#[tokio::test]
async fn recovery_heartbeat_detects_idle_blackhole_and_reconnects() {
    let addr = ephemeral_tcp_addr();
    let server_opts = ServerRecoveryOptions {
        enable: true,
        detached_ttl: Duration::from_secs(2),
        max_replay_bytes: 1 << 20,
        ack_every: 1,
        ack_delay: Duration::from_millis(1),
        heartbeat_interval: Duration::from_millis(20),
        heartbeat_timeout: Duration::from_millis(80),
    };
    let (_blackhole_server, resumed_rx) =
        start_heartbeat_blackhole_server(&addr, server_opts).await;

    let client = crate::Client::new()
        .with_timeout(Duration::from_millis(500))
        .with_recovery(crate::ClientRecoveryOptions {
            enable: true,
            reconnect_min_backoff: Duration::from_millis(5),
            reconnect_max_backoff: Duration::from_millis(20),
            max_replay_bytes: 1 << 20,
        });
    let conn = client
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("client connect");
    let transport_before = conn.transport_generation_for_test();

    tokio::time::timeout(Duration::from_secs(2), resumed_rx)
        .await
        .expect("resume signal timeout")
        .expect("resume signal channel");
    wait_for_generation_change(&conn, transport_before).await;
}

#[tokio::test]
async fn recovery_send_recv_after_reconnect() {
    let addr = ephemeral_tcp_addr();
    let server = start_message_recovery_server(&addr).await;
    wait_server_ready(&addr).await;

    let client = crate::Client::new()
        .with_timeout(Duration::from_millis(500))
        .with_recovery(crate::ClientRecoveryOptions {
            enable: true,
            reconnect_min_backoff: Duration::from_millis(5),
            reconnect_max_backoff: Duration::from_millis(20),
            max_replay_bytes: 1 << 20,
        });
    let conn = client
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("client connect");
    conn.set_recv_timeout(Duration::from_secs(2));

    let first: RecoveryEchoReply = conn
        .send_recv(RecoveryEchoRequest {
            text: "first".to_string(),
        })
        .await
        .expect("first send_recv");
    assert_eq!(first.text, "first");

    let before = conn.transport_generation_for_test();
    conn.close_transport_for_test();
    wait_for_generation_change(&conn, before).await;

    let second: RecoveryEchoReply = conn
        .send_recv(RecoveryEchoRequest {
            text: "second".to_string(),
        })
        .await
        .expect("second send_recv after reconnect");
    assert_eq!(second.text, "second");

    server.abort();
}

#[tokio::test]
async fn recovery_heartbeat_keeps_idle_connection_alive() {
    let addr = ephemeral_tcp_addr();
    let server = start_message_recovery_server(&addr).await;
    wait_server_ready(&addr).await;

    let client = crate::Client::new()
        .with_timeout(Duration::from_millis(500))
        .with_recovery(crate::ClientRecoveryOptions {
            enable: true,
            reconnect_min_backoff: Duration::from_millis(5),
            reconnect_max_backoff: Duration::from_millis(20),
            max_replay_bytes: 1 << 20,
        });
    let conn = client
        .connect(&format!("tcp://{addr}"))
        .await
        .expect("client connect");
    conn.set_recv_timeout(Duration::from_secs(2));

    let gen_before = conn.transport_generation_for_test();

    // Wait 5x the heartbeat interval; heartbeat should keep connection alive
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        conn.transport_generation_for_test(),
        gen_before,
        "heartbeat should keep the same transport alive"
    );

    let reply: RecoveryEchoReply = conn
        .send_recv(RecoveryEchoRequest {
            text: "idle-ok".to_string(),
        })
        .await
        .expect("send_recv after idle");
    assert_eq!(reply.text, "idle-ok");

    server.abort();
}
