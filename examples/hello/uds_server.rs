use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use adaptivemsg::{Registry, Server, WorkerConfig};

mod message;
use message::{HelloRequest, SessionInfo};

fn registry() -> Registry {
    let mut reg = Registry::new();
    reg.register_known::<HelloRequest>();
    reg
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let path = "uds://@adaptivemsg";

    let reg = registry();
    let worker_cfg = WorkerConfig::default();

    struct ServerState {
        session_seq: AtomicU64,
        conn_seq: AtomicU64,
    }

    struct ConnState {
        server: Arc<ServerState>,
        id: u64,
    }

    let server_state = Arc::new(ServerState {
        session_seq: AtomicU64::new(0),
        conn_seq: AtomicU64::new(0),
    });

    let server = Server::new(reg)
        .with_worker_config(worker_cfg)
        .on_connect({
            let server_state = Arc::clone(&server_state);
            move |_conn| {
                let id = server_state.conn_seq.fetch_add(1, Ordering::Relaxed) + 1;
                Arc::new(ConnState {
                    server: Arc::clone(&server_state),
                    id,
                })
            }
        })
        .on_disconnect(|conn_ctx| {
            if let Ok(ctx) = Arc::clone(conn_ctx).downcast::<ConnState>() {
                println!("disconnect: conn {}", ctx.id);
            }
        })
        .on_new_stream(|stream, conn_ctx| {
            if let Ok(ctx) = Arc::clone(conn_ctx).downcast::<ConnState>() {
                let id = ctx.server.session_seq.fetch_add(1, Ordering::Relaxed) + 1;
                stream.set_context(Arc::new(SessionInfo { id }));
            }
        })
        .on_stream_close(|stream, _conn_ctx| {
            let _ = stream.get_context::<SessionInfo>();
        });

    server.serve(path).await?;
    Ok(())
}
