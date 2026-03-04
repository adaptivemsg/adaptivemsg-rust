use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use adaptivemsg_echo_server::state::StatMgr;
use adaptivemsg as am;
use clap::Parser;
use tracing::info;

#[derive(Parser)]
#[command(
    name = "adaptivemsg-echo-server",
    about = "Echo server example for adaptivemsg"
)]
struct Args {
    /// Bind address (examples: tcp://127.0.0.1:5560, uds://@adaptivemsg-echo, uds:///tmp/adaptivemsg-echo.sock)
    #[arg(
        short,
        long,
        default_value = "tcp://127.0.0.1:5560",
        help = "Use tcp://HOST:PORT for TCP, uds://@adaptivemsg-* for abstract UDS, or uds:///tmp/adaptivemsg-*.sock for a filesystem socket"
    )]
    addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let mgr = Arc::new(StatMgr::new());
    let stream_seq = Arc::new(AtomicU64::new(1));

    let server = am::Server::new()
        .on_connect({
            let mgr = Arc::clone(&mgr);
            move |netconn| {
                let addr = netconn.peer_addr().unwrap_or("client-unknown");
                mgr.on_connect(addr);
                info!("connect: {}", addr);
                Ok(())
            }
        })
        .on_disconnect({
            let mgr = Arc::clone(&mgr);
            move |netconn| {
                let addr = netconn.peer_addr().unwrap_or("client-unknown");
                mgr.on_disconnect(addr);
                info!("disconnect: {}", addr);
                Ok(())
            }
        })
        .on_new_stream({
            let mgr = Arc::clone(&mgr);
            let stream_seq = Arc::clone(&stream_seq);
            move |ctx| {
                ctx.set_context(Arc::clone(&mgr));
                let id = stream_seq.fetch_add(1, Ordering::Relaxed);
                info!("on new stream: {}", id);
            }
        });

    info!("echo server listening on {}", args.addr);
    server.serve(&args.addr).await?;
    Ok(())
}
