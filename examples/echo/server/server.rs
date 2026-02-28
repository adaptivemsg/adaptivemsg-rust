use std::sync::Arc;

use adaptivemsg_echo_server::state::StatMgr;
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

    let server = adaptivemsg::Server::new()
        .on_connect({
            let mgr = mgr.clone();
            move |conn| {
                let addr = conn
                    .peer_addr()
                    .unwrap_or_else(|| "client-unknown".to_string());
                mgr.on_connect(&addr);
                info!("connect: {}", addr);
                Ok(())
            }
        })
        .on_disconnect({
            let mgr = mgr.clone();
            move |conn| {
                let addr = conn
                    .peer_addr()
                    .unwrap_or_else(|| "client-unknown".to_string());
                mgr.on_disconnect(&addr);
                info!("disconnect: {}", addr);
                Ok(())
            }
        })
        .on_new_stream({
            let mgr = mgr.clone();
            move |stream| {
                stream.set_context(mgr.clone());
                info!("on new stream {}", stream.id());
            }
        });

    info!("echo server listening on {}", args.addr);
    server.serve(&args.addr).await?;
    Ok(())
}
