use clap::Parser;

#[derive(Parser)]
#[command(
    name = "adaptivemsg-hello-server",
    about = "Hello server example for adaptivemsg"
)]
struct Args {
    /// Bind address (examples: tcp://127.0.0.1:5555, uds://@adaptivemsg-hello, uds:///tmp/adaptivemsg-hello.sock)
    #[arg(
        short,
        long,
        default_value = "tcp://127.0.0.1:5555",
        help = "Use tcp://HOST:PORT for TCP, uds://@adaptivemsg-* for abstract UDS, or uds:///tmp/adaptivemsg-*.sock for a filesystem socket"
    )]
    addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    adaptivemsg::Server::new().serve(&args.addr).await?;
    Ok(())
}
