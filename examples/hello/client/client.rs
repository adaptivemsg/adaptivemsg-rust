use adaptivemsg_hello_server::message::{HelloReply, HelloRequest};
use clap::Parser;
use tracing::{info, warn};

type Task = tokio::task::JoinHandle<anyhow::Result<()>>;

#[derive(Parser)]
#[command(
    name = "adaptivemsg-hello-client",
    about = "Hello client example for adaptivemsg"
)]
struct Args {
    /// Server address (examples: tcp://127.0.0.1:5555, uds://@adaptivemsg-hello, uds:///tmp/adaptivemsg-hello.sock)
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
    let client = adaptivemsg::Client::new();
    let conn = client.connect(&args.addr).await?;

    let stream_a = conn.new_stream();
    let stream_b = conn.new_stream();

    let t_default: Task = tokio::spawn(async move {
        let reply: HelloReply = conn
            .send_recv(HelloRequest {
                who: "John".into(),
                question: "who are you".into(),
            })
            .await?;
        info!("default stream: {}", reply.answer);
        Ok(())
    });

    let t1: Task = tokio::spawn(async move {
        let reply: HelloReply = stream_a
            .send_recv(HelloRequest {
                who: "Alice".into(),
                question: "how are you".into(),
            })
            .await?;
        info!("stream A: {}", reply.answer);
        Ok(())
    });

    let t2: Task = tokio::spawn(async move {
        let reply: HelloReply = stream_b
            .send_recv(HelloRequest {
                who: "Bob".into(),
                question: "error please".into(),
            })
            .await?;
        info!("stream B: {}", reply.answer);
        Ok(())
    });

    let join = |t: Task| async move { t.await? };
    let (r0, r1, r2) = tokio::join!(join(t_default), join(t1), join(t2));
    for result in [r0, r1, r2] {
        if let Err(err) = result {
            warn!("task error: {err}");
            return Err(err);
        }
    }

    Ok(())
}
