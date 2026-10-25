use adaptivemsg_hello_server::message::{HelloReply, HelloRequest};
use adaptivemsg as am;
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
    let client = am::Client::new();
    let conn = client.connect(&args.addr).await?;

    let conn_default = conn.clone();
    let t_default: Task = tokio::spawn(async move {
        let reply: HelloReply = conn_default
            .send_recv(HelloRequest {
                who: "John".into(),
                question: "who are you".into(),
            })
            .await?;
        info!(
            "default stream: {} (trace {})",
            reply.answer, reply.internal.trace_id
        );
        Ok(())
    });

    let conn_a = conn.clone();
    let t1: Task = tokio::spawn(async move {
        let stream = conn_a.new_stream();
        let reply: Result<HelloReply, am::Error> = stream
            .send_recv(HelloRequest {
                who: "Alice".into(),
                question: "how are you".into(),
            })
            .await;
        stream.close();
        let reply = reply?;
        info!(
            "stream A: {} (trace {})",
            reply.answer, reply.internal.trace_id
        );
        Ok(())
    });

    let conn_b = conn.clone();
    let t2: Task = tokio::spawn(async move {
        let stream = conn_b.new_stream();
        let reply: Result<HelloReply, am::Error> = stream
            .send_recv(HelloRequest {
                who: "Bob".into(),
                question: "error please".into(),
            })
            .await;
        stream.close();
        match reply {
            Ok(reply) => info!("stream B: {}", reply.answer),
            Err(am::Error::Remote { code, message }) => {
                warn!("stream B expected error: {code}: {message}");
            }
            Err(err) => return Err(err.into()),
        }
        Ok(())
    });

    let join = |t: Task| async move { t.await? };
    let (r0, r1, r2) = tokio::join!(join(t_default), join(t1), join(t2));
    conn.close().await;
    for result in [r0, r1, r2] {
        if let Err(err) = result {
            warn!("task error: {err}");
            return Err(err);
        }
    }

    Ok(())
}
