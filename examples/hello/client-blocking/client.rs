use std::thread;

use adaptivemsg as am;
use adaptivemsg_hello_server::message::{HelloReply, HelloRequest};
use am::blocking::Client;
use clap::Parser;
use tracing::{info, warn};

#[derive(Parser)]
#[command(
    name = "adaptivemsg-hello-client-blocking",
    about = "Hello client example for adaptivemsg, without tokio"
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
    /// Negotiate protocol v3, which reconnects and replays after a transport
    /// failure, if the server supports it
    #[arg(long)]
    recovery: bool,
}

fn ask(who: &str, question: &str) -> HelloRequest {
    HelloRequest {
        who: who.into(),
        question: question.into(),
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let client = Client::new().with_recovery(am::ClientRecoveryOptions {
        enable: args.recovery,
        ..am::ClientRecoveryOptions::default()
    });
    let conn = client.connect(&args.addr)?;
    info!("protocol v{}", conn.debug_state().protocol);

    let reply: HelloReply = conn.send_recv(ask("John", "who are you"))?;
    info!(
        "default stream: {} (trace {})",
        reply.answer, reply.internal.trace_id
    );

    let result = thread::scope(|scope| {
        let a = scope.spawn(|| -> anyhow::Result<()> {
            let stream = conn.new_stream();
            let reply = stream.send_recv::<HelloReply>(ask("Alice", "how are you"));
            stream.close();
            let reply = reply?;
            info!(
                "stream A: {} (trace {})",
                reply.answer, reply.internal.trace_id
            );
            Ok(())
        });
        let b = scope.spawn(|| -> anyhow::Result<()> {
            let stream = conn.new_stream();
            let reply = stream.send_recv::<HelloReply>(ask("Bob", "error please"));
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
        [a.join(), b.join()]
            .into_iter()
            .map(|joined| joined.unwrap_or_else(|_| Err(anyhow::anyhow!("stream thread panicked"))))
            .collect::<anyhow::Result<()>>()
    });
    conn.close();
    result
}
