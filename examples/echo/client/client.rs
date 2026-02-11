use std::sync::Arc;
use std::time::Duration;

use adaptivemsg::OkReply;
use adaptivemsg_echo_server::message::{
    MessageReply,
    MessageRequest,
    MessageTimeout,
    SubWhoElseEvent,
    WhoElse,
    WhoElseEvent,
    WhoElseReply,
};
use clap::Parser;
use tracing::info;

#[derive(Parser)]
#[command(
    name = "adaptivemsg-echo-client",
    about = "Echo client example for adaptivemsg"
)]
struct Args {
    /// Server address (examples: tcp://127.0.0.1:5560, uds://@adaptivemsg-echo, uds:///tmp/adaptivemsg-echo.sock)
    #[arg(
        short,
        long,
        default_value = "tcp://127.0.0.1:5560",
        help = "Use tcp://HOST:PORT for TCP, uds://@adaptivemsg-* for abstract UDS, or uds:///tmp/adaptivemsg-*.sock for a filesystem socket"
    )]
    addr: String,
    /// Demo command: echo (default), timeout, or whoelse
    #[arg(default_value = "echo")]
    cmd: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let client = adaptivemsg::Client::new();
    let conn = client.connect(&args.addr).await?;

    match args.cmd.as_str() {
        "timeout" => timeout_demo(&conn).await?,
        "whoelse" => whoelse_demo(&conn).await?,
        _ => echo_demo(&conn, &args.addr).await?,
    }

    Ok(())
}

async fn timeout_demo(conn: &adaptivemsg::Connection) -> anyhow::Result<()> {
    let stream = conn.new_stream();

    info!("No timeout by default");
    stream.send_recv(MessageTimeout { secs: 10 }).await?;
    info!("Recv OK");

    info!("Set timeout to 15s");
    stream.set_recv_timeout(Duration::from_secs(15));
    stream.send_recv::<_, OkReply>(MessageTimeout { secs: 10 }).await?;
    info!("Recv OK");

    info!("Set timeout to 3s");
    stream.set_recv_timeout(Duration::from_secs(3));
    if let Err(err) = stream.send_recv::<_, OkReply>(MessageTimeout { secs: 10 }).await {
        info!("Recv timeout: {err}");
    } else {
        info!("Some unexpected error happened");
    }

    info!("Set back to no timeout");
    stream.set_recv_timeout(Duration::ZERO);
    stream.send_recv(MessageTimeout { secs: 10 }).await?;
    info!("Recv OK");
    Ok(())
}

async fn whoelse_demo(conn: &adaptivemsg::Connection) -> anyhow::Result<()> {
    let event_stream = conn.new_stream();
    event_stream.send(SubWhoElseEvent {}).await?;

    let event_task = tokio::spawn(async move {
        loop {
            let evt: WhoElseEvent = event_stream.recv().await?;
            info!("event: new client {}", evt.addr);
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    });

    for _ in 0..100 {
        let rep: WhoElseReply = conn.send_recv(WhoElse {}).await?;
        info!("clients: {}", rep.clients);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    event_task.abort();
    Ok(())
}

async fn echo_demo(conn: &adaptivemsg::Connection, addr: &str) -> anyhow::Result<()> {
    let msg = "ni hao".to_string();
    let mut num = 0;

    for _ in 0..5 {
        num += 100;
        let req = MessageRequest {
            msg: msg.clone(),
            num,
        };
        let rep: MessageReply = conn.send_recv(req).await?;
        info!("{}:{} ==> {}:{}, {}", msg, num, rep.msg, rep.num, rep.signature);
    }

    concurrent_demo(addr).await
}

async fn concurrent_demo(addr: &str) -> anyhow::Result<()> {
    let mut client_tasks = Vec::new();
    for _ in 0..6 {
        let addr = addr.to_string();
        client_tasks.push(tokio::spawn(async move {
            let client = adaptivemsg::Client::new();
            let conn = client.connect(&addr).await?;
            let conn = Arc::new(conn);

            let mut stream_tasks = Vec::new();
            for i in 0..10 {
                let conn = conn.clone();
                stream_tasks.push(tokio::spawn(async move {
                    let stream = conn.new_stream();
                    let msg = "ni hao".to_string();
                    let mut num = 100 * (i as i32);
                    for _ in 0..9 {
                        num += 10;
                        let req = MessageRequest {
                            msg: msg.clone(),
                            num,
                        };
                        let rep: MessageReply = stream.send_recv(req).await?;
                        if num + 1 != rep.num {
                            anyhow::bail!("wrong number: expected {}, got {}", num + 1, rep.num);
                        }
                    }
                    Ok::<_, anyhow::Error>(())
                }));
            }

            for task in stream_tasks {
                task.await??;
            }
            Ok(())
        }));
    }

    for task in client_tasks {
        task.await??;
    }
    Ok(())
}
