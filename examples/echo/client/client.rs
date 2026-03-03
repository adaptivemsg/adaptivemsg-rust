use std::time::Duration;

use adaptivemsg as am;
use am::OkReply;
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
use futures::future::try_join_all;
use tracing::{info, warn};

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
    /// Demo command: echo (default), timeout, whoelse (query), or whoelse_sub (subscribe)
    #[arg(default_value = "echo")]
    cmd: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let client = am::Client::new();
    let conn = client.connect(&args.addr).await?;

    match args.cmd.as_str() {
        "timeout" => timeout_demo(&conn).await?,
        "whoelse" => whoelse_query_demo(&conn).await?,
        "whoelse_sub" => {
            let event_task = whoelse_subscribe_demo(&conn).await?;
            match event_task.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => warn!("event task failed: {err}"),
                Err(err) if err.is_cancelled() => {}
                Err(err) => warn!("event task join error: {err}"),
            }
        }
        _ => echo_demo(&conn, &args.addr).await?,
    }

    Ok(())
}

async fn timeout_demo(conn: &am::Connection) -> anyhow::Result<()> {
    let stream = conn.new_stream();

    info!("No timeout by default");
    let _: OkReply = stream.send_recv(MessageTimeout { secs: 10 }).await?;
    info!("Recv OK");

    info!("Set timeout to 15s");
    stream.set_recv_timeout(Duration::from_secs(15));
    let _: OkReply = stream.send_recv(MessageTimeout { secs: 10 }).await?;
    info!("Recv OK");

    info!("Set timeout to 3s");
    stream.set_recv_timeout(Duration::from_secs(3));
    let result: Result<OkReply, _> = stream.send_recv(MessageTimeout { secs: 10 }).await;
    if let Err(err) = result {
        info!("Recv timeout: {err}");
    } else {
        info!("Some unexpected error happened");
    }

    info!("Set back to no timeout");
    stream.set_recv_timeout(Duration::ZERO);
    let _: OkReply = stream.send_recv(MessageTimeout { secs: 10 }).await?;
    info!("Recv OK");
    Ok(())
}

async fn whoelse_subscribe_demo(
    conn: &am::Connection,
) -> anyhow::Result<tokio::task::JoinHandle<anyhow::Result<()>>> {
    let event_stream = conn.new_stream();
    let _: OkReply = event_stream.send_recv(SubWhoElseEvent {}).await?;

    let event_task = tokio::spawn(async move {
        loop {
            match event_stream.recv::<WhoElseEvent>().await {
                Ok(evt) => info!("event: new client {}", evt.addr),
                Err(err) => {
                    warn!("event recv error: {err}");
                    return Err(err.into());
                }
            }
        }
    });
    Ok(event_task)
}

async fn whoelse_query_demo(conn: &am::Connection) -> anyhow::Result<()> {
    for _ in 0..100 {
        let rep: WhoElseReply = conn.send_recv(WhoElse {}).await?;
        info!("clients: {}", rep.clients);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Ok(())
}

async fn echo_demo(conn: &am::Connection, addr: &str) -> anyhow::Result<()> {
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
    let mut client_tasks: Vec<tokio::task::JoinHandle<anyhow::Result<()>>> = Vec::new();
    for _ in 0..6 {
        let addr = addr.to_string();
        client_tasks.push(tokio::spawn(async move {
            let client = am::Client::new();
            let conn = client.connect(&addr).await?;

            let mut stream_tasks: Vec<tokio::task::JoinHandle<anyhow::Result<()>>> = Vec::new();
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

            for result in try_join_all(stream_tasks).await? {
                result?;
            }
            Ok(())
        }));
    }

    for result in try_join_all(client_tasks).await? {
        result?;
    }
    Ok(())
}
