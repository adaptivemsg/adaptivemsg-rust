mod message;
use message::{HelloReply, HelloRequest};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let conn = adaptivemsg::transport::uds::connect("/tmp/adaptivemsg.sock").await?;

    let reply: HelloReply = conn
        .send_recv(HelloRequest {
            who: "John".into(),
            question: "who are you".into(),
        })
        .await?;

    println!("{}", reply.answer);
    Ok(())
}
