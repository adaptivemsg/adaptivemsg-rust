mod message;
use message::{HelloReply, HelloRequest};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = adaptivemsg::Client::new();
    let conn = client.connect("uds://@adaptivemsg").await?;

    let reply: HelloReply = conn
        .send_recv(HelloRequest {
            who: "John".into(),
            question: "who are you".into(),
        })
        .await?;

    println!("{}", reply.answer);
    Ok(())
}
