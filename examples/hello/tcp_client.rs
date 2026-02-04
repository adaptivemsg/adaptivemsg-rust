mod message;
use message::{HelloReply, HelloRequest};

type Task = tokio::task::JoinHandle<anyhow::Result<()>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let conn = adaptivemsg::transport::tcp::connect("127.0.0.1:5555").await?;

    let stream_a = conn.new_stream();
    let stream_b = conn.new_stream();

    let t_default: Task = tokio::spawn(async move {
        let reply: HelloReply = conn
            .send_recv(HelloRequest {
                who: "John".into(),
                question: "who are you".into(),
            })
            .await?;
        println!("default stream: {}", reply.answer);
        Ok(())
    });

    let t1: Task = tokio::spawn(async move {
        let reply: HelloReply = stream_a
            .send_recv(HelloRequest {
                who: "Alice".into(),
                question: "how are you".into(),
            })
            .await?;
        println!("stream A: {}", reply.answer);
        Ok(())
    });

    let t2: Task = tokio::spawn(async move {
        let reply: HelloReply = stream_b
            .send_recv(HelloRequest {
                who: "Bob".into(),
                question: "who are you".into(),
            })
            .await?;
        println!("stream B: {}", reply.answer);
        Ok(())
    });

    let join = |t: Task| async move { t.await?? };
    tokio::try_join!(join(t_default), join(t1), join(t2))?;

    Ok(())
}
