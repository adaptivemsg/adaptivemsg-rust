use std::sync::Arc;

use adaptivemsg::{Handler, Registry, RequestCtx, WorkerConfig};
use futures::future::BoxFuture;

mod message;
use message::{HelloReply, HelloRequest};

struct HelloHandler;

impl Handler for HelloHandler {
    fn handle(
        &self,
        msg: Box<dyn adaptivemsg::Message>,
        _ctx: RequestCtx,
    ) -> BoxFuture<'static, Result<Option<Box<dyn adaptivemsg::Message>>, adaptivemsg::Error>> {
        Box::pin(async move {
            let req = msg.as_any().downcast_ref::<HelloRequest>().unwrap();
            let question = req.question.to_lowercase();
            let answer = if question.contains("who are you") {
                "I am hello server"
            } else if question.contains("how are you") {
                "I am good"
            } else {
                "I don't know"
            };
            let reply = HelloReply {
                answer: format!("{answer}, {}", req.who),
            };
            Ok(Some(Box::new(reply)))
        })
    }
}

fn registry() -> Registry {
    let mut reg = Registry::new();
    reg.register("hello.request", Arc::new(HelloHandler));
    reg
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let path = "/tmp/adaptivemsg.sock";
    let _ = std::fs::remove_file(path);

    let reg = registry();
    let worker_cfg = WorkerConfig::default();
    let listener = adaptivemsg::transport::uds::listen(path).await?;
    let _conn = adaptivemsg::transport::uds::accept(&listener, Some(reg), Some(worker_cfg)).await?;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
