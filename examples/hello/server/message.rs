use adaptivemsg::{Message, MessageHandler, StreamContext};
use futures::future::BoxFuture;

#[adaptivemsg::message]
pub struct HelloRequest {
    pub who: String,
    pub question: String,
}

#[adaptivemsg::message]
pub struct HelloReply {
    pub answer: String,
}

#[adaptivemsg::message_handler]
impl MessageHandler for HelloRequest {
    fn handle(
        self: Box<Self>,
        _ctx: StreamContext,
    ) -> BoxFuture<'static, Result<Option<Box<dyn Message>>, adaptivemsg::Error>> {
        Box::pin(async move {
            let question = self.question.to_lowercase();
            let answer = if question.contains("who are you") {
                "I am hello server"
            } else if question.contains("how are you") {
                "I am good"
            } else {
                "I don't know"
            };
            let reply = HelloReply {
                answer: format!("{answer}, {}", self.who),
            };
            Ok(Some(Box::new(reply)))
        })
    }
}
