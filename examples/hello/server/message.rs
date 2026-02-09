use adaptivemsg::{ContextStream, Message, MessageHandler, Result};

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
    async fn handle(
        self: Box<Self>,
        _ctxstream: ContextStream,
    ) -> Result<Option<Box<dyn Message>>> {
        let question = self.question.to_lowercase();
        if question.contains("error") {
            return Err(anyhow::anyhow!("bad request: {question}"));
        }
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
    }
}
