use adaptivemsg::{KnownMessage, Message, StreamContext};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct HelloRequest {
    pub who: String,
    pub question: String,
}

#[derive(Serialize, Deserialize)]
pub struct HelloReply {
    pub answer: String,
}

#[typetag::serde]
impl Message for HelloRequest {}

#[typetag::serde]
impl Message for HelloReply {}

impl KnownMessage for HelloRequest {
    fn handle(
        self: Box<Self>,
        ctx: StreamContext,
    ) -> BoxFuture<'static, Result<Option<Box<dyn Message>>, adaptivemsg::Error>> {
        Box::pin(async move {
            let session_id = ctx
                .stream
                .get_context::<SessionInfo>()
                .map(|info| info.id)
                .unwrap_or(0);
            let question = self.question.to_lowercase();
            let answer = if question.contains("who are you") {
                "I am hello server"
            } else if question.contains("how are you") {
                "I am good"
            } else {
                "I don't know"
            };
            let reply = HelloReply {
                answer: format!("{answer}, {} (session {session_id})", self.who),
            };
            Ok(Some(Box::new(reply)))
        })
    }
}

pub struct SessionInfo {
    pub id: u64,
}
