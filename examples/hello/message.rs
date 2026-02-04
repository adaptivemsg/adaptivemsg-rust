use adaptivemsg::Message;
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
impl Message for HelloRequest {
    fn type_name(&self) -> &'static str {
        "hello.request"
    }
}

#[typetag::serde]
impl Message for HelloReply {
    fn type_name(&self) -> &'static str {
        "hello.reply"
    }
}
