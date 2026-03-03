use std::time::Duration;

use adaptivemsg as am;
use am::{HandlerStream, Message, MessageHandler, Result};
use anyhow::anyhow;
use tokio::sync::mpsc;

use crate::state::StreamContext;

#[am::message]
pub struct MessageRequest {
    pub msg: String,
    pub num: i32,
}

#[am::message]
pub struct MessageReply {
    pub msg: String,
    pub num: i32,
    pub signature: String,
}

#[am::message_handler]
impl MessageHandler for MessageRequest {
    async fn handle(mut self: Box<Self>, stream: HandlerStream) -> Result<Option<Box<dyn Message>>> {
        let ctx = stream
            .get_context::<StreamContext>()
            .ok_or_else(|| anyhow!("missing StreamContext"))?;
        let mgr = ctx.mgr();
        mgr.inc_counter();
        self.msg.push('!');
        self.num += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let signature = format!("yours echo.v1.0 from {}", stream.id());
        let reply = MessageReply {
            msg: self.msg.clone(),
            num: self.num,
            signature,
        };
        Ok(Some(Box::new(reply)))
    }
}

#[am::message]
pub struct SubWhoElseEvent {}

#[am::message]
pub struct WhoElseEvent {
    pub addr: String,
}

#[am::message_handler]
impl MessageHandler for SubWhoElseEvent {
    async fn handle(self: Box<Self>, stream: HandlerStream) -> Result<Option<Box<dyn Message>>> {
        let ctx = stream
            .get_context::<StreamContext>()
            .ok_or_else(|| anyhow!("missing StreamContext"))?;
        let mgr = ctx.mgr();
        let (tx, mut rx) = mpsc::channel::<String>(8);
        let sub_id = mgr.add_subscriber(tx);
        ctx.set_subscriber(sub_id);
        stream.new_task(move |stream| async move {
            while let Some(addr) = rx.recv().await {
                let msg = WhoElseEvent { addr };
                if stream.send(msg).await.is_err() {
                    mgr.remove_subscriber(sub_id);
                    break;
                }
            }
        });
        Ok(None)
    }
}

#[am::message]
pub struct WhoElse {}

#[am::message]
pub struct WhoElseReply {
    pub clients: String,
}

#[am::message_handler]
impl MessageHandler for WhoElse {
    async fn handle(self: Box<Self>, stream: HandlerStream) -> Result<Option<Box<dyn Message>>> {
        let ctx = stream
            .get_context::<StreamContext>()
            .ok_or_else(|| anyhow!("missing StreamContext"))?;
        let mgr = ctx.mgr();
        let reply = WhoElseReply {
            clients: mgr.list_clients(),
        };
        Ok(Some(Box::new(reply)))
    }
}

#[am::message]
pub struct MessageTimeout {
    pub secs: u64,
}

#[am::message_handler]
impl MessageHandler for MessageTimeout {
    async fn handle(self: Box<Self>, _stream: HandlerStream) -> Result<Option<Box<dyn Message>>> {
        tokio::time::sleep(Duration::from_secs(self.secs)).await;
        Ok(None)
    }
}
