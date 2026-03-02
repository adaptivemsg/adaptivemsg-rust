use std::time::Duration;

use adaptivemsg::{HandlerStream, Message, MessageHandler, Result};
use anyhow::anyhow;
use tokio::sync::mpsc;

use crate::state::StatMgr;

#[adaptivemsg::message]
pub struct MessageRequest {
    pub msg: String,
    pub num: i32,
}

#[adaptivemsg::message]
pub struct MessageReply {
    pub msg: String,
    pub num: i32,
    pub signature: String,
}

#[adaptivemsg::message_handler]
impl MessageHandler for MessageRequest {
    async fn handle(mut self: Box<Self>, stream: HandlerStream) -> Result<Option<Box<dyn Message>>> {
        let mgr = stream
            .get_context::<StatMgr>()
            .ok_or_else(|| anyhow!("missing StatMgr context"))?;
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

#[adaptivemsg::message]
pub struct SubWhoElseEvent {}

#[adaptivemsg::message]
pub struct WhoElseEvent {
    pub addr: String,
}

#[adaptivemsg::message_handler]
impl MessageHandler for SubWhoElseEvent {
    async fn handle(self: Box<Self>, stream: HandlerStream) -> Result<Option<Box<dyn Message>>> {
        let mgr = stream
            .get_context::<StatMgr>()
            .ok_or_else(|| anyhow!("missing StatMgr context"))?;
        let (tx, mut rx) = mpsc::channel::<String>(8);
        let sub_id = mgr.add_subscriber(tx);
        stream.new_task(|stream| async move {
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

#[adaptivemsg::message]
pub struct WhoElse {}

#[adaptivemsg::message]
pub struct WhoElseReply {
    pub clients: String,
}

#[adaptivemsg::message_handler]
impl MessageHandler for WhoElse {
    async fn handle(self: Box<Self>, stream: HandlerStream) -> Result<Option<Box<dyn Message>>> {
        let mgr = stream
            .get_context::<StatMgr>()
            .ok_or_else(|| anyhow!("missing StatMgr context"))?;
        let reply = WhoElseReply {
            clients: mgr.list_clients(),
        };
        Ok(Some(Box::new(reply)))
    }
}

#[adaptivemsg::message]
pub struct MessageTimeout {
    pub secs: u64,
}

#[adaptivemsg::message_handler]
impl MessageHandler for MessageTimeout {
    async fn handle(self: Box<Self>, _stream: HandlerStream) -> Result<Option<Box<dyn Message>>> {
        tokio::time::sleep(Duration::from_secs(self.secs)).await;
        Ok(None)
    }
}
