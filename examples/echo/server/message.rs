use std::time::Duration;

use adaptivemsg as am;
use am::{Message, MessageHandler, Result, StreamContext};
use anyhow::anyhow;
use tokio::sync::broadcast;

use crate::state::StatMgr;

// The names match the Go echo example, whose package is echo.
#[am::message(name = "echo.MessageRequest")]
pub struct MessageRequest {
    pub msg: String,
    pub num: i32,
}

#[am::message(name = "echo.MessageReply")]
pub struct MessageReply {
    pub msg: String,
    pub num: i32,
    pub signature: String,
}

#[am::message_handler]
impl MessageHandler for MessageRequest {
    async fn handle(
        mut self: Box<Self>,
        stream_ctx: StreamContext,
    ) -> Result<Option<Box<dyn Message>>> {
        let mgr = stream_ctx
            .get_context::<StatMgr>()
            .ok_or_else(|| anyhow!("missing stream context"))?;
        mgr.inc_counter();
        self.msg.push('!');
        self.num += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let signature = "yours echo.v1.0".to_string();
        let reply = MessageReply {
            msg: self.msg.clone(),
            num: self.num,
            signature,
        };
        Ok(Some(Box::new(reply)))
    }
}

#[am::message(name = "echo.SubWhoElseEvent")]
pub struct SubWhoElseEvent {}

#[am::message(name = "echo.WhoElseEvent")]
pub struct WhoElseEvent {
    pub addr: String,
}

#[am::message_handler]
impl MessageHandler for SubWhoElseEvent {
    async fn handle(
        self: Box<Self>,
        stream_ctx: StreamContext,
    ) -> Result<Option<Box<dyn Message>>> {
        let mgr = stream_ctx
            .get_context::<StatMgr>()
            .ok_or_else(|| anyhow!("missing stream context"))?;
        let mut rx = mgr.subscribe();
        let _task = stream_ctx.new_task(move |stream| async move {
            loop {
                let addr = match rx.recv().await {
                    Ok(addr) => addr,
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                };
                let msg = WhoElseEvent { addr };
                if stream.send(msg).await.is_err() {
                    break;
                }
            }
        })?;
        Ok(None)
    }
}

#[am::message(name = "echo.WhoElse")]
pub struct WhoElse {}

#[am::message(name = "echo.WhoElseReply")]
pub struct WhoElseReply {
    pub clients: String,
}

#[am::message_handler]
impl MessageHandler for WhoElse {
    async fn handle(self: Box<Self>, stream_ctx: StreamContext) -> Result<Option<Box<dyn Message>>> {
        let mgr = stream_ctx
            .get_context::<StatMgr>()
            .ok_or_else(|| anyhow!("missing stream context"))?;
        let reply = WhoElseReply {
            clients: mgr.list_clients(),
        };
        Ok(Some(Box::new(reply)))
    }
}

#[am::message(name = "echo.MessageTimeout")]
pub struct MessageTimeout {
    pub secs: u64,
}

#[am::message_handler]
impl MessageHandler for MessageTimeout {
    async fn handle(self: Box<Self>, _stream_ctx: StreamContext) -> Result<Option<Box<dyn Message>>> {
        tokio::time::sleep(Duration::from_secs(self.secs)).await;
        Ok(None)
    }
}
