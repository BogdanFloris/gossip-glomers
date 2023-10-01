use std::sync::atomic::AtomicUsize;

use anyhow::{Context, Ok};
use async_trait::async_trait;
use gossip_glomers::{event_loop, Event, Init, Node};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    id: AtomicUsize,
    stdout: Mutex<tokio::io::Stdout>,
}

#[async_trait]
impl Node<Payload> for EchoNode {
    fn from_init(
        _init: Init,
        _tx: tokio::sync::mpsc::Sender<Event<Payload>>,
        stdout: Mutex<tokio::io::Stdout>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            id: 1.into(),
            stdout,
        })
    }

    async fn handle(&self, event: gossip_glomers::Event<Payload>) -> anyhow::Result<()> {
        let gossip_glomers::Event::Message(message) = event else {
            panic!("unexpected event: {:?}", event);
        };
        let mut reply = message.into_reply(Some(&self.id));
        match reply.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                reply
                    .send(&self.stdout)
                    .await
                    .context("send response message")?;
            }
            Payload::EchoOk { .. } => {}
        };
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    event_loop::<EchoNode, _, _>().await
}
