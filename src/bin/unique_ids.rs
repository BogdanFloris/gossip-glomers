use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Ok};
use async_trait::async_trait;
use gossip_glomers::{event_loop, Event, Init, Node};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

struct UniqueIdsNode {
    node: String,
    id: AtomicUsize,
}

#[async_trait]
impl Node<Payload> for UniqueIdsNode {
    fn from_init(init: Init, _tx: tokio::sync::mpsc::Sender<Event<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            node: init.node_id,
            id: 1.into(),
        })
    }

    async fn handle(
        &self,
        event: gossip_glomers::Event<Payload>,
        output: &mut tokio::io::Stdout,
    ) -> anyhow::Result<()> {
        let gossip_glomers::Event::Message(message) = event else {
            panic!("unexpected event: {:?}", event);
        };
        let mut reply = message.into_reply(Some(&self.id));
        match reply.body.payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node, self.id.load(Ordering::SeqCst));
                reply.body.payload = Payload::GenerateOk { guid };
                reply.send(output).await.context("send response message")?;
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    event_loop::<UniqueIdsNode, _, _>().await
}
