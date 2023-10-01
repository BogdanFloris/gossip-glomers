use std::{collections::HashMap, sync::atomic::AtomicUsize};

use anyhow::{Context, Ok};
use async_trait::async_trait;
use gossip_glomers::{event_loop, Event, Init, Node};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        msg: u64,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<Vec<u64>>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InjectedPayload {}

struct KafkaNode {
    id: AtomicUsize,
    stdout: Mutex<tokio::io::Stdout>,
}

#[async_trait]
impl Node<Payload, InjectedPayload> for KafkaNode {
    fn from_init(
        _init: Init,
        _tx: tokio::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
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

    async fn handle(
        &self,
        event: gossip_glomers::Event<Payload, InjectedPayload>,
    ) -> anyhow::Result<()> {
        match event {
            gossip_glomers::Event::EOF => {}
            gossip_glomers::Event::Message(message) => {
                let mut reply = message.into_reply(Some(&self.id));
                match reply.body.payload {
                    Payload::Send { .. } => {
                        reply.body.payload = Payload::SendOk { offset: 0 };
                        reply
                            .send(&self.stdout)
                            .await
                            .context("send send ok response")?;
                    }
                    Payload::Poll { .. } => {
                        reply.body.payload = Payload::PollOk {
                            msgs: HashMap::new(),
                        };
                        reply
                            .send(&self.stdout)
                            .await
                            .context("send poll ok response")?;
                    }
                    Payload::CommitOffsets { .. } => {
                        reply.body.payload = Payload::CommitOffsetsOk;
                        reply
                            .send(&self.stdout)
                            .await
                            .context("send commit offsets ok response")?;
                    }
                    Payload::ListCommittedOffsets { .. } => {
                        reply.body.payload = Payload::ListCommittedOffsetsOk {
                            offsets: HashMap::new(),
                        };
                        reply
                            .send(&self.stdout)
                            .await
                            .context("send list commit offsets ok response")?;
                    }
                    Payload::ListCommittedOffsetsOk { .. }
                    | Payload::CommitOffsetsOk
                    | Payload::PollOk { .. }
                    | Payload::SendOk { .. } => {}
                }
            }
            gossip_glomers::Event::Injected(_) => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    event_loop::<KafkaNode, _, _>().await
}
