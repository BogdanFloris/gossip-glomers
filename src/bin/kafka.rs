use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use anyhow::{Context, Ok};
use async_trait::async_trait;
use gossip_glomers::{event_loop, Body, Event, Init, Message, Node};
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
    Echo {
        msg: String,
    },
    EchoOk {
        msg: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InjectedPayload {}

struct KafkaNode {
    id: AtomicUsize,
    node: String,
    nodes: Vec<String>,
    stdout: Mutex<tokio::io::Stdout>,
    rpc: Mutex<HashMap<usize, tokio::sync::oneshot::Sender<Message<Payload>>>>,
}
impl KafkaNode {
    async fn rpc(&self, to: &String, payload: Payload) -> anyhow::Result<Message<Payload>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Message {
            src: self.node.clone(),
            dest: to.to_string(),
            body: Body {
                id: self.id.fetch_add(1, Ordering::SeqCst).into(),
                in_reply_to: None,
                payload,
            },
        };
        self.rpc.lock().await.insert(msg.body.id.unwrap(), tx);
        msg.send(&self.stdout).await.context("send rpc message")?;
        rx.await.context("receive rpc response")
    }
}

#[async_trait]
impl Node<Payload, InjectedPayload> for KafkaNode {
    fn from_init(
        init: Init,
        _tx: tokio::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
        stdout: Mutex<tokio::io::Stdout>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let id = AtomicUsize::new(1);

        Ok(Self {
            id,
            node: init.node_id,
            nodes: init.node_ids,
            stdout,
            rpc: Mutex::new(HashMap::new()),
        })
    }

    async fn handle(
        &self,
        event: gossip_glomers::Event<Payload, InjectedPayload>,
    ) -> anyhow::Result<()> {
        match event {
            gossip_glomers::Event::EOF => {}
            gossip_glomers::Event::Message(message) => {
                // Handle RPC responses
                if message.body.in_reply_to.is_some() {
                    let id = message.body.in_reply_to.unwrap();
                    let tx = self.rpc.lock().await.remove(&id).unwrap();
                    if let Err(_) = tx.send(message.clone()) {
                        anyhow::bail!("rpc response channel closed");
                    }
                    return Ok(());
                }

                let mut reply = message.into_reply(Some(&self.id));
                match reply.body.payload {
                    Payload::Echo { .. } => {
                        reply.body.payload = Payload::EchoOk {
                            msg: format!("copy from {}", self.node),
                        };
                        reply
                            .send(&self.stdout)
                            .await
                            .context("send echo ok response")?;
                    }
                    Payload::Send { .. } => {
                        reply.body.payload = Payload::SendOk { offset: 0 };
                        // Test RPC calls
                        for node in &self.nodes {
                            if node != &self.node {
                                let payload = Payload::Echo {
                                    msg: format!("hello from {}", self.node),
                                };
                                eprintln!("payload: {:?}", payload);
                                let reply = self.rpc(&node, payload).await?;
                                eprintln!("reply: {:?}", reply);
                            }
                        }
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
                    | Payload::SendOk { .. }
                    | Payload::EchoOk { .. } => {}
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
