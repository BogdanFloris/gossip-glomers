use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use anyhow::{Context, Ok};
use async_trait::async_trait;
use gossip_glomers::{event_loop, Body, Event, Init, Message, Node, KV};
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
    Read {
        key: String,
    },
    ReadOk {
        value: u64,
    },
    Write {
        key: String,
        value: u64,
    },
    WriteOk {},
    Cas {
        key: String,
        from: u64,
        to: u64,
        #[serde(default, rename = "create_if_not_exists")]
        put: bool,
    },
    CasOk {},
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InjectedPayload {}

struct KafkaNode {
    id: AtomicUsize,
    node: String,
    stdout: Mutex<tokio::io::Stdout>,
    storage: String,
    rpc: Mutex<HashMap<usize, tokio::sync::oneshot::Sender<Message<Payload>>>>,
}

impl KafkaNode {
    async fn rpc(&self, to: &String, payload: Payload) -> anyhow::Result<Message<Payload>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Message {
            src: self.node.clone(),
            dest: to.clone(),
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
impl KV<u64> for KafkaNode {
    async fn read(&self, key: String) -> anyhow::Result<u64> {
        let payload = Payload::Read { key };
        let result = self
            .rpc(&self.storage, payload)
            .await
            .context("read from storage")?;
        match result.body.payload {
            Payload::ReadOk { value } => Ok(value),
            _ => anyhow::bail!("unexpected payload"),
        }
    }

    async fn write(&self, key: String, value: u64) -> anyhow::Result<()> {
        let payload = Payload::Write { key, value };
        let _result = self
            .rpc(&self.storage, payload)
            .await
            .context("write to storage");
        Ok(())
    }

    async fn cas(&self, key: String, from: u64, to: u64, put: bool) -> anyhow::Result<()> {
        let payload = Payload::Cas { key, from, to, put };
        let _result = self
            .rpc(&self.storage, payload)
            .await
            .context("cas to storage");
        Ok(())
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
        let storage = "lin-kv".to_string();

        Ok(Self {
            id,
            node: init.node_id,
            stdout,
            storage,
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
                    if let Err(_) = tx.send(message) {
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
                        // Test storage
                        // Write
                        let _ = self.write(self.node.clone(), 69).await.context("write i32");
                        // Read
                        let result = self.read(self.node.clone()).await.context("read i32")?;
                        eprintln!("read i32: {}", result);
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
                    | Payload::EchoOk { .. }
                    | Payload::Read { .. }
                    | Payload::ReadOk { .. }
                    | Payload::Write { .. }
                    | Payload::WriteOk {}
                    | Payload::Cas { .. }
                    | Payload::CasOk {} => {}
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
