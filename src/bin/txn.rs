use std::{collections::HashMap, sync::atomic::AtomicUsize};

use anyhow::{Context, Ok};
use async_trait::async_trait;
use gossip_glomers::{event_loop, Event, Init, Node};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

type Operation = (String, u32, Option<u32>);

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Txn { txn: Vec<Operation> },
    TxnOk { txn: Vec<Operation> },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InjectedPayload {}

struct TxnNode {
    id: AtomicUsize,
    stdout: Mutex<tokio::io::Stdout>,
    storage: Mutex<HashMap<u32, u32>>,
}

#[async_trait]
impl Node<Payload, InjectedPayload> for TxnNode {
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
            storage: Mutex::new(HashMap::new()),
        })
    }

    async fn handle(&self, event: Event<Payload, InjectedPayload>) -> anyhow::Result<()> {
        match event {
            Event::EOF => {}
            Event::Message(payload) => {
                let mut reply = payload.into_reply(Some(&self.id));
                match reply.body.payload {
                    Payload::Txn { txn } => {
                        let mut storage = self.storage.lock().await;
                        let mut txn_ok = vec![];
                        for op in txn {
                            match op.0.as_str() {
                                "r" => {
                                    let val = storage.get(&op.1);
                                    txn_ok.push((op.0, op.1, val.cloned()));
                                }
                                "w" => {
                                    if let Some(val) = op.2 {
                                        storage.insert(op.1, val);
                                    }
                                    txn_ok.push(op.clone());
                                }
                                _ => {}
                            }
                        }
                        reply.body.payload = Payload::TxnOk { txn: txn_ok };
                        reply.send(&self.stdout).await.context("send reply")?;
                    }
                    Payload::TxnOk { .. } => {}
                }
            }
            Event::Injected(..) => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    event_loop::<TxnNode, _, _>().await
}
