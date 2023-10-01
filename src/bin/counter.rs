use std::{cmp, collections::HashMap, sync::atomic::AtomicUsize, time::Duration};

use anyhow::{Context, Ok};
use async_trait::async_trait;
use gossip_glomers::{event_loop, Event, Init, Node};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add { delta: u64 },
    AddOk,
    Read,
    ReadOk { value: u64 },
    Sync { value: u64 },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InjectedPayload {
    Sync,
}

struct CounterNode {
    id: AtomicUsize,
    node: String,
    nodes: Vec<String>,
    counter: Mutex<HashMap<String, u64>>,
    stdout: Mutex<tokio::io::Stdout>,
}

#[async_trait]
impl Node<Payload, InjectedPayload> for CounterNode {
    fn from_init(
        init: Init,
        tx: tokio::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
        stdout: Mutex<tokio::io::Stdout>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        // Generate a Gossip injection event every 500ms
        // TODO: handle EOF (AtomicBool?)
        tokio::spawn(async move {
            loop {
                std::thread::sleep(Duration::from_millis(500));
                if let Err(_) = tx
                    .send(gossip_glomers::Event::Injected(InjectedPayload::Sync))
                    .await
                {
                    break;
                }
            }
        });

        Ok(Self {
            id: 1.into(),
            node: init.node_id,
            nodes: init.node_ids.clone(),
            counter: Mutex::new(init.node_ids.into_iter().map(|id| (id, 0)).collect()),
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
                    Payload::Add { delta } => {
                        if let Some(current) = self.counter.lock().await.get_mut(&reply.src) {
                            *current += delta;
                        }
                        reply.body.payload = Payload::AddOk;
                        reply
                            .send(&self.stdout)
                            .await
                            .context("send add response")?;
                    }
                    Payload::AddOk => {}
                    Payload::Read => {
                        let value = self.counter.lock().await.iter().map(|(_, v)| v).sum();
                        reply.body.payload = Payload::ReadOk { value };
                        reply
                            .send(&self.stdout)
                            .await
                            .context("send read response")?;
                    }
                    Payload::ReadOk { .. } => {}
                    Payload::Sync { value } => {
                        if let Some(current) = self.counter.lock().await.get_mut(&reply.dest) {
                            *current = cmp::max(*current, value);
                        }
                    }
                }
            }
            gossip_glomers::Event::Injected(_) => {
                for node in &self.nodes {
                    if node != &self.node {
                        let sync_msg = gossip_glomers::Message {
                            src: self.node.clone(),
                            dest: node.clone(),
                            body: gossip_glomers::Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::Sync {
                                    value: self.counter.lock().await[&self.node],
                                },
                            },
                        };
                        sync_msg
                            .send(&self.stdout)
                            .await
                            .context("send sync message")?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    event_loop::<CounterNode, _, _>().await
}
