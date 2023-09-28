use std::{cmp, collections::HashMap, io::StdoutLock, time::Duration};

use anyhow::{Context, Ok};
use gossip_glomers::{event_loop, Event, Init, Node};
use serde::{Deserialize, Serialize};

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
    id: usize,
    node: String,
    nodes: Vec<String>,
    counter: HashMap<String, u64>,
}

impl Node<Payload, InjectedPayload> for CounterNode {
    fn from_init(
        init: Init,
        tx: tokio::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
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
            id: 1,
            node: init.node_id,
            nodes: init.node_ids.clone(),
            counter: init.node_ids.into_iter().map(|id| (id, 0)).collect(),
        })
    }

    fn handle(
        &mut self,
        event: gossip_glomers::Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match event {
            gossip_glomers::Event::EOF => {}
            gossip_glomers::Event::Message(message) => {
                let mut reply = message.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Add { delta } => {
                        if let Some(current) = self.counter.get_mut(&reply.src) {
                            *current += delta;
                        }
                        reply.body.payload = Payload::AddOk;
                        reply.send(output).context("send add response")?;
                    }
                    Payload::AddOk => {}
                    Payload::Read => {
                        let value = self.counter.iter().map(|(_, v)| v).sum();
                        reply.body.payload = Payload::ReadOk { value };
                        reply.send(output).context("send read response")?;
                    }
                    Payload::ReadOk { .. } => {}
                    Payload::Sync { value } => {
                        if let Some(current) = self.counter.get_mut(&reply.dest) {
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
                                    value: self.counter[&self.node],
                                },
                            },
                        };
                        sync_msg.send(output).context("send sync message")?;
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
