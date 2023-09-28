use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
    time::Duration,
};

use anyhow::{Context, Ok};
use gossip_glomers::{event_loop, Event, Init, Node};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        #[serde(rename = "message")]
        msg: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        #[serde(rename = "messages")]
        msgs: HashSet<usize>,
    },
    Topology {
        #[serde(rename = "topology")]
        topo: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        seen: HashSet<usize>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InjectedPayload {
    Gossip,
}

struct BroadcastNode {
    node: String,
    msgs: HashSet<usize>,
    neighbors: Vec<String>,
    known: HashMap<String, HashSet<usize>>,
    id: usize,
}

impl Node<Payload, InjectedPayload> for BroadcastNode {
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
                    .send(gossip_glomers::Event::Injected(InjectedPayload::Gossip))
                    .await
                {
                    break;
                }
            }
        });
        Ok(Self {
            node: init.node_id,
            msgs: HashSet::new(),
            neighbors: Vec::new(),
            known: init
                .node_ids
                .into_iter()
                .map(|id| (id, HashSet::new()))
                .collect(),
            id: 1,
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
                    Payload::Gossip { seen } => {
                        self.known
                            .get_mut(&reply.dest)
                            .expect("got gossip from unknown node")
                            .extend(seen.iter().copied());
                        self.msgs.extend(seen);
                    }
                    Payload::Broadcast { msg } => {
                        self.msgs.insert(msg);
                        reply.body.payload = Payload::BroadcastOk;
                        reply.send(output).context("send response message")?;
                    }
                    Payload::BroadcastOk => {}
                    Payload::Read => {
                        reply.body.payload = Payload::ReadOk {
                            msgs: self.msgs.clone(),
                        };
                        reply.send(output).context("send response message")?;
                    }
                    Payload::ReadOk { .. } => {}
                    Payload::Topology { mut topo } => {
                        self.neighbors = topo
                            .remove(&self.node)
                            .unwrap_or_else(|| panic!("node {} not found in topology", self.node));
                        reply.body.payload = Payload::TopologyOk;
                        reply.send(output).context("send response message")?;
                    }
                    Payload::TopologyOk => {}
                }
            }
            gossip_glomers::Event::Injected(_) => {
                for neighbor in &self.neighbors {
                    let known_to_n = &self.known[neighbor];
                    let seen = self.msgs.difference(&known_to_n).copied().collect();
                    let to_send = gossip_glomers::Message {
                        src: self.node.clone(),
                        dest: neighbor.clone(),
                        body: gossip_glomers::Body {
                            id: None,
                            in_reply_to: None,
                            payload: Payload::Gossip { seen },
                        },
                    };
                    to_send.send(output).context("send gossip message")?;
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    event_loop::<BroadcastNode, _, _>().await
}
