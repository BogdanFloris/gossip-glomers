use std::{collections::HashMap, io::StdoutLock};

use anyhow::{Context, Ok};
use gossip_glomers::{event_loop, Init, Node};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        #[serde(rename = "message")]
        msg: i32,
    },
    BroadcastOk,
    Read,
    ReadOk {
        #[serde(rename = "messages")]
        msgs: Vec<i32>,
    },
    Topology {
        #[serde(rename = "topology")]
        topo: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

struct BroadcastNode {
    node: String,
    nodes: Vec<String>,
    msgs: Vec<i32>,
    topo: HashMap<String, Vec<String>>,
    id: usize,
}

impl Node<Payload> for BroadcastNode {
    fn from_init(init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            node: init.node_id,
            nodes: init.node_ids,
            msgs: Vec::new(),
            topo: HashMap::new(),
            id: 1,
        })
    }

    fn handle(
        &mut self,
        event: gossip_glomers::Event<Payload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let gossip_glomers::Event::Message(message) = event else {
            panic!("unexpected event: {:?}", event);
        };
        let mut reply = message.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Broadcast { msg } => {
                self.msgs.push(msg);
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
            Payload::Topology { topo } => {
                self.topo = topo;
                reply.body.payload = Payload::TopologyOk;
                reply.send(output).context("send response message")?;
            }
            Payload::TopologyOk => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    event_loop::<BroadcastNode, _>()
}
