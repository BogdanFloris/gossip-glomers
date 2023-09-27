use std::{collections::HashMap, io::StdoutLock};

use anyhow::{Context, Ok};
use gossip_glomers::{event_loop, Event, Init, Node};
use serde::{Deserialize, Serialize};

const MSGS_TO_POLL: usize = 5;

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
    id: usize,
    /// Map of key to map of offset to message
    logs: HashMap<String, HashMap<usize, u64>>,
    /// Map of key to latest offset
    latest_offsets: HashMap<String, usize>,
    /// Map of key to committed offset
    committed_offsets: HashMap<String, usize>,
}

impl Node<Payload, InjectedPayload> for KafkaNode {
    fn from_init(
        _init: Init,
        _tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            id: 1,
            logs: HashMap::new(),
            latest_offsets: HashMap::new(),
            committed_offsets: HashMap::new(),
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
                    Payload::Send { key, msg } => {
                        let offset_to_msg_map = self.logs.entry(key.clone()).or_default();
                        let latest_offset = self.latest_offsets.entry(key).or_insert_with(|| 0);
                        offset_to_msg_map.insert(*latest_offset, msg);
                        reply.body.payload = Payload::SendOk {
                            offset: *latest_offset,
                        };
                        *latest_offset += 1;
                        reply.send(output).context("send send ok response")?;
                    }
                    Payload::Poll { offsets } => {
                        let mut msgs: HashMap<String, Vec<Vec<u64>>> = HashMap::new();
                        for (key, offset) in offsets {
                            let offset_to_msg_map = self.logs.entry(key.clone()).or_default();
                            for i in offset..(offset + MSGS_TO_POLL) {
                                if let Some(msg) = offset_to_msg_map.get(&i) {
                                    msgs.entry(key.clone())
                                        .or_default()
                                        .push(vec![i as u64, *msg]);
                                }
                            }
                        }
                        reply.body.payload = Payload::PollOk { msgs };
                        reply.send(output).context("send poll ok response")?;
                    }
                    Payload::CommitOffsets { offsets } => {
                        offsets.into_iter().for_each(|(key, offset)| {
                            self.committed_offsets
                                .entry(key)
                                .and_modify(|o| *o = offset)
                                .or_insert(offset);
                        });
                        reply.body.payload = Payload::CommitOffsetsOk;
                        reply
                            .send(output)
                            .context("send commit offsets ok response")?;
                    }
                    Payload::ListCommittedOffsets { keys } => {
                        let mut offsets = HashMap::new();
                        keys.into_iter().for_each(|key| {
                            if let Some(offset) = self.committed_offsets.get(&key) {
                                offsets.insert(key, *offset);
                            }
                        });
                        reply.body.payload = Payload::ListCommittedOffsetsOk { offsets };
                        reply
                            .send(output)
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

fn main() -> anyhow::Result<()> {
    event_loop::<KafkaNode, _, _>()
}
