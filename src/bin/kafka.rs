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
    logs: HashMap<String, Vec<u64>>,
    commit_offsets: HashMap<String, usize>,
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
            commit_offsets: HashMap::new(),
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
                        let logs_for_key = self.logs.entry(key).or_default();
                        logs_for_key.push(msg);
                        reply.body.payload = Payload::SendOk {
                            offset: logs_for_key.len() - 1,
                        };
                        reply.send(output).context("send send ok response")?;
                    }
                    Payload::Poll { offsets } => {
                        let mut msgs = HashMap::new();
                        for (key, offset) in offsets {
                            let logs_for_key = self.logs.entry(key.clone()).or_default();
                            let msgs_for_key = logs_for_key
                                .iter()
                                .skip(offset)
                                .take(MSGS_TO_POLL)
                                .enumerate()
                                .map(|(i, msg)| {
                                    vec![(offset + i) as u64, (*msg).try_into().unwrap()]
                                })
                                .collect::<Vec<Vec<u64>>>();
                            msgs.insert(key, msgs_for_key);
                        }
                        reply.body.payload = Payload::PollOk { msgs };
                        reply.send(output).context("send poll ok response")?;
                    }
                    Payload::CommitOffsets { offsets } => {
                        offsets.into_iter().for_each(|(key, offset)| {
                            self.commit_offsets
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
                            if let Some(offset) = self.commit_offsets.get(&key) {
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
