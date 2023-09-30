use std::{collections::HashMap, sync::atomic::AtomicUsize};

use anyhow::{Context, Ok};
use async_trait::async_trait;
use gossip_glomers::{event_loop, Event, Init, Node};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

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
    id: AtomicUsize,
    /// Map of key to map of offset to message
    logs: Mutex<HashMap<String, HashMap<usize, u64>>>,
    /// Map of key to latest offset
    latest_offsets: Mutex<HashMap<String, usize>>,
    /// Map of key to committed offset
    committed_offsets: Mutex<HashMap<String, usize>>,
}

#[async_trait]
impl Node<Payload, InjectedPayload> for KafkaNode {
    fn from_init(
        _init: Init,
        _tx: tokio::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            id: 1.into(),
            logs: Mutex::new(HashMap::new()),
            latest_offsets: Mutex::new(HashMap::new()),
            committed_offsets: Mutex::new(HashMap::new()),
        })
    }

    async fn handle(
        &self,
        event: gossip_glomers::Event<Payload, InjectedPayload>,
        output: &mut tokio::io::Stdout,
    ) -> anyhow::Result<()> {
        match event {
            gossip_glomers::Event::EOF => {}
            gossip_glomers::Event::Message(message) => {
                let mut reply = message.into_reply(Some(&self.id));
                match reply.body.payload {
                    Payload::Send { key, msg } => {
                        let mut logs = self.logs.lock().await;
                        let offset_to_msg_map = logs.entry(key.clone()).or_default();
                        let mut latest_offsets = self.latest_offsets.lock().await;
                        let latest_offset = latest_offsets.entry(key).or_insert_with(|| 0);
                        offset_to_msg_map.insert(*latest_offset, msg);
                        reply.body.payload = Payload::SendOk {
                            offset: *latest_offset,
                        };
                        *latest_offset += 1;
                        reply.send(output).await.context("send send ok response")?;
                    }
                    Payload::Poll { offsets } => {
                        let mut msgs: HashMap<String, Vec<Vec<u64>>> = HashMap::new();
                        for (key, offset) in offsets {
                            let mut logs = self.logs.lock().await;
                            let offset_to_msg_map = logs.entry(key.clone()).or_default();
                            for i in offset..(offset + MSGS_TO_POLL) {
                                if let Some(msg) = offset_to_msg_map.get(&i) {
                                    msgs.entry(key.clone())
                                        .or_default()
                                        .push(vec![i as u64, *msg]);
                                }
                            }
                        }
                        reply.body.payload = Payload::PollOk { msgs };
                        reply.send(output).await.context("send poll ok response")?;
                    }
                    Payload::CommitOffsets { offsets } => {
                        let mut committed_offsets = self.committed_offsets.lock().await;
                        offsets.into_iter().for_each(|(key, offset)| {
                            committed_offsets
                                .entry(key)
                                .and_modify(|o| *o = offset)
                                .or_insert(offset);
                        });
                        reply.body.payload = Payload::CommitOffsetsOk;
                        reply
                            .send(output)
                            .await
                            .context("send commit offsets ok response")?;
                    }
                    Payload::ListCommittedOffsets { keys } => {
                        let mut offsets = HashMap::new();
                        let committed_offsets = self.committed_offsets.lock().await;
                        keys.into_iter().for_each(|key| {
                            if let Some(offset) = committed_offsets.get(&key) {
                                offsets.insert(key, *offset);
                            }
                        });
                        reply.body.payload = Payload::ListCommittedOffsetsOk { offsets };
                        reply
                            .send(output)
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
