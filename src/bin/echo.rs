use std::io::StdoutLock;

use anyhow::{Context, Ok};
use gossip_glomers::{event_loop, Init, Node};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    id: usize,
}

impl Node<Payload> for EchoNode {
    fn from_init(init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self { id: 1 })
    }

    fn handle(
        &mut self,
        event: gossip_glomers::Event<Payload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let gossip_glomers::Event::Message(message) = event else {
            panic!("unexpected event: {:?}", event);
        };
        let mut reply = message.clone().into_reply(Some(&mut self.id));
        match message.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                reply.send(output).context("send response message")?;
            }
            Payload::EchoOk { .. } => {}
        };
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    event_loop::<EchoNode, _>()
}
