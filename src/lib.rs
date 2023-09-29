use std::collections::HashMap;
use tokio::io::AsyncWriteExt;

use anyhow::{Context, Ok};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message<Payload> {
    pub src: String,
    pub dest: String,
    pub body: Body<Payload>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

impl<Payload> Message<Payload> {
    pub fn into_reply(self, id: Option<&mut usize>) -> Self {
        Self {
            src: self.dest,
            dest: self.src,
            body: Body {
                id: id.map(|id| {
                    let temp = *id;
                    *id += 1;
                    temp
                }),
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }

    pub async fn send(&self, out: &mut tokio::io::Stdout) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let raw_msg = serde_json::to_string(self).context("deserialize message")?;
        out.write_all(raw_msg.as_bytes())
            .await
            .context("serialize response message")?;
        out.write_all(b"\n")
            .await
            .context("write trailing newline")?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[async_trait]
pub trait Node<Payload, InjectedPayload = ()> {
    fn from_init(
        init: Init,
        tx: tokio::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
        rpc_senders: tokio::sync::Mutex<
            HashMap<usize, tokio::sync::oneshot::Sender<Event<Payload, InjectedPayload>>>,
        >,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;

    async fn handle(
        &mut self,
        event: Event<Payload, InjectedPayload>,
        output: &mut tokio::io::Stdout,
    ) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    Injected(InjectedPayload),
    EOF,
}

pub async fn event_loop<N, P, IP>() -> anyhow::Result<()>
where
    N: Node<P, IP>,
    P: DeserializeOwned + Send + 'static,
    IP: Send + 'static,
{
    let stdin = tokio::io::stdin();
    let stdin = tokio::io::BufReader::new(stdin);
    let mut stdout = tokio::io::stdout();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .lines()
            .next_line()
            .await
            .expect("read init")
            .context("failed to read init message from stdin")?,
    )
    .context("init message could not be deserialized")?;

    let InitPayload::Init(init) = init_msg.body.payload else {
        return Err(anyhow::anyhow!("expected init message"));
    };
    let rpc_senders = tokio::sync::Mutex::new(HashMap::new());
    let mut node: N = N::from_init(init, tx.clone(), rpc_senders)?;

    let reply = Message {
        src: init_msg.dest,
        dest: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };

    reply
        .send(&mut stdout)
        .await
        .context("send response to init")?;

    let thread = tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut stdin = tokio::io::BufReader::new(stdin).lines();
        while let Some(line) = stdin.next_line().await.expect("read line") {
            let input: Message<P> = serde_json::from_str(&line)
                .context("input from Maelstrom on stdin could not be deserialized")?;
            if let Err(_) = tx.send(Event::Message(input)).await {
                return Ok(());
            }
        }
        let _ = tx.send(Event::EOF);
        Ok(())
    });

    while let Some(event) = rx.recv().await {
        node.handle(event, &mut stdout)
            .await
            .context("failed to handle event")?;
    }

    thread
        .await
        .expect("stdin thread panicked")
        .context("stdin thread errored")?;

    Ok(())
}
