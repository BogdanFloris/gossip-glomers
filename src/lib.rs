use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::io::AsyncWriteExt;

use anyhow::{Context, Ok};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

pub mod kv;

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
    pub fn into_reply(self, id: Option<&AtomicUsize>) -> Self {
        Self {
            src: self.dest,
            dest: self.src,
            body: Body {
                id: id.map(|id| id.fetch_add(1, Ordering::SeqCst)),
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }

    pub async fn send(&self, out: &Mutex<tokio::io::Stdout>) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let raw_msg = serde_json::to_string(self).context("deserialize message")?;
        let mut out = out.lock().await;
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
pub trait Node<Payload, InjectedPayload = ()>: Sync + Send {
    fn from_init(
        init: Init,
        tx: tokio::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
        stdout: Mutex<tokio::io::Stdout>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;

    async fn handle(&self, event: Event<Payload, InjectedPayload>) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    Injected(InjectedPayload),
    EOF,
}

pub async fn event_loop<N, P, IP>() -> anyhow::Result<()>
where
    N: Node<P, IP> + 'static,
    P: DeserializeOwned + Send + 'static,
    IP: Send + 'static,
{
    let stdin = tokio::io::stdin();
    let stdin = tokio::io::BufReader::new(stdin);
    let stdout = Mutex::new(tokio::io::stdout());
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

    let reply = Message {
        src: init_msg.dest,
        dest: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };
    reply.send(&stdout).await.context("send response to init")?;

    let node = Arc::new(N::from_init(init, tx.clone(), stdout)?);

    let mut join_set = JoinSet::new();
    join_set.spawn(async move {
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
        let node_clone = node.clone();
        join_set.spawn(async move {
            node_clone
                .handle(event)
                .await
                .context("failed to handle event")?;
            Ok(())
        });
    }

    while let Some(_) = join_set.join_next().await {}

    Ok(())
}
