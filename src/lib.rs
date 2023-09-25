use std::io::BufRead;
use std::io::StdoutLock;
use std::io::Write;

use anyhow::{Context, Ok};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

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

    pub fn send(&self, output: &mut impl Write) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        serde_json::to_writer(&mut *output, self).context("serialize response message")?;
        output.write_all(b"\n").context("write trailing newline")?;
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

pub trait Node<Payload, InjectedPayload = ()> {
    fn from_init(
        init: Init,
        tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn handle(
        &mut self,
        event: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    Injected(InjectedPayload),
    EOF,
}

pub fn event_loop<N, P, IP>() -> anyhow::Result<()>
where
    N: Node<P, IP>,
    P: DeserializeOwned + Send + 'static,
    IP: Send + 'static,
{
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();
    let (tx, rx) = std::sync::mpsc::channel();

    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next()
            .expect("read init")
            .context("failed to read init message from stdin")?,
    )
    .context("init message could not be deserialized")?;

    let InitPayload::Init(init) = init_msg.body.payload else {
        return Err(anyhow::anyhow!("expected init message"));
    };
    let mut node: N = N::from_init(init, tx.clone())?;

    let reply = Message {
        src: init_msg.dest,
        dest: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };

    reply.send(&mut stdout).context("send response to init")?;

    drop(stdin);

    let thread = std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        for line in stdin.lines() {
            let line = line.context("input from Maelstrom on stdin could not be read")?;
            let input: Message<P> = serde_json::from_str(&line)
                .context("input from Maelstrom on stdin could not be deserialized")?;
            if let Err(_) = tx.send(Event::Message(input)) {
                return Ok(());
            }
        }
        let _ = tx.send(Event::EOF);
        Ok(())
    });

    for event in rx {
        node.handle(event, &mut stdout)
            .context("failed to handle event")?;
    }

    thread
        .join()
        .expect("stdin thread panicked")
        .context("stdin thread errored")?;

    Ok(())
}
