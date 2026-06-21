//! Throwaway end-to-end check for chat fan-out + sanitization.
//! Run a relay on 127.0.0.1:4455, then: cargo run --example chat_e2e -p watch

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use protocol::{
    decode, encode, HostHello, HostToRelay, RelayToHost, RelayToWatch, WatchHello, WatchToRelay,
    PROTOCOL_VERSION,
};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn b<T: serde::Serialize>(m: &T) -> Message {
    Message::Binary(Bytes::from(encode(m)))
}

async fn join(base: &str, name: &str, code: &str) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("{base}/watch"))
        .await
        .expect("watch connect");
    ws.send(b(&WatchToRelay::Hello(WatchHello {
        version: PROTOCOL_VERSION.into(),
        name: Some(name.into()),
        cols: 0,
        rows: 0,
    })))
    .await
    .unwrap();
    ws.next().await; // Welcome
    ws.send(b(&WatchToRelay::Join { target: code.into() }))
        .await
        .unwrap();
    loop {
        match ws.next().await {
            Some(Ok(Message::Binary(x))) => {
                if matches!(decode::<RelayToWatch>(&x[..]), Ok(RelayToWatch::Joined { .. })) {
                    break;
                }
            }
            _ => panic!("no joined for {name}"),
        }
    }
    ws
}

#[tokio::main]
async fn main() {
    let base = "ws://127.0.0.1:4455";

    // Host with chat enabled.
    let (mut host, _) = tokio_tungstenite::connect_async(format!("{base}/host"))
        .await
        .expect("host connect");
    host.send(b(&HostToRelay::Hello(HostHello {
        name: "host".into(),
        shell: "test".into(),
        public: true,
        cols: 80,
        rows: 24,
        auth_key: None,
        chat: true,
        version: PROTOCOL_VERSION.into(),
    })))
    .await
    .unwrap();
    let code = loop {
        match host.next().await {
            Some(Ok(Message::Binary(x))) => {
                if let Ok(RelayToHost::Welcome { code, .. }) = decode::<RelayToHost>(&x[..]) {
                    break code;
                }
            }
            _ => panic!("no host welcome"),
        }
    };
    tokio::spawn(async move { while let Some(Ok(_)) = host.next().await {} }); // keep alive

    let mut bob = join(base, "bob", &code).await; // receiver
    let mut alice = join(base, "alice", &code).await; // sender

    // Alice sends a message with an embedded ANSI escape to test sanitization.
    alice
        .send(b(&WatchToRelay::Chat {
            text: "hello\x1b[31m world".into(),
        }))
        .await
        .unwrap();

    let got = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(Ok(Message::Binary(x))) = bob.next().await
                && let Ok(RelayToWatch::Chat { from, text, .. }) = decode::<RelayToWatch>(&x[..])
            {
                return (from, text);
            }
        }
    })
    .await;

    match got {
        Ok((from, text)) => {
            let no_esc = !text.contains('\u{1b}');
            let ok = from == "alice" && text.contains("hello") && text.contains("world") && no_esc;
            println!("received from={from:?} text={text:?} (no ESC: {no_esc})");
            println!("{}", if ok { "PASS" } else { "FAIL" });
            if !ok {
                std::process::exit(1);
            }
        }
        Err(_) => {
            println!("FAIL: timed out waiting for chat");
            std::process::exit(1);
        }
    }
}
