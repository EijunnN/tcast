//! tcast relay.
//!
//! Central server that hosts connect to (pushing terminal output) and that
//! watchers connect to (browsing and viewing streams). TLS is expected to be
//! terminated by a reverse proxy (Caddy/nginx) in front of this process, so the
//! relay itself speaks plain HTTP/WebSocket.
//!
//! Endpoints:
//!   GET /             health text
//!   GET /api/streams  JSON list of public streams (debugging / future web UI)
//!   GET /host         WebSocket: a streamer
//!   GET /watch        WebSocket: a spectator

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use clap::Parser as ClapParser;
use futures_util::{SinkExt, StreamExt};
use nanoid::nanoid;
use serde::Serialize;
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use protocol::{
    decode, encode, HostToRelay, RelayToHost, RelayToWatch, StreamInfo, WatchToRelay, PROTOCOL_VERSION,
};

/// Unambiguous alphabet for join codes (no 0/O/1/I/L).
const CODE_ALPHABET: [char; 30] = [
    '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'M',
    'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
];

/// Max accepted WebSocket message/frame size. Terminal output is chunked far
/// below this; the cap bounds memory a single peer can force the relay to buffer.
const MAX_MSG: usize = 1 << 20; // 1 MiB

/// Max number of concurrent streams the relay will host (coarse DoS guard).
const MAX_STREAMS: usize = 512;

/// Constant-time byte-slice equality, to avoid leaking the auth key prefix via
/// response-timing differences.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let n = a.len().max(b.len());
    let mut diff = (a.len() ^ b.len()) as u64;
    for i in 0..n {
        diff |= (*a.get(i).unwrap_or(&0) ^ *b.get(i).unwrap_or(&0)) as u64;
    }
    diff == 0
}

/// What gets fanned out to every viewer of a stream.
#[derive(Clone)]
enum Bcast {
    Output(Arc<Vec<u8>>),
    Resize { cols: u16, rows: u16 },
    Privacy(bool),
    Viewers(u32),
    Chat { from: String, text: String, ts: u64 },
    Ended,
}

/// Live state for a single stream.
struct Stream {
    /// Mutable metadata (viewer count and size change over time).
    info: Mutex<StreamInfo>,
    /// Fan-out channel to all current viewers.
    tx: broadcast::Sender<Bcast>,
    /// Terminal emulator mirroring the host screen, used to snapshot late joins.
    parser: Mutex<vt100::Parser>,
    paused: AtomicBool,
    /// Outbound channel to the host task (viewer-count updates, etc.).
    host_tx: mpsc::UnboundedSender<RelayToHost>,
    /// Whether the host opted into viewer chat (`tcast stream --chat`).
    chat_enabled: bool,
}

struct Registry {
    streams: RwLock<HashMap<String, Arc<Stream>>>,
    /// Private join codes → stream_id.
    codes: RwLock<HashMap<String, String>>,
    auth_key: Option<String>,
}

#[derive(ClapParser)]
#[command(name = "tcast-relay", about = "tcast relay server")]
struct Args {
    /// Address to bind, e.g. 0.0.0.0:4455
    #[arg(long, default_value = "0.0.0.0:4455")]
    bind: String,
    /// Optional shared secret hosts must present to stream. Can also be set via
    /// the TCAST_AUTH_KEY environment variable (handy for systemd).
    #[arg(long, env = "TCAST_AUTH_KEY")]
    auth_key: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let args = Args::parse();

    let reg = Arc::new(Registry {
        streams: RwLock::new(HashMap::new()),
        codes: RwLock::new(HashMap::new()),
        auth_key: args.auth_key,
    });
    if reg.auth_key.is_some() {
        info!("host authentication is ENABLED (--auth-key set)");
    }

    let app = Router::new()
        .route("/", get(root))
        .route("/api/streams", get(api_streams))
        .route("/host", get(ws_host))
        .route("/watch", get(ws_watch))
        .with_state(reg);

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    info!("relay listening on {}", args.bind);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn root() -> &'static str {
    "tcast relay — OK"
}

async fn api_streams(State(reg): State<Arc<Registry>>) -> impl IntoResponse {
    Json(collect_public(&reg).await)
}

async fn ws_host(ws: WebSocketUpgrade, State(reg): State<Arc<Registry>>) -> impl IntoResponse {
    ws.max_message_size(MAX_MSG)
        .max_frame_size(MAX_MSG)
        .on_upgrade(move |socket| handle_host(socket, reg))
}

async fn ws_watch(ws: WebSocketUpgrade, State(reg): State<Arc<Registry>>) -> impl IntoResponse {
    ws.max_message_size(MAX_MSG)
        .max_frame_size(MAX_MSG)
        .on_upgrade(move |socket| handle_watch(socket, reg))
}

/// Build a `Message::Binary` from any protocol message.
fn bin<T: Serialize>(msg: &T) -> Message {
    Message::Binary(Bytes::from(encode(msg)))
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Clean an untrusted chat message: replace control bytes (incl. the ESC that
/// introduces ANSI sequences) with spaces, collapse whitespace, trim, and cap
/// length. Returns `None` when nothing printable remains, so chat can never
/// inject VT codes into a host's mirror or another viewer's TUI.
fn sanitize_chat(text: &str) -> Option<String> {
    let spaced: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let collapsed = spaced.split_whitespace().collect::<Vec<_>>().join(" ");
    let capped: String = collapsed.chars().take(256).collect();
    let trimmed = capped.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Clean an untrusted display name for chat labels. Falls back to "anon".
fn sanitize_name(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .filter(|c| !c.is_control())
        .take(24)
        .collect();
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        "anon".to_string()
    } else {
        trimmed.to_string()
    }
}

fn make_code(name: &str) -> String {
    let slug: String = name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(12)
        .collect::<String>()
        .to_lowercase();
    let slug = if slug.is_empty() {
        "term".to_string()
    } else {
        slug
    };
    format!("{}-{}", slug, nanoid!(4, &CODE_ALPHABET[..]))
}

async fn collect_public(reg: &Registry) -> Vec<StreamInfo> {
    let streams = reg.streams.read().await;
    let mut out = Vec::new();
    for s in streams.values() {
        let info = s.info.lock().await;
        if info.public {
            out.push(info.clone());
        }
    }
    out.sort_by(|a, b| b.viewers.cmp(&a.viewers).then(a.name.cmp(&b.name)));
    out
}

/// Resolve a join target to a stream.
///
/// Private streams are reachable **only** by their join code. A public stream may
/// additionally be joined by its `stream_id` (as advertised in the directory), so
/// a private stream's id is never a valid join target.
async fn resolve(reg: &Registry, target: &str) -> Option<Arc<Stream>> {
    // 1) Try the private join code.
    if let Some(id) = reg.codes.read().await.get(target).cloned()
        && let Some(s) = reg.streams.read().await.get(&id).cloned()
    {
        return Some(s);
    }
    // 2) Fall back to stream_id, but only for public streams.
    let s = reg.streams.read().await.get(target).cloned()?;
    if s.info.lock().await.public {
        Some(s)
    } else {
        None
    }
}

/// Decrement viewer count on leave/disconnect and notify host + viewers.
async fn leave(s: &Stream) {
    let n = {
        let mut i = s.info.lock().await;
        i.viewers = i.viewers.saturating_sub(1);
        i.viewers
    };
    let _ = s.host_tx.send(RelayToHost::Viewers(n));
    let _ = s.tx.send(Bcast::Viewers(n));
}

// ───────────────────────────── Host side ────────────────────────────────

async fn handle_host(socket: WebSocket, reg: Arc<Registry>) {
    let (mut sender, mut receiver) = socket.split();

    // First frame must be Hello.
    let hello = match receiver.next().await {
        Some(Ok(Message::Binary(b))) => match decode::<HostToRelay>(&b[..]) {
            Ok(HostToRelay::Hello(h)) => h,
            _ => {
                let _ = sender
                    .send(bin(&RelayToHost::Error("expected Hello".into())))
                    .await;
                return;
            }
        },
        _ => return,
    };

    if hello.version != PROTOCOL_VERSION {
        let _ = sender
            .send(bin(&RelayToHost::Error(format!(
                "protocol mismatch: relay {PROTOCOL_VERSION}, host {}",
                hello.version
            ))))
            .await;
        return;
    }
    if let Some(key) = &reg.auth_key
        && !ct_eq(
            hello.auth_key.as_deref().unwrap_or("").as_bytes(),
            key.as_bytes(),
        )
    {
        let _ = sender
            .send(bin(&RelayToHost::Error("invalid auth key".into())))
            .await;
        return;
    }

    // Coarse capacity guard against stream-count exhaustion.
    if reg.streams.read().await.len() >= MAX_STREAMS {
        let _ = sender
            .send(bin(&RelayToHost::Error("relay at capacity".into())))
            .await;
        return;
    }

    let stream_id = nanoid!(10);
    let started = now_unix();
    let cols = hello.cols.max(1);
    let rows = hello.rows.max(1);

    let info = StreamInfo {
        stream_id: stream_id.clone(),
        name: hello.name.clone(),
        shell: hello.shell.clone(),
        cols,
        rows,
        viewers: 0,
        started_unix: started,
        public: hello.public,
    };

    let (btx, _) = broadcast::channel::<Bcast>(2048);
    let (htx, mut hrx) = mpsc::unbounded_channel::<RelayToHost>();
    let parser = vt100::Parser::new(rows, cols, 0);

    let stream = Arc::new(Stream {
        info: Mutex::new(info),
        tx: btx,
        parser: Mutex::new(parser),
        paused: AtomicBool::new(false),
        host_tx: htx,
        chat_enabled: hello.chat,
    });

    reg.streams
        .write()
        .await
        .insert(stream_id.clone(), stream.clone());

    // Pick a unique join code and register it atomically under a single write
    // lock, so two hosts can't pick the same code and then overwrite each other.
    let code = {
        let mut codes = reg.codes.write().await;
        let mut c = make_code(&hello.name);
        while codes.contains_key(&c) {
            c = make_code(&hello.name);
        }
        codes.insert(c.clone(), stream_id.clone());
        c
    };

    let _ = sender
        .send(bin(&RelayToHost::Welcome {
            stream_id: stream_id.clone(),
            code: code.clone(),
            viewers: 0,
        }))
        .await;
    // Note: the join code is a shared secret for private streams, so it is not
    // logged in clear text.
    info!(
        "stream started: id={} name={} public={}",
        stream_id, hello.name, hello.public
    );

    loop {
        tokio::select! {
            // Relay → host (viewer counts, pings).
            Some(msg) = hrx.recv() => {
                if sender.send(bin(&msg)).await.is_err() {
                    break;
                }
            }
            // Host → relay.
            incoming = receiver.next() => {
                match incoming {
                    Some(Ok(Message::Binary(b))) => match decode::<HostToRelay>(&b[..]) {
                        Ok(HostToRelay::Output(data)) => {
                            if !stream.paused.load(Ordering::Relaxed) {
                                stream.parser.lock().await.process(&data);
                                let _ = stream.tx.send(Bcast::Output(Arc::new(data)));
                            }
                        }
                        Ok(HostToRelay::Resize { cols, rows }) => {
                            let (c, r) = (cols.max(1), rows.max(1));
                            stream.parser.lock().await.screen_mut().set_size(r, c);
                            {
                                let mut i = stream.info.lock().await;
                                i.cols = c;
                                i.rows = r;
                            }
                            let _ = stream.tx.send(Bcast::Resize { cols: c, rows: r });
                        }
                        Ok(HostToRelay::Privacy { paused }) => {
                            stream.paused.store(paused, Ordering::Relaxed);
                            let _ = stream.tx.send(Bcast::Privacy(paused));
                        }
                        Ok(HostToRelay::Pong) => {}
                        Ok(HostToRelay::Hello(_)) => {}
                        Ok(HostToRelay::Bye) => break,
                        Err(e) => warn!("bad host frame: {e}"),
                    },
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }

    let _ = stream.tx.send(Bcast::Ended);
    reg.streams.write().await.remove(&stream_id);
    reg.codes.write().await.remove(&code);
    info!("stream ended: id={}", stream_id);
}

// ──────────────────────────── Watch side ────────────────────────────────

async fn handle_watch(socket: WebSocket, reg: Arc<Registry>) {
    let (mut sender, mut receiver) = socket.split();

    // First frame must be Hello; capture the (sanitized) chat display name.
    let name = match receiver.next().await {
        Some(Ok(Message::Binary(b))) => match decode::<WatchToRelay>(&b[..]) {
            Ok(WatchToRelay::Hello(h)) => {
                if h.version != PROTOCOL_VERSION {
                    let _ = sender
                        .send(bin(&RelayToWatch::Error(format!(
                            "protocol mismatch: relay {PROTOCOL_VERSION}, watcher {}",
                            h.version
                        ))))
                        .await;
                    return;
                }
                sanitize_name(h.name.as_deref().unwrap_or("anon"))
            }
            _ => {
                let _ = sender
                    .send(bin(&RelayToWatch::Error("expected Hello".into())))
                    .await;
                return;
            }
        },
        _ => return,
    };
    if sender.send(bin(&RelayToWatch::Welcome)).await.is_err() {
        return;
    }

    let mut joined: Option<Arc<Stream>> = None;
    let mut rx: Option<broadcast::Receiver<Bcast>> = None;
    // Coarse chat rate-limit: at most 5 messages per rolling 10s per connection.
    let mut chat_times: std::collections::VecDeque<u64> = std::collections::VecDeque::new();

    loop {
        tokio::select! {
            incoming = receiver.next() => {
                match incoming {
                    Some(Ok(Message::Binary(b))) => match decode::<WatchToRelay>(&b[..]) {
                        Ok(WatchToRelay::List) => {
                            let list = collect_public(&reg).await;
                            if sender.send(bin(&RelayToWatch::Streams(list))).await.is_err() {
                                break;
                            }
                        }
                        Ok(WatchToRelay::Join { target }) => {
                            if let Some(s) = joined.take() {
                                leave(&s).await;
                                rx = None;
                            }
                            match resolve(&reg, &target).await {
                                Some(s) => {
                                    rx = Some(s.tx.subscribe());
                                    let snapshot =
                                        s.parser.lock().await.screen().contents_formatted();
                                    let info = {
                                        let mut i = s.info.lock().await;
                                        i.viewers += 1;
                                        i.clone()
                                    };
                                    let n = info.viewers;
                                    let _ = s.host_tx.send(RelayToHost::Viewers(n));
                                    let _ = s.tx.send(Bcast::Viewers(n));
                                    // Record membership BEFORE the fallible send so the
                                    // unified cleanup at loop exit always decrements,
                                    // even if this Joined frame fails to send.
                                    joined = Some(s);
                                    if sender
                                        .send(bin(&RelayToWatch::Joined { info, snapshot }))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                None => {
                                    let _ = sender
                                        .send(bin(&RelayToWatch::Error(format!(
                                            "no live stream for '{target}'"
                                        ))))
                                        .await;
                                }
                            }
                        }
                        Ok(WatchToRelay::Leave) => {
                            if let Some(s) = joined.take() {
                                leave(&s).await;
                                rx = None;
                            }
                        }
                        Ok(WatchToRelay::Chat { text }) => {
                            match &joined {
                                Some(s) if s.chat_enabled => {
                                    if let Some(clean) = sanitize_chat(&text) {
                                        let now = now_unix();
                                        while chat_times
                                            .front()
                                            .is_some_and(|t| now.saturating_sub(*t) >= 10)
                                        {
                                            chat_times.pop_front();
                                        }
                                        if chat_times.len() < 5 {
                                            chat_times.push_back(now);
                                            let _ = s.tx.send(Bcast::Chat {
                                                from: name.clone(),
                                                text: clean,
                                                ts: now,
                                            });
                                        }
                                    }
                                }
                                Some(_) => {
                                    let _ = sender
                                        .send(bin(&RelayToWatch::Error(
                                            "chat is disabled for this stream".into(),
                                        )))
                                        .await;
                                }
                                None => {}
                            }
                        }
                        Ok(WatchToRelay::Pong) => {}
                        Ok(WatchToRelay::Hello(_)) => {}
                        Err(e) => warn!("bad watch frame: {e}"),
                    },
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
            // Stream events; only polled while joined.
            res = async { rx.as_mut().unwrap().recv().await }, if rx.is_some() => {
                match res {
                    Ok(Bcast::Output(buf)) => {
                        if sender.send(bin(&RelayToWatch::Output((*buf).clone()))).await.is_err() {
                            break;
                        }
                    }
                    Ok(Bcast::Resize { cols, rows }) => {
                        if sender.send(bin(&RelayToWatch::Resize { cols, rows })).await.is_err() {
                            break;
                        }
                    }
                    Ok(Bcast::Privacy(paused)) => {
                        if sender.send(bin(&RelayToWatch::Privacy { paused })).await.is_err() {
                            break;
                        }
                    }
                    Ok(Bcast::Viewers(n)) => {
                        if sender.send(bin(&RelayToWatch::Viewers(n))).await.is_err() {
                            break;
                        }
                    }
                    Ok(Bcast::Chat { from, text, ts }) => {
                        if sender
                            .send(bin(&RelayToWatch::Chat { from, text, ts_unix: ts }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(Bcast::Ended) => {
                        let _ = sender.send(bin(&RelayToWatch::Ended)).await;
                        joined = None;
                        rx = None;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Slow viewer: drop the stale backlog and resync with a full
                        // repaint. Capturing the snapshot and resubscribing while holding
                        // the parser lock keeps them consistent — the host locks the
                        // parser before broadcasting, so no Output can slip in between,
                        // and the fresh subscription starts strictly after the snapshot.
                        if let Some(s) = &joined {
                            let snap = {
                                let p = s.parser.lock().await;
                                let snap = p.screen().contents_formatted();
                                rx = Some(s.tx.subscribe());
                                snap
                            };
                            if sender.send(bin(&RelayToWatch::Output(snap))).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        rx = None;
                    }
                }
            }
        }
    }

    if let Some(s) = joined.take() {
        leave(&s).await;
    }
}
