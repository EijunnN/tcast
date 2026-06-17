//! Wire protocol shared by `host`, `relay` and `watch`.
//!
//! Everything travels over a single WebSocket connection per peer as **binary**
//! frames encoded with MessagePack (`rmp-serde`). There are two connection
//! roles on the relay:
//!
//! * **Host** connects to `…/host` and pushes its terminal output.
//! * **Watch** connects to `…/watch`, browses streams and receives output.
//!
//! ## Read-only invariant
//!
//! By design there is **no** message variant that carries keystrokes from a
//! watcher towards a host. A spectator physically cannot type into a streamer's
//! shell: [`WatchToRelay`] only lets a viewer list, join and leave. This makes
//! the "spectators are read-only" guarantee a property of the type system, not
//! of runtime checks.
//!
//! ## Lifecycle
//!
//! ```text
//! HOST                         RELAY                          WATCH
//!  | --Hello(meta)----------->  |                               |
//!  | <--Welcome{id,code}------  |                               |
//!  | --Output(bytes)--------->  | (feeds a per-stream vt100)    |
//!  | --Resize{cols,rows}----->  |                               |
//!  |                            | <----------------List-------- |
//!  |                            | --Streams(public list)------> |
//!  |                            | <----------Join{target}------ |
//!  |                            | --Joined{info,snapshot}-----> |  (renders at once)
//!  | --Output(bytes)--------->  | --Output(bytes)-------------> |  (live deltas)
//!  | --Privacy{paused}------->  | --Privacy{paused}-----------> |
//!  | --Bye------------------->  | --Ended--------------------->  |
//! ```

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Protocol version. Bumped on any breaking change to the message shapes below.
/// The host sends it in [`HostHello::version`] and a watcher in [`WatchHello`]
/// so the relay can reject incompatible peers cleanly instead of mis-decoding.
pub const PROTOCOL_VERSION: &str = "0.1";

/// Default relay listen / connect port.
pub const DEFAULT_PORT: u16 = 4455;

// ───────────────────────────── Host ⇄ Relay ─────────────────────────────

/// Messages a **host** sends to the relay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HostToRelay {
    /// First frame after connecting. Announces the session.
    Hello(HostHello),
    /// A chunk of raw PTY output (already ANSI/VT-encoded). This is the bulk of
    /// the traffic, so the payload is sent as raw bytes via `serde_bytes`.
    Output(#[serde(with = "serde_bytes")] Vec<u8>),
    /// The host's terminal was resized. Viewers and the relay's emulator must
    /// resize their screen to match.
    Resize { cols: u16, rows: u16 },
    /// Privacy toggle. While `paused == true` the relay stops forwarding output
    /// and shows viewers a privacy placeholder instead of the live screen.
    Privacy { paused: bool },
    /// Application-level keepalive (in addition to WS pings).
    Pong,
    /// Graceful shutdown of the stream.
    Bye,
}

/// Messages the relay sends back to a **host**.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RelayToHost {
    /// Session accepted. `code` is the private join code to share (e.g.
    /// `"jean-9F3X"`); `stream_id` is the stable id used in public listings.
    Welcome {
        stream_id: String,
        code: String,
        viewers: u32,
    },
    /// Live viewer count, so the host can show "👁 N watching".
    Viewers(u32),
    /// Relay refused or terminated the session (bad auth key, version skew…).
    Error(String),
    /// Application-level keepalive; host replies with [`HostToRelay::Pong`].
    Ping,
}

/// Session metadata announced by a host on connect.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostHello {
    /// Display handle, e.g. `"jean"`.
    pub name: String,
    /// Shell being shared, e.g. `"pwsh"`, `"bash"`, `"zsh"`.
    pub shell: String,
    /// Whether this stream appears in the global public listing. When `false`
    /// the stream is reachable only via its private `code`.
    pub public: bool,
    pub cols: u16,
    pub rows: u16,
    /// Optional operator gate. If the relay was started with `--auth-key`, the
    /// host must present the matching key here or it is rejected.
    pub auth_key: Option<String>,
    /// [`PROTOCOL_VERSION`] of the host build.
    pub version: String,
}

// ──────────────────────────── Watch ⇄ Relay ─────────────────────────────

/// Messages a **watcher** sends to the relay. Note: nothing here can reach a
/// host's PTY — see the read-only invariant in the module docs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WatchToRelay {
    /// First frame after connecting.
    Hello(WatchHello),
    /// Ask for the current list of public streams.
    List,
    /// Join a stream. `target` is either a public `stream_id` (from a [`List`]
    /// response) or a private `code` shared by a host.
    ///
    /// [`List`]: WatchToRelay::List
    Join { target: String },
    /// Stop watching the current stream and return to browsing.
    Leave,
    /// Application-level keepalive; reply to [`RelayToWatch::Ping`].
    Pong,
}

/// Messages the relay sends to a **watcher**.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RelayToWatch {
    /// Handshake accepted.
    Welcome,
    /// Response to [`WatchToRelay::List`]: the public streams live right now.
    Streams(Vec<StreamInfo>),
    /// Join accepted. `snapshot` is a self-contained VT byte dump of the
    /// stream's current screen (from `vt100::Screen::contents_formatted`), so
    /// the viewer can render the full screen immediately, before any live
    /// [`Output`] deltas arrive.
    ///
    /// [`Output`]: RelayToWatch::Output
    Joined {
        info: StreamInfo,
        #[serde(with = "serde_bytes")]
        snapshot: Vec<u8>,
    },
    /// A live output delta from the joined host.
    Output(#[serde(with = "serde_bytes")] Vec<u8>),
    /// The joined host resized its terminal.
    Resize { cols: u16, rows: u16 },
    /// The joined host toggled privacy mode.
    Privacy { paused: bool },
    /// Viewer count of the joined stream changed.
    Viewers(u32),
    /// The joined stream ended (host disconnected or sent `Bye`).
    Ended,
    /// A request failed (unknown code, version skew, etc.).
    Error(String),
    /// Application-level keepalive; watcher replies with [`WatchToRelay::Pong`].
    Ping,
}

/// Handshake metadata sent by a watcher on connect.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchHello {
    /// [`PROTOCOL_VERSION`] of the watcher build.
    pub version: String,
    /// Viewer's render area, in case the relay wants it for analytics. The
    /// viewer always renders the host's screen scaled/clipped to its own size,
    /// so this is advisory only.
    pub cols: u16,
    pub rows: u16,
}

/// Public description of a live stream, shown in the browse list and on join.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    pub stream_id: String,
    pub name: String,
    pub shell: String,
    pub cols: u16,
    pub rows: u16,
    pub viewers: u32,
    /// Unix seconds when the stream started (for uptime display).
    pub started_unix: u64,
    pub public: bool,
}

// ────────────────────────────── Codec ──────────────────────────────────

/// Encode a message to a MessagePack byte frame.
///
/// Uses the compact (array-based) representation. Both ends link this exact
/// crate, so the layouts always match.
pub fn encode<T: Serialize>(msg: &T) -> Vec<u8> {
    rmp_serde::to_vec(msg).expect("MessagePack encoding of a protocol message cannot fail")
}

/// Decode a MessagePack byte frame into a message.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, rmp_serde::decode::Error> {
    rmp_serde::from_slice(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_round_trips() {
        let msg = HostToRelay::Output(b"\x1b[31mhello\x1b[0m".to_vec());
        let bytes = encode(&msg);
        let back: HostToRelay = decode(&bytes).unwrap();
        match back {
            HostToRelay::Output(b) => assert_eq!(b, b"\x1b[31mhello\x1b[0m"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn hello_round_trips() {
        let msg = HostToRelay::Hello(HostHello {
            name: "jean".into(),
            shell: "pwsh".into(),
            public: true,
            cols: 120,
            rows: 40,
            auth_key: Some("secret".into()),
            version: PROTOCOL_VERSION.into(),
        });
        let bytes = encode(&msg);
        let back: HostToRelay = decode(&bytes).unwrap();
        assert!(matches!(back, HostToRelay::Hello(_)));
    }

    #[test]
    fn watch_messages_round_trip() {
        let msg = WatchToRelay::Join {
            target: "jean-9F3X".into(),
        };
        let bytes = encode(&msg);
        let back: WatchToRelay = decode(&bytes).unwrap();
        match back {
            WatchToRelay::Join { target } => assert_eq!(target, "jean-9F3X"),
            _ => panic!("wrong variant"),
        }
    }
}
