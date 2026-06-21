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
//! No watcher input is ever delivered to the host's PTY/shell. The shell stays
//! strictly read-only by construction: there is no path from any protocol
//! message into the host's PTY writer — the host forwards only its own local
//! keystrokes. Watchers can list, join, leave and (when the host opts in with
//! `--chat`) send display-only [`Chat`] text, which the host and other viewers
//! render as inert UI and never feed to a shell.
//!
//! [`Chat`]: WatchToRelay::Chat
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
pub const PROTOCOL_VERSION: &str = "0.2";

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
    /// Whether this host accepts viewer chat (`tcast stream --chat`). When
    /// `false` the relay rejects chat messages for this stream.
    pub chat: bool,
    /// [`PROTOCOL_VERSION`] of the host build.
    pub version: String,
}

// ──────────────────────────── Watch ⇄ Relay ─────────────────────────────

/// Messages a **watcher** sends to the relay. Nothing here can reach a host's
/// PTY/shell — see the read-only invariant in the module docs. `Chat` carries
/// display-only text, never keystrokes.
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
    /// Send a chat message to the joined stream. Display-only text that never
    /// reaches any shell; requires the host to have enabled chat (`--chat`).
    Chat { text: String },
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
    /// A chat message from a viewer of the joined stream (sanitized,
    /// display-only). Fanned out to everyone watching, including the sender.
    Chat {
        from: String,
        text: String,
        ts_unix: u64,
    },
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
    /// Optional display name used to label this viewer's chat messages. The
    /// relay treats it as untrusted (sanitized/truncated); `None` → "anon".
    pub name: Option<String>,
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

// ───────────────────────────── Owned streams ────────────────────────────

/// Local markers for streams *this machine* is currently hosting, so the watch
/// client can hide/refuse the operator's own streams (you shouldn't watch
/// yourself). Purely client-side and advisory — it never touches the wire and
/// only catches the common same-machine case. A marker is a file named after the
/// `stream_id` whose contents are the join `code`, under the OS config dir at
/// `tcast/owned/`. Markers are removed when the host stops; a marker left behind
/// by a crash is harmless (ids/codes are unique, so it can only ever match a
/// stream that no longer exists).
pub mod owned {
    use std::fs;
    use std::path::PathBuf;

    fn dir() -> Option<PathBuf> {
        dirs::config_dir().map(|d| d.join("tcast").join("owned"))
    }

    fn file_name(stream_id: &str) -> String {
        stream_id
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
            .collect()
    }

    /// Record that this machine owns `stream_id` (joinable via `code`).
    pub fn mark(stream_id: &str, code: &str) {
        if let Some(d) = dir() {
            let _ = fs::create_dir_all(&d);
            let _ = fs::write(d.join(file_name(stream_id)), code);
        }
    }

    /// Drop the marker for a stream this machine no longer hosts.
    pub fn unmark(stream_id: &str) {
        if let Some(d) = dir() {
            let _ = fs::remove_file(d.join(file_name(stream_id)));
        }
    }

    /// Every `(stream_id, code)` this machine currently claims to own.
    pub fn list() -> Vec<(String, String)> {
        let Some(d) = dir() else {
            return Vec::new();
        };
        let Ok(entries) = fs::read_dir(&d) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for e in entries.flatten() {
            if let Ok(name) = e.file_name().into_string() {
                let code = fs::read_to_string(e.path()).unwrap_or_default().trim().to_string();
                out.push((name, code));
            }
        }
        out
    }

    /// True if `target` (a join code or a stream id) is owned by this machine.
    pub fn is_owned(target: &str) -> bool {
        list()
            .iter()
            .any(|(id, code)| id == target || code == target)
    }
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
            chat: true,
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

    #[test]
    fn chat_messages_round_trip() {
        let up = WatchToRelay::Chat {
            text: "hello host".into(),
        };
        let back: WatchToRelay = decode(&encode(&up)).unwrap();
        assert!(matches!(back, WatchToRelay::Chat { text } if text == "hello host"));

        let down = RelayToWatch::Chat {
            from: "viewer".into(),
            text: "hi".into(),
            ts_unix: 42,
        };
        let back: RelayToWatch = decode(&encode(&down)).unwrap();
        assert!(matches!(back, RelayToWatch::Chat { ts_unix, .. } if ts_unix == 42));
    }
}
