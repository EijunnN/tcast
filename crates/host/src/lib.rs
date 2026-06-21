//! tcast host library (the "streamer").
//!
//! Spawns the user's shell inside a PTY, mirrors its output to the local
//! terminal *and* to the relay, and forwards local keystrokes into the PTY so
//! the session stays fully usable. Spectators are read-only — nothing they do
//! can ever reach this PTY.
//!
//! The CLI front-end (`tcast stream`) parses arguments and calls [`run`].
//!
//! Hotkeys use a configurable prefix key (default Ctrl-], override with
//! `tcast stream --prefix <letter>`). With the default prefix:
//!   Ctrl-] p   toggle privacy (pause/resume what viewers see)
//!   Ctrl-] q   stop streaming and quit
//!   Ctrl-] Ctrl-]  send a literal Ctrl-] to the shell
//! Typing `exit` (or Ctrl-D) in the shell also ends the stream — handy when an
//! inner app (Claude Code, nano…) binds the prefix key.

use std::io::{Read, Write};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

use protocol::{decode, encode, HostHello, HostToRelay, RelayToHost, PROTOCOL_VERSION};

/// Default hotkey prefix (Ctrl-], 0x1D). Avoids the common Ctrl-O binding
/// (Claude Code, nano's WriteOut, Emacs). Override with `tcast stream --prefix`.
pub const DEFAULT_PREFIX: u8 = 0x1D;

/// Everything needed to start a host session. The CLI builds this from its
/// arguments + saved config; unset `name`/`shell` fall back to sensible
/// per-platform defaults inside [`run`].
pub struct StreamConfig {
    /// Relay base URL (ws:// or wss://). `/host` is appended automatically.
    pub relay: String,
    /// Display name shown to viewers (default: your username).
    pub name: Option<String>,
    /// Shell to launch (default: powershell.exe on Windows, $SHELL on Unix).
    pub shell: Option<String>,
    /// List this stream in the public directory (default: private, code-only).
    pub public: bool,
    /// Auth key, if the relay requires one.
    pub auth_key: Option<String>,
    /// Accept viewer chat for this stream (`tcast stream --chat`).
    pub chat: bool,
    /// Capture the microphone for push-to-talk voice (`tcast stream --voice`).
    pub voice: bool,
    /// Single key (a Ctrl- control byte) that toggles the mic while voice is on.
    /// Only intercepted when `voice` is true, so it stays usable otherwise.
    pub voice_key: u8,
    /// Hotkey prefix byte (a Ctrl- control code). See [`DEFAULT_PREFIX`].
    pub prefix: u8,
}

/// Control messages from the stdin thread to the async core.
enum Ctl {
    Privacy(bool),
    Quit,
}

fn bin<T: Serialize>(msg: &T) -> Message {
    Message::Binary(Bytes::from(encode(msg)))
}

fn default_shell() -> String {
    if cfg!(windows) {
        "powershell.exe".to_string()
    } else {
        std::env::var("SHELL").unwrap_or_else(|_| "/bin/bash".to_string())
    }
}

/// Short, viewer-facing shell label, e.g. "powershell" or "bash".
fn shell_label(path: &str) -> String {
    let base = path
        .replace('\\', "/")
        .rsplit('/')
        .next()
        .unwrap_or(path)
        .to_string();
    base.strip_suffix(".exe").unwrap_or(&base).to_string()
}

fn default_name() -> String {
    std::env::var("USERNAME")
        .or_else(|_| std::env::var("USER"))
        .unwrap_or_else(|_| "anon".to_string())
}

// ───────────────────────── platform raw mode ────────────────────────────

#[cfg(windows)]
mod console {
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::System::Console::{
        GetConsoleMode, GetStdHandle, SetConsoleMode, ENABLE_ECHO_INPUT, ENABLE_LINE_INPUT,
        ENABLE_PROCESSED_INPUT, ENABLE_PROCESSED_OUTPUT, ENABLE_VIRTUAL_TERMINAL_INPUT,
        ENABLE_VIRTUAL_TERMINAL_PROCESSING, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
    };

    /// Restores the original console modes when dropped.
    pub struct Guard {
        in_h: HANDLE,
        out_h: HANDLE,
        in_mode: u32,
        out_mode: u32,
    }

    pub fn enable() -> std::io::Result<Guard> {
        unsafe {
            let in_h = GetStdHandle(STD_INPUT_HANDLE);
            let out_h = GetStdHandle(STD_OUTPUT_HANDLE);
            let mut in_mode = 0u32;
            let mut out_mode = 0u32;
            GetConsoleMode(in_h, &mut in_mode);
            GetConsoleMode(out_h, &mut out_mode);

            // Raw input + deliver special keys as VT escape sequences.
            let new_in = (in_mode
                & !(ENABLE_LINE_INPUT | ENABLE_ECHO_INPUT | ENABLE_PROCESSED_INPUT))
                | ENABLE_VIRTUAL_TERMINAL_INPUT;
            SetConsoleMode(in_h, new_in);

            // Interpret the VT sequences the PTY emits when we echo them locally.
            let new_out = out_mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING | ENABLE_PROCESSED_OUTPUT;
            SetConsoleMode(out_h, new_out);

            Ok(Guard {
                in_h,
                out_h,
                in_mode,
                out_mode,
            })
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            unsafe {
                SetConsoleMode(self.in_h, self.in_mode);
                SetConsoleMode(self.out_h, self.out_mode);
            }
        }
    }

    /// Read raw console input via `ReadFile`. With `ENABLE_VIRTUAL_TERMINAL_INPUT`
    /// active this yields the VT byte stream (special keys as escape sequences),
    /// unlike `std::io::stdin()` on Windows which goes through `ReadConsoleW` and
    /// its line/UTF-16 handling — that path doesn't deliver raw keystrokes, which
    /// is why hotkeys weren't intercepted.
    pub fn read_console_input(buf: &mut [u8]) -> std::io::Result<usize> {
        use windows_sys::Win32::Storage::FileSystem::ReadFile;
        unsafe {
            let h = GetStdHandle(STD_INPUT_HANDLE);
            let mut read: u32 = 0;
            let ok = ReadFile(
                h,
                buf.as_mut_ptr().cast(),
                buf.len() as u32,
                &mut read,
                core::ptr::null_mut(),
            );
            if ok == 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(read as usize)
        }
    }
}

/// Read a chunk of raw terminal input (bytes / VT sequences).
#[cfg(windows)]
fn read_stdin(buf: &mut [u8]) -> std::io::Result<usize> {
    console::read_console_input(buf)
}

#[cfg(not(windows))]
fn read_stdin(buf: &mut [u8]) -> std::io::Result<usize> {
    std::io::stdin().lock().read(buf)
}

struct RawGuard {
    #[cfg(windows)]
    _win: console::Guard,
}

fn enable_raw() -> Result<RawGuard> {
    #[cfg(windows)]
    {
        let g = console::enable().context("enabling Windows console raw/VT mode")?;
        Ok(RawGuard { _win: g })
    }
    #[cfg(not(windows))]
    {
        crossterm::terminal::enable_raw_mode().context("enabling raw mode")?;
        Ok(RawGuard {})
    }
}

impl Drop for RawGuard {
    fn drop(&mut self) {
        #[cfg(not(windows))]
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

/// Serialize all writes to the local stdout (PTY mirror + title updates).
fn write_stdout(lock: &Mutex<()>, bytes: &[u8]) {
    // Tolerate a poisoned lock: the guarded data is `()`, so recovering keeps
    // stdout serialization working instead of cascading a panic into the core.
    let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(bytes);
    let _ = out.flush();
}

/// Reflect the push-to-talk state in the terminal title bar, plus an optional
/// desktop notification (OSC 9) on change. These OSC sequences are interpreted
/// by the terminal and never drawn, so they can't corrupt the mirrored shell.
fn write_status(lock: &Mutex<()>, title: &str, notify: Option<&str>) {
    let mut seq = format!("\x1b]0;{title}\x07");
    if let Some(n) = notify {
        seq.push_str(&format!("\x1b]9;{n}\x07"));
    }
    write_stdout(lock, seq.as_bytes());
}

// ──────────────────────────────── run ────────────────────────────────────

/// Clears this machine's "owned stream" marker (see [`protocol::owned`]) when the
/// host session ends — on any return path or panic — so the watcher stops
/// hiding/refusing a stream that is no longer live.
struct OwnedGuard(String);

impl OwnedGuard {
    fn new(stream_id: &str, code: &str) -> Self {
        protocol::owned::mark(stream_id, code);
        OwnedGuard(stream_id.to_string())
    }
}

impl Drop for OwnedGuard {
    fn drop(&mut self) {
        protocol::owned::unmark(&self.0);
    }
}

/// Start a host session and stream until the shell exits or the user quits.
pub async fn run(cfg: StreamConfig) -> Result<()> {
    let name = cfg.name.unwrap_or_else(default_name);
    let shell = cfg.shell.unwrap_or_else(default_shell);
    let shell_lbl = shell_label(&shell);
    let prefix = cfg.prefix;
    let voice_key = cfg.voice_key;

    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));

    // Build the host WebSocket URL.
    let url = format!("{}/host", cfg.relay.trim_end_matches('/'));
    println!("tcast · connecting to {url} …");

    let (ws, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .with_context(|| format!("connecting to relay at {url}"))?;
    let (mut write, mut read) = ws.split();

    // Handshake.
    write
        .send(bin(&HostToRelay::Hello(HostHello {
            name: name.clone(),
            shell: shell_lbl.clone(),
            public: cfg.public,
            cols,
            rows,
            auth_key: cfg.auth_key.clone(),
            chat: cfg.chat,
            version: PROTOCOL_VERSION.to_string(),
        })))
        .await
        .context("sending Hello")?;

    let (stream_id, code) = match read.next().await {
        Some(Ok(Message::Binary(b))) => match decode::<RelayToHost>(&b[..]) {
            Ok(RelayToHost::Welcome { stream_id, code, .. }) => (stream_id, code),
            Ok(RelayToHost::Error(e)) => {
                anyhow::bail!("relay refused the stream: {e}");
            }
            _ => anyhow::bail!("unexpected first frame from relay"),
        },
        _ => anyhow::bail!("relay closed the connection during handshake"),
    };

    print_banner(&name, &shell_lbl, cfg.public, &code, &stream_id, &cfg.relay, prefix);

    // Mark this stream as locally owned so `tcast watch` hides/refuses it; the
    // guard clears the marker when the session ends (any exit path or panic).
    let _owned = OwnedGuard::new(&stream_id, &code);

    // Optional push-to-talk voice. Capture runs on its own thread and feeds 20 ms
    // PCM frames through this channel; we forward them while PTT is active.
    let (audio_tx, mut audio_rx) = mpsc::unbounded_channel::<Vec<i16>>();
    let capture = if cfg.voice {
        match audio::Capture::start(audio_tx) {
            Ok(c) => {
                let vk = format!("Ctrl-{}", (voice_key | 0x40) as char);
                println!("tcast · voice ready — press {vk} to talk (toggles your mic)");
                Some(c)
            }
            Err(e) => {
                eprintln!("tcast · voice disabled: {e}");
                None
            }
        }
    } else {
        drop(audio_tx); // close the channel so the forward arm stays inert
        None
    };
    // Shared push-to-talk flag toggled from the stdin thread.
    let ptt = capture.as_ref().map(|c| c.enabled());

    // ── Spawn the PTY + shell ────────────────────────────────────────────
    let pty_system = native_pty_system();
    let pair = pty_system
        .openpty(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        })
        .context("opening PTY")?;

    let mut cmd = CommandBuilder::new(&shell);
    if let Ok(cwd) = std::env::current_dir() {
        cmd.cwd(cwd);
    }
    let child = pair
        .slave
        .spawn_command(cmd)
        .with_context(|| format!("spawning shell '{shell}'"))?;
    let mut killer = child.clone_killer();

    let mut pty_reader = pair.master.try_clone_reader().context("cloning PTY reader")?;
    let mut pty_writer = pair.master.take_writer().context("taking PTY writer")?;
    let master = pair.master;
    drop(pair.slave); // let the child own the only slave handle

    // Enter raw mode (restored on drop).
    let _raw = enable_raw()?;

    let stdout_lock = Arc::new(Mutex::new(()));

    // Channels.
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (ctl_tx, mut ctl_rx) = mpsc::unbounded_channel::<Ctl>();
    let (exit_tx, mut exit_rx) = oneshot::channel::<()>();

    // PTY reader thread: mirror to stdout + forward to relay.
    {
        let stdout_lock = stdout_lock.clone();
        std::thread::spawn(move || {
            let mut buf = [0u8; 8192];
            loop {
                match pty_reader.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let chunk = buf[..n].to_vec();
                        write_stdout(&stdout_lock, &chunk);
                        if out_tx.send(chunk).is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }

    // stdin reader thread: hotkeys + forward keystrokes into the PTY.
    {
        let ptt_thread = ptt.clone();
        std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            let mut prefix_armed = false;
            let mut paused = false;
            loop {
                let n = match read_stdin(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                let mut forward: Vec<u8> = Vec::with_capacity(n);
                for &byte in &buf[..n] {
                    if prefix_armed {
                        prefix_armed = false;
                        match byte {
                            b'p' | b'P' => {
                                paused = !paused;
                                let _ = ctl_tx.send(Ctl::Privacy(paused));
                            }
                            b'q' | b'Q' => {
                                let _ = ctl_tx.send(Ctl::Quit);
                                return;
                            }
                            b if b == prefix => forward.push(prefix),
                            _ => {} // unknown command: swallow
                        }
                    } else if ptt_thread.is_some() && byte == voice_key {
                        // Single-key push-to-talk toggle (only while voice is on).
                        if let Some(p) = &ptt_thread {
                            let on = !p.load(Ordering::Relaxed);
                            p.store(on, Ordering::Relaxed);
                        }
                    } else if byte == prefix {
                        prefix_armed = true;
                    } else {
                        forward.push(byte);
                    }
                }
                if !forward.is_empty()
                    && pty_writer
                        .write_all(&forward)
                        .and_then(|_| pty_writer.flush())
                        .is_err()
                {
                    break;
                }
            }
        });
    }

    // child waiter thread.
    {
        std::thread::spawn(move || {
            let mut child = child;
            let _ = child.wait();
            let _ = exit_tx.send(());
        });
    }

    // ── Async core ──────────────────────────────────────────────────────
    let mut resize_tick = tokio::time::interval(std::time::Duration::from_millis(400));
    let mut last_size = (cols, rows);
    let mut last_talking = false;

    loop {
        tokio::select! {
            // PTY output → relay.
            Some(chunk) = out_rx.recv() => {
                if write.send(bin(&HostToRelay::Output(chunk))).await.is_err() {
                    break;
                }
            }
            // Voice frames → relay (only yields while voice is on and PTT active).
            Some(frame) = audio_rx.recv() => {
                if write.send(bin(&HostToRelay::Audio(audio::encode_frame(&frame)))).await.is_err() {
                    break;
                }
            }
            // Hotkey actions.
            Some(ctl) = ctl_rx.recv() => match ctl {
                Ctl::Privacy(p) => {
                    // Viewers get a privacy overlay; we deliberately do NOT write to
                    // the host's own screen here. The reader thread is the only writer
                    // to local stdout, so the mirrored shell output is never corrupted
                    // by an interleaved write mid-VT-sequence.
                    let _ = write.send(bin(&HostToRelay::Privacy { paused: p })).await;
                }
                Ctl::Quit => {
                    let _ = write.send(bin(&HostToRelay::Bye)).await;
                    break;
                }
            },
            // Relay → host.
            msg = read.next() => match msg {
                Some(Ok(Message::Binary(b))) => match decode::<RelayToHost>(&b[..]) {
                    // Viewer count changes are reflected on the watcher side; the host
                    // keeps its screen free of overlay writes.
                    Ok(RelayToHost::Viewers(_)) => {}
                    Ok(RelayToHost::Ping) => { let _ = write.send(bin(&HostToRelay::Pong)).await; }
                    Ok(RelayToHost::Error(_)) | Ok(RelayToHost::Welcome { .. }) => {}
                    Err(_) => {}
                },
                Some(Ok(Message::Ping(p))) => { let _ = write.send(Message::Pong(p)).await; }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(_)) => break,
            },
            // Terminal resize → PTY + relay.
            _ = resize_tick.tick() => {
                if let Ok((c, r)) = crossterm::terminal::size()
                    && (c, r) != last_size {
                        last_size = (c, r);
                        let _ = master.resize(PtySize { rows: r, cols: c, pixel_width: 0, pixel_height: 0 });
                        let _ = write.send(bin(&HostToRelay::Resize { cols: c, rows: r })).await;
                    }
                // Show push-to-talk state in the title bar (voice streams only).
                if let Some(p) = &ptt {
                    let talking = p.load(Ordering::Relaxed);
                    if talking != last_talking {
                        last_talking = talking;
                        if talking {
                            write_status(&stdout_lock, "🎙 ON · tcast", Some("tcast — mic ON"));
                        } else {
                            write_status(&stdout_lock, "tcast", Some("tcast — mic off"));
                        }
                    } else if talking {
                        // Re-assert (the shell may reset the title at each prompt).
                        write_status(&stdout_lock, "🎙 ON · tcast", None);
                    }
                }
            }
            // Shell exited: flush any output still queued from the reader thread
            // so viewers see the final screen, then say goodbye.
            _ = &mut exit_rx => {
                while let Ok(chunk) = out_rx.try_recv() {
                    if write.send(bin(&HostToRelay::Output(chunk))).await.is_err() {
                        break;
                    }
                }
                let _ = write.send(bin(&HostToRelay::Bye)).await;
                break;
            }
        }
    }

    let _ = killer.kill();
    let _ = write.send(Message::Close(None)).await;
    drop(_raw);
    println!("\r\ntcast · stream ended.");
    Ok(())
}

fn print_banner(name: &str, shell: &str, public: bool, code: &str, stream_id: &str, relay: &str, prefix: u8) {
    let scope = if public {
        "PUBLIC (listed)"
    } else {
        "private (code only)"
    };
    println!("\n┌─ tcast ─────────────────────────────────────────");
    println!("│ streaming as : {name}  ({shell})");
    println!("│ visibility   : {scope}");
    println!("│ join code    : {code}");
    if public {
        println!("│ stream id    : {stream_id}");
    }
    println!("│");
    println!("│ viewers run  : tcast watch {code}");
    println!("│   (first point them at this relay:");
    println!("│    tcast config set-relay {relay})");
    println!("│");
    let pk = format!("Ctrl-{}", (prefix | 0x40) as char);
    println!("│ hotkeys      : {pk} p = privacy · {pk} q = quit");
    println!("│ stop also    : type `exit` (or Ctrl-D) in the shell");
    println!("└──────────────────────────────────────────────────\n");
}
