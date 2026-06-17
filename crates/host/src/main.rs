//! share-terminal host (the "streamer").
//!
//! Spawns the user's shell inside a PTY, mirrors its output to the local
//! terminal *and* to the relay, and forwards local keystrokes into the PTY so
//! the session stays fully usable. Spectators are read-only — nothing they do
//! can ever reach this PTY.
//!
//! Hotkeys (prefix = Ctrl-O):
//!   Ctrl-O p   toggle privacy (pause/resume what viewers see)
//!   Ctrl-O q   stop streaming and quit
//!   Ctrl-O Ctrl-O  send a literal Ctrl-O to the shell

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser as ClapParser;
use futures_util::{SinkExt, StreamExt};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

use protocol::{decode, encode, HostHello, HostToRelay, RelayToHost, PROTOCOL_VERSION};

/// Hotkey prefix (Ctrl-O).
const PREFIX: u8 = 0x0F;

#[derive(ClapParser)]
#[command(name = "share-terminal-host", about = "Stream your terminal with share-terminal")]
struct Args {
    /// Relay base URL (ws:// or wss://). "/host" is appended automatically.
    #[arg(default_value = "ws://127.0.0.1:4455")]
    relay: String,
    /// Display name shown to viewers (default: your username).
    #[arg(long)]
    name: Option<String>,
    /// Shell to launch (default: powershell.exe on Windows, $SHELL on Unix).
    #[arg(long)]
    shell: Option<String>,
    /// List this stream in the public directory (default: private, code-only).
    #[arg(long)]
    public: bool,
    /// Auth key, if the relay requires one.
    #[arg(long)]
    auth_key: Option<String>,
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

// ──────────────────────────────── main ──────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let name = args.name.unwrap_or_else(default_name);
    let shell = args.shell.unwrap_or_else(default_shell);
    let shell_lbl = shell_label(&shell);

    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));

    // Build the host WebSocket URL.
    let url = format!("{}/host", args.relay.trim_end_matches('/'));
    println!("share-terminal · connecting to {url} …");

    let (ws, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .with_context(|| format!("connecting to relay at {url}"))?;
    let (mut write, mut read) = ws.split();

    // Handshake.
    write
        .send(bin(&HostToRelay::Hello(HostHello {
            name: name.clone(),
            shell: shell_lbl.clone(),
            public: args.public,
            cols,
            rows,
            auth_key: args.auth_key.clone(),
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

    print_banner(&name, &shell_lbl, args.public, &code, &stream_id, &args.relay);

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
        std::thread::spawn(move || {
            let mut stdin = std::io::stdin().lock();
            let mut buf = [0u8; 4096];
            let mut prefix_armed = false;
            let mut paused = false;
            loop {
                let n = match stdin.read(&mut buf) {
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
                            PREFIX => forward.push(PREFIX),
                            _ => {} // unknown command: swallow
                        }
                    } else if byte == PREFIX {
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

    loop {
        tokio::select! {
            // PTY output → relay.
            Some(chunk) = out_rx.recv() => {
                if write.send(bin(&HostToRelay::Output(chunk))).await.is_err() {
                    break;
                }
            }
            // Hotkey actions.
            Some(ctl) = ctl_rx.recv() => match ctl {
                Ctl::Privacy(p) => {
                    let _ = write.send(bin(&HostToRelay::Privacy { paused: p })).await;
                    let note = if p { "● PRIVACY ON — viewers paused" } else { "○ privacy off — live" };
                    write_stdout(&stdout_lock, format!("\x1b]0;share-terminal: {note}\x07").as_bytes());
                }
                Ctl::Quit => {
                    let _ = write.send(bin(&HostToRelay::Bye)).await;
                    break;
                }
            },
            // Relay → host.
            msg = read.next() => match msg {
                Some(Ok(Message::Binary(b))) => match decode::<RelayToHost>(&b[..]) {
                    Ok(RelayToHost::Viewers(n)) => {
                        write_stdout(&stdout_lock, format!("\x1b]0;share-terminal: {n} watching · {code}\x07").as_bytes());
                    }
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
    println!("\r\nshare-terminal · stream ended.");
    Ok(())
}

fn print_banner(name: &str, shell: &str, public: bool, code: &str, stream_id: &str, relay: &str) {
    let scope = if public {
        "PUBLIC (listed)"
    } else {
        "private (code only)"
    };
    println!("\n┌─ share-terminal ────────────────────────────────");
    println!("│ streaming as : {name}  ({shell})");
    println!("│ visibility   : {scope}");
    println!("│ join code    : {code}");
    if public {
        println!("│ stream id    : {stream_id}");
    }
    println!("│");
    println!("│ viewers run  : share-terminal-watch {relay}");
    println!("│   then pick you from the list, or join directly:");
    println!("│   share-terminal-watch {relay} {code}");
    println!("│");
    println!("│ hotkeys      : Ctrl-O p = privacy · Ctrl-O q = quit");
    println!("└──────────────────────────────────────────────────\n");
}
