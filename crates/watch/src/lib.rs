//! tcast watch library (the "spectator").
//!
//! Connects to the relay, lets you browse live public streams (or join a private
//! one by code), and renders the chosen terminal read-only using a vt100
//! emulator drawn through `tui-term`.
//!
//! The CLI front-end calls [`run`] for the interactive browser/viewer and
//! [`list`] for a one-shot, non-interactive directory dump (`tcast list`).

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytes::Bytes;
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use futures_util::{SinkExt, StreamExt};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use protocol::{
    decode, encode, RelayToWatch, StreamInfo, WatchHello, WatchToRelay, PROTOCOL_VERSION,
};

/// Everything needed to open the spectator UI.
pub struct WatchConfig {
    /// Relay base URL (ws:// or wss://). `/watch` is appended automatically.
    pub relay: String,
    /// Join this code / stream id directly, skipping the browser.
    pub target: Option<String>,
    /// Display name used to label this viewer's chat messages.
    pub name: Option<String>,
    /// Allow watching a stream owned by this machine (used by `tcast chat`).
    pub allow_self: bool,
    /// Open the chat pane immediately (used by `tcast chat`).
    pub chat_open: bool,
    /// Show only the chat (no terminal mirror) — used by `tcast chat` so the host
    /// gets a pure chat window instead of their own mirrored screen.
    pub chat_only: bool,
}

/// Events from the network task to the UI.
enum Net {
    Connected,
    Disconnected(String),
    Msg(RelayToWatch),
}

#[derive(PartialEq)]
enum Screen {
    Browsing,
    Watching,
}

struct App {
    relay: String,
    screen: Screen,
    status: String,
    connected: bool,
    streams: Vec<StreamInfo>,
    list_state: ListState,
    joined: Option<StreamInfo>,
    parser: Option<vt100::Parser>,
    paused: bool,
    /// Target to (re)join on the next successful (re)connect, if any.
    pending_target: Option<String>,
    /// The exact target string last joined (a code or a public id), used to
    /// auto-rejoin after a reconnect.
    last_target: Option<String>,
    /// stream_ids hosted by this same machine — filtered out of the browse list
    /// so you never end up watching your own stream.
    owned_ids: HashSet<String>,
    /// Chat scrollback: (from, text, ts_unix), capped to the most recent lines.
    chat_log: Vec<(String, String, u64)>,
    /// Current chat compose buffer.
    chat_input: String,
    /// Whether keystrokes are captured into `chat_input` instead of UI commands.
    composing: bool,
    /// Whether the chat pane is shown.
    chat_open: bool,
    /// Render only the chat (no terminal) — the `tcast chat` host window.
    chat_only: bool,
    /// Lazily-started speaker playback for host voice (None until the first
    /// frame arrives, or while muted).
    playback: Option<audio::Playback>,
    /// Whether the viewer muted incoming voice.
    muted: bool,
    /// Whether any voice frame has been received (even if playback failed).
    audio_seen: bool,
    /// If opening the speaker failed, the reason — surfaced in the footer so
    /// audio isn't silently lost.
    audio_err: Option<String>,
}

impl App {
    fn selected(&self) -> Option<&StreamInfo> {
        self.list_state.selected().and_then(|i| self.streams.get(i))
    }
}

fn bin<T: Serialize>(msg: &T) -> Message {
    Message::Binary(Bytes::from(encode(msg)))
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn fmt_uptime(started: u64) -> String {
    let now = now_unix();
    let s = now.saturating_sub(started);
    if s >= 3600 {
        format!("{}h{:02}m", s / 3600, (s % 3600) / 60)
    } else if s >= 60 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{s}s")
    }
}

// ──────────────────────────── non-interactive list ───────────────────────

/// Connect once, fetch the public stream directory, print it, and exit.
/// Backs `tcast list [--json]` — no TUI, scriptable.
pub async fn list(relay: String, json: bool) -> Result<()> {
    let url = format!("{}/watch", relay.trim_end_matches('/'));
    let (ws, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .with_context(|| format!("connecting to relay at {url}"))?;
    let (mut write, mut read) = ws.split();

    write
        .send(bin(&WatchToRelay::Hello(WatchHello {
            version: PROTOCOL_VERSION.to_string(),
            name: None,
            cols: 0,
            rows: 0,
        })))
        .await
        .context("sending Hello")?;

    // Wait for the handshake to be accepted before asking for the list.
    loop {
        match read.next().await {
            Some(Ok(Message::Binary(b))) => match decode::<RelayToWatch>(&b[..]) {
                Ok(RelayToWatch::Welcome) => break,
                Ok(RelayToWatch::Error(e)) => anyhow::bail!("relay refused the connection: {e}"),
                _ => {} // ignore anything else until Welcome
            },
            Some(Ok(Message::Ping(p))) => {
                let _ = write.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            _ => anyhow::bail!("relay closed the connection during handshake"),
        }
    }

    write
        .send(bin(&WatchToRelay::List))
        .await
        .context("requesting stream list")?;

    let streams = loop {
        match read.next().await {
            Some(Ok(Message::Binary(b))) => match decode::<RelayToWatch>(&b[..]) {
                Ok(RelayToWatch::Streams(v)) => break v,
                Ok(RelayToWatch::Error(e)) => anyhow::bail!("relay error: {e}"),
                _ => {}
            },
            Some(Ok(Message::Ping(p))) => {
                let _ = write.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            _ => anyhow::bail!("relay closed the connection before sending the list"),
        }
    };

    let owned: HashSet<String> = protocol::owned::list().into_iter().map(|(id, _)| id).collect();
    let streams: Vec<StreamInfo> = streams
        .into_iter()
        .filter(|s| !owned.contains(&s.stream_id))
        .collect();

    if json {
        println!("{}", serde_json::to_string_pretty(&streams)?);
        return Ok(());
    }

    if streams.is_empty() {
        println!("No public streams are live right now.");
        return Ok(());
    }

    println!(
        "{:<18}{:<12}{:>4}  {:>9}  {:<8}{}",
        "NAME", "SHELL", "👁", "SIZE", "UPTIME", "ID"
    );
    for s in &streams {
        let size = format!("{}x{}", s.cols, s.rows);
        println!(
            "{:<18}{:<12}{:>4}  {:>9}  {:<8}{}",
            trunc(&s.name, 17),
            trunc(&s.shell, 11),
            s.viewers,
            size,
            fmt_uptime(s.started_unix),
            s.stream_id,
        );
    }
    Ok(())
}

// ──────────────────────────────── run ────────────────────────────────────

/// Open the interactive spectator UI: browse public streams, or (if
/// `cfg.target` is set) join that code/id directly.
pub async fn run(cfg: WatchConfig) -> Result<()> {
    // Same-machine guard: don't let the operator watch their own stream — unless
    // this is `tcast chat`, which deliberately joins your own stream.
    let owned_ids: HashSet<String> = if cfg.allow_self {
        HashSet::new()
    } else {
        let owned = protocol::owned::list();
        if let Some(t) = &cfg.target
            && owned.iter().any(|(id, code)| id == t || code == t)
        {
            anyhow::bail!("that's your own stream — you can't watch yourself");
        }
        owned.into_iter().map(|(id, _)| id).collect()
    };

    let url = format!("{}/watch", cfg.relay.trim_end_matches('/'));

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<WatchToRelay>();
    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<Net>();

    tokio::spawn(net_task(url, cfg.name.clone(), cmd_rx, ui_tx));

    let mut app = App {
        relay: cfg.relay.clone(),
        screen: Screen::Browsing,
        status: "connecting…".to_string(),
        connected: false,
        streams: Vec::new(),
        list_state: ListState::default(),
        joined: None,
        parser: None,
        paused: false,
        pending_target: cfg.target.clone(),
        last_target: None,
        owned_ids,
        chat_log: Vec::new(),
        chat_input: String::new(),
        composing: false,
        chat_open: cfg.chat_open,
        chat_only: cfg.chat_only,
        playback: None,
        muted: false,
        audio_seen: false,
        audio_err: None,
    };

    let mut terminal = ratatui::init();
    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(250));

    let res = loop {
        if let Err(e) = terminal.draw(|f| ui(f, &mut app)) {
            break Err(anyhow::anyhow!(e));
        }
        tokio::select! {
            ev = events.next() => {
                match ev {
                    Some(Ok(Event::Key(k))) if k.kind == KeyEventKind::Press => {
                        if handle_key(&mut app, &cmd_tx, k.code, k.modifiers) {
                            break Ok(());
                        }
                    }
                    // A persistently-erroring (or terminated) event stream would
                    // otherwise busy-spin; quit cleanly instead.
                    Some(Err(_)) | None => break Ok(()),
                    _ => {}
                }
            }
            net = ui_rx.recv() => {
                if let Some(n) = net {
                    handle_net(&mut app, &cmd_tx, n);
                    // Coalesce a burst of network messages into a single redraw.
                    while let Ok(n) = ui_rx.try_recv() {
                        handle_net(&mut app, &cmd_tx, n);
                    }
                }
            }
            _ = tick.tick() => {}
        }
    };

    ratatui::restore();
    if let Err(e) = &res {
        eprintln!("error: {e}");
    } else if !app.status.is_empty() {
        println!("{}", app.status);
    }
    res
}

/// Returns true if the app should quit.
fn handle_key(
    app: &mut App,
    cmd_tx: &mpsc::UnboundedSender<WatchToRelay>,
    code: KeyCode,
    mods: KeyModifiers,
) -> bool {
    if mods.contains(KeyModifiers::CONTROL)
        && matches!(code, KeyCode::Char('c') | KeyCode::Char('C'))
    {
        return true;
    }

    match app.screen {
        Screen::Browsing => match code {
            KeyCode::Char('q') | KeyCode::Esc => return true,
            KeyCode::Down | KeyCode::Char('j') => move_sel(app, 1),
            KeyCode::Up | KeyCode::Char('k') => move_sel(app, -1),
            KeyCode::Char('r') => {
                let _ = cmd_tx.send(WatchToRelay::List);
                app.status = "refreshing…".into();
            }
            KeyCode::Enter => {
                if let Some(s) = app.selected() {
                    let target = s.stream_id.clone();
                    app.last_target = Some(target.clone());
                    let _ = cmd_tx.send(WatchToRelay::Join { target });
                    app.status = "joining…".into();
                }
            }
            _ => {}
        },
        Screen::Watching => {
            if app.composing {
                // Keystrokes go to the chat input, not UI commands.
                match code {
                    KeyCode::Esc => {
                        app.composing = false;
                        app.chat_input.clear();
                    }
                    KeyCode::Enter => {
                        let text = app.chat_input.trim().to_string();
                        if !text.is_empty() {
                            let _ = cmd_tx.send(WatchToRelay::Chat { text });
                        }
                        app.chat_input.clear();
                    }
                    KeyCode::Backspace => {
                        app.chat_input.pop();
                    }
                    KeyCode::Char(c) => app.chat_input.push(c),
                    _ => {}
                }
            } else {
                match code {
                    KeyCode::Char('c') => {
                        app.chat_open = true;
                        app.composing = true;
                    }
                    KeyCode::Char('m') => {
                        app.muted = !app.muted;
                        if app.muted {
                            app.playback = None; // release the output device
                        }
                    }
                    KeyCode::Char('q') | KeyCode::Esc | KeyCode::Backspace => {
                        let _ = cmd_tx.send(WatchToRelay::Leave);
                        let _ = cmd_tx.send(WatchToRelay::List);
                        app.screen = Screen::Browsing;
                        app.parser = None;
                        app.joined = None;
                        app.paused = false;
                        app.last_target = None; // deliberate leave: don't auto-rejoin
                        app.status = "browsing".into();
                    }
                    _ => {}
                }
            }
        }
    }
    false
}

fn move_sel(app: &mut App, delta: i32) {
    if app.streams.is_empty() {
        app.list_state.select(None);
        return;
    }
    let len = app.streams.len() as i32;
    let cur = app.list_state.selected().unwrap_or(0) as i32;
    let next = (cur + delta).rem_euclid(len);
    app.list_state.select(Some(next as usize));
}

fn handle_net(app: &mut App, cmd_tx: &mpsc::UnboundedSender<WatchToRelay>, net: Net) {
    match net {
        Net::Connected => {
            app.connected = true;
            if let Some(target) = app.pending_target.take() {
                app.last_target = Some(target.clone());
                let _ = cmd_tx.send(WatchToRelay::Join { target });
                app.status = "joining…".into();
            } else {
                let _ = cmd_tx.send(WatchToRelay::List);
                app.status = "browsing".into();
            }
        }
        Net::Disconnected(e) => {
            app.connected = false;
            app.status = format!("disconnected: {e} — reconnecting…");
            // Don't leave a misleading frozen "LIVE" frame: drop back to the
            // browser and remember the stream so the reconnect can auto-rejoin it.
            if app.screen == Screen::Watching {
                app.pending_target = app.last_target.clone();
                app.joined = None;
                app.parser = None;
                app.paused = false;
                app.screen = Screen::Browsing;
            }
        }
        Net::Msg(msg) => match msg {
            RelayToWatch::Streams(v) => {
                app.streams = v
                    .into_iter()
                    .filter(|s| !app.owned_ids.contains(&s.stream_id))
                    .collect();
                if app.streams.is_empty() {
                    app.list_state.select(None);
                } else {
                    let sel = app
                        .list_state
                        .selected()
                        .unwrap_or(0)
                        .min(app.streams.len() - 1);
                    app.list_state.select(Some(sel));
                }
            }
            RelayToWatch::Joined { info, snapshot } => {
                let mut parser = vt100::Parser::new(info.rows.max(1), info.cols.max(1), 0);
                parser.process(&snapshot);
                app.parser = Some(parser);
                app.joined = Some(info);
                app.paused = false;
                app.screen = Screen::Watching;
                app.status.clear();
            }
            RelayToWatch::Output(b) => {
                if let Some(p) = app.parser.as_mut() {
                    p.process(&b);
                }
            }
            RelayToWatch::Audio(bytes) => {
                app.audio_seen = true;
                if !app.muted {
                    // Lazily open the speaker on the first voice frame; surface a
                    // failure instead of silently dropping the audio.
                    if app.playback.is_none() && app.audio_err.is_none() {
                        match audio::Playback::start() {
                            Ok(pb) => app.playback = Some(pb),
                            Err(e) => app.audio_err = Some(e.to_string()),
                        }
                    }
                    if let Some(pb) = app.playback.as_mut() {
                        pb.push(&audio::decode_frame(&bytes));
                    }
                }
            }
            RelayToWatch::Resize { cols, rows } => {
                if let Some(p) = app.parser.as_mut() {
                    p.screen_mut().set_size(rows.max(1), cols.max(1));
                }
                if let Some(info) = app.joined.as_mut() {
                    info.cols = cols;
                    info.rows = rows;
                }
            }
            RelayToWatch::Privacy { paused } => app.paused = paused,
            RelayToWatch::Viewers(n) => {
                if let Some(info) = app.joined.as_mut() {
                    info.viewers = n;
                }
            }
            RelayToWatch::Chat { from, text, ts_unix } => {
                app.chat_log.push((from, text, ts_unix));
                let overflow = app.chat_log.len().saturating_sub(200);
                if overflow > 0 {
                    app.chat_log.drain(0..overflow);
                }
                app.chat_open = true; // surface chat even if the pane was closed
            }
            RelayToWatch::Ended => {
                app.screen = Screen::Browsing;
                app.parser = None;
                app.joined = None;
                app.paused = false;
                app.last_target = None; // stream is gone: don't auto-rejoin
                app.status = "stream ended".into();
                let _ = cmd_tx.send(WatchToRelay::List);
            }
            RelayToWatch::Error(e) => {
                app.status = e;
            }
            RelayToWatch::Welcome | RelayToWatch::Ping => {}
        },
    }
}

// ──────────────────────────────── UI ─────────────────────────────────────

fn ui(f: &mut Frame, app: &mut App) {
    match app.screen {
        Screen::Browsing => ui_browse(f, app),
        Screen::Watching => ui_watch(f, app),
    }
}

fn ui_browse(f: &mut Frame, app: &mut App) {
    let chunks = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(1),
    ])
    .split(f.area());

    let dot = if app.connected { "●" } else { "○" };
    let header = Line::from(vec![
        Span::styled(
            format!(" {dot} tcast "),
            Style::new().bold().fg(Color::Cyan),
        ),
        Span::styled(format!("· {} ", app.relay), Style::new().fg(Color::DarkGray)),
        Span::raw(format!("· {}", app.status)),
    ]);
    f.render_widget(Paragraph::new(header), chunks[0]);

    if app.streams.is_empty() {
        let msg = Paragraph::new(
            "No public streams are live right now.\n\n\
             • press r to refresh\n\
             • to watch a private stream, pass its code on the CLI:\n    tcast watch <code>",
        )
        .wrap(Wrap { trim: false })
        .block(Block::default().borders(Borders::ALL).title(" live streams "));
        f.render_widget(msg, chunks[1]);
    } else {
        let items: Vec<ListItem> = app
            .streams
            .iter()
            .map(|s| {
                let size = format!("{}x{}", s.cols, s.rows);
                let line = Line::from(vec![
                    Span::styled("🔴 ", Style::new().fg(Color::Red)),
                    Span::styled(format!("{:<16}", trunc(&s.name, 16)), Style::new().bold()),
                    Span::styled(
                        format!("{:<10}", trunc(&s.shell, 10)),
                        Style::new().fg(Color::Yellow),
                    ),
                    Span::styled(format!("{:>3} 👁  ", s.viewers), Style::new().fg(Color::Green)),
                    Span::styled(format!("{size:>9}  "), Style::new().fg(Color::DarkGray)),
                    Span::styled(fmt_uptime(s.started_unix), Style::new().fg(Color::DarkGray)),
                ]);
                ListItem::new(line)
            })
            .collect();

        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!(" live streams ({}) ", app.streams.len())),
            )
            .highlight_symbol("➤ ")
            .highlight_style(Style::new().bold().bg(Color::Cyan).fg(Color::Black));
        f.render_stateful_widget(list, chunks[1], &mut app.list_state);
    }

    let footer = Line::from(vec![Span::styled(
        " ↑/↓ move · Enter watch · r refresh · q quit ",
        Style::new().fg(Color::Black).bg(Color::DarkGray),
    )]);
    f.render_widget(Paragraph::new(footer), chunks[2]);
}

fn ui_watch(f: &mut Frame, app: &mut App) {
    let chunks = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(1),
    ])
    .split(f.area());

    let info = app.joined.as_ref();
    let title = if let Some(i) = info {
        Line::from(vec![
            Span::styled(
                " 🔴 LIVE ",
                Style::new().bold().fg(Color::White).bg(Color::Red),
            ),
            Span::styled(format!(" {} ", i.name), Style::new().bold()),
            Span::styled(format!("({}) ", i.shell), Style::new().fg(Color::Yellow)),
            Span::styled(format!("· {} 👁 ", i.viewers), Style::new().fg(Color::Green)),
            Span::styled(
                format!("· {} ", fmt_uptime(i.started_unix)),
                Style::new().fg(Color::DarkGray),
            ),
            Span::styled(
                format!("· {}x{} ", i.cols, i.rows),
                Style::new().fg(Color::DarkGray),
            ),
        ])
    } else {
        Line::from(" connecting… ")
    };
    f.render_widget(Paragraph::new(title), chunks[0]);

    if app.chat_only {
        // Pure chat window (the `tcast chat` host view): no terminal mirror.
        render_chat(f, chunks[1], app);
    } else {
        // Body: the read-only terminal, plus an optional chat pane on the right.
        let (term_area, chat_area) = if app.chat_open {
            let cols =
                Layout::horizontal([Constraint::Min(20), Constraint::Length(34)]).split(chunks[1]);
            (cols[0], Some(cols[1]))
        } else {
            (chunks[1], None)
        };

        let block = Block::default()
            .borders(Borders::ALL)
            .title(" read-only ")
            .border_style(Style::new().fg(Color::DarkGray));
        let inner = block.inner(term_area);
        f.render_widget(block, term_area);

        if let Some(parser) = app.parser.as_ref() {
            let pseudo = tui_term::widget::PseudoTerminal::new(parser.screen());
            f.render_widget(pseudo, inner);
        } else {
            f.render_widget(Paragraph::new("waiting for output…"), inner);
        }

        if let Some(chat_area) = chat_area {
            render_chat(f, chat_area, app);
        }

        if app.paused {
            let overlay = centered_rect(60, 20, term_area);
            f.render_widget(Clear, overlay);
            let p = Paragraph::new("🙈  PRIVACY\n\nThe host paused the stream.")
                .alignment(Alignment::Center)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(Style::new().fg(Color::Magenta)),
                );
            f.render_widget(p, overlay);
        }
    }

    let footer = if app.composing {
        Line::from(vec![
            Span::styled(" chat> ", Style::new().fg(Color::Black).bg(Color::Cyan)),
            Span::raw(format!("{}\u{2588}", app.chat_input)),
            Span::styled("  (Enter send · Esc cancel)", Style::new().fg(Color::DarkGray)),
        ])
    } else {
        let mut hint = String::from(" c chat · ");
        if let Some(e) = &app.audio_err {
            hint.push_str(&format!("🔇 audio: {e} · "));
        } else if app.muted {
            hint.push_str("🔇 m unmute · ");
        } else if app.playback.is_some() {
            hint.push_str("🔊 m mute · ");
        } else if app.audio_seen {
            hint.push_str("🔊 starting… · ");
        }
        hint.push_str("q/Esc back · Ctrl-C quit ");
        Line::from(vec![Span::styled(
            hint,
            Style::new().fg(Color::Black).bg(Color::DarkGray),
        )])
    };
    f.render_widget(Paragraph::new(footer), chunks[2]);
}

/// Render the chat scrollback into `area`, wrapping each message and keeping the
/// newest lines visible at the bottom (overflow clips the oldest, not the newest).
fn render_chat(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" chat ")
        .border_style(Style::new().fg(Color::Cyan));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let width = inner.width.max(1) as usize;
    let h = inner.height.max(1) as usize;
    let mut lines: Vec<Line> = Vec::new();
    for (from, text, _) in &app.chat_log {
        wrap_chat_message(from, text, width, &mut lines);
    }
    let start = lines.len().saturating_sub(h);
    f.render_widget(Paragraph::new(lines[start..].to_vec()), inner);
}

/// Char-wrap one chat message to `width`, styling the sender prefix on its first
/// line. Appends the resulting display lines to `out`.
fn wrap_chat_message(from: &str, text: &str, width: usize, out: &mut Vec<Line<'static>>) {
    let prefix = format!("{from}: ");
    let plen = prefix.chars().count();
    let full: Vec<char> = format!("{prefix}{text}").chars().collect();
    if full.is_empty() {
        return;
    }
    let w = width.max(1);
    let mut i = 0;
    while i < full.len() {
        let end = (i + w).min(full.len());
        let seg: String = full[i..end].iter().collect();
        if i == 0 {
            let pc = plen.min(seg.chars().count());
            let p: String = seg.chars().take(pc).collect();
            let r: String = seg.chars().skip(pc).collect();
            let mut spans = vec![Span::styled(p, Style::new().fg(Color::Cyan).bold())];
            if !r.is_empty() {
                spans.push(Span::raw(r));
            }
            out.push(Line::from(spans));
        } else {
            out.push(Line::from(seg));
        }
        i = end;
    }
}

fn trunc(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

fn centered_rect(pct_x: u16, pct_y: u16, area: Rect) -> Rect {
    let vert = Layout::vertical([
        Constraint::Percentage((100 - pct_y) / 2),
        Constraint::Percentage(pct_y),
        Constraint::Percentage((100 - pct_y) / 2),
    ])
    .split(area);
    Layout::horizontal([
        Constraint::Percentage((100 - pct_x) / 2),
        Constraint::Percentage(pct_x),
        Constraint::Percentage((100 - pct_x) / 2),
    ])
    .split(vert[1])[1]
}

// ─────────────────────────────── network ─────────────────────────────────

/// How a single connection session ended.
enum SessionEnd {
    /// The UI side is gone; stop the task entirely.
    Shutdown,
    /// The connection was lost; reconnect. `was_connected` is true if we got past
    /// the handshake (so the backoff can reset).
    Lost { was_connected: bool },
}

async fn net_task(
    url: String,
    name: Option<String>,
    mut cmd_rx: mpsc::UnboundedReceiver<WatchToRelay>,
    ui_tx: mpsc::UnboundedSender<Net>,
) {
    let mut backoff = 1u64;
    loop {
        match connect_once(&url, &name, &mut cmd_rx, &ui_tx).await {
            SessionEnd::Shutdown => return,
            SessionEnd::Lost { was_connected } => {
                if was_connected {
                    backoff = 1; // a real session ended; retry promptly first
                }
                tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(10);
            }
        }
    }
}

/// Runs one connection: connect, handshake, then pump commands/messages until the
/// link drops or the UI goes away. The unbounded `cmd_rx` is borrowed (not moved)
/// so queued commands survive across reconnects.
async fn connect_once(
    url: &str,
    name: &Option<String>,
    cmd_rx: &mut mpsc::UnboundedReceiver<WatchToRelay>,
    ui_tx: &mpsc::UnboundedSender<Net>,
) -> SessionEnd {
    let (ws, _resp) = match tokio_tungstenite::connect_async(url).await {
        Ok(x) => x,
        Err(e) => {
            let _ = ui_tx.send(Net::Disconnected(format!("{e}")));
            return SessionEnd::Lost { was_connected: false };
        }
    };
    let (mut write, mut read) = ws.split();

    let hello = WatchToRelay::Hello(WatchHello {
        version: PROTOCOL_VERSION.to_string(),
        name: name.clone(),
        cols: 0,
        rows: 0,
    });
    if write.send(bin(&hello)).await.is_err() {
        let _ = ui_tx.send(Net::Disconnected("handshake send failed".into()));
        return SessionEnd::Lost { was_connected: false };
    }

    match read.next().await {
        Some(Ok(Message::Binary(b))) => match decode::<RelayToWatch>(&b[..]) {
            Ok(RelayToWatch::Welcome) => {
                let _ = ui_tx.send(Net::Connected);
            }
            Ok(RelayToWatch::Error(e)) => {
                let _ = ui_tx.send(Net::Disconnected(e));
                return SessionEnd::Lost { was_connected: false };
            }
            _ => {
                let _ = ui_tx.send(Net::Disconnected("bad handshake".into()));
                return SessionEnd::Lost { was_connected: false };
            }
        },
        _ => {
            let _ = ui_tx.send(Net::Disconnected("no handshake".into()));
            return SessionEnd::Lost { was_connected: false };
        }
    }

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => match cmd {
                Some(c) => {
                    if write.send(bin(&c)).await.is_err() {
                        let _ = ui_tx.send(Net::Disconnected("send failed".into()));
                        return SessionEnd::Lost { was_connected: true };
                    }
                }
                None => return SessionEnd::Shutdown, // cmd_tx dropped: UI is gone
            },
            msg = read.next() => match msg {
                Some(Ok(Message::Binary(b))) => {
                    if let Ok(m) = decode::<RelayToWatch>(&b[..])
                        && ui_tx.send(Net::Msg(m)).is_err()
                    {
                        return SessionEnd::Shutdown;
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = write.send(Message::Pong(p)).await;
                }
                Some(Ok(Message::Close(_))) | None => {
                    let _ = ui_tx.send(Net::Disconnected("relay closed".into()));
                    return SessionEnd::Lost { was_connected: true };
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    let _ = ui_tx.send(Net::Disconnected(format!("{e}")));
                    return SessionEnd::Lost { was_connected: true };
                }
            }
        }
    }
}
