//! share-terminal watch (the "spectator").
//!
//! Connects to the relay, lets you browse live public streams (or join a private
//! one by code), and renders the chosen terminal read-only using a vt100
//! emulator drawn through `tui-term`.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bytes::Bytes;
use clap::Parser as ClapParser;
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

#[derive(ClapParser)]
#[command(name = "share-terminal-watch", about = "Watch a shared terminal")]
struct Args {
    /// Relay base URL (ws:// or wss://). "/watch" is appended automatically.
    #[arg(default_value = "ws://127.0.0.1:4455")]
    relay: String,
    /// Join this code / stream id directly, skipping the browser.
    target: Option<String>,
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let url = format!("{}/watch", args.relay.trim_end_matches('/'));

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<WatchToRelay>();
    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<Net>();

    tokio::spawn(net_task(url, cmd_rx, ui_tx));

    let mut app = App {
        relay: args.relay.clone(),
        screen: Screen::Browsing,
        status: "connecting…".to_string(),
        connected: false,
        streams: Vec::new(),
        list_state: ListState::default(),
        joined: None,
        parser: None,
        paused: false,
        pending_target: args.target.clone(),
        last_target: None,
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
        Screen::Watching => match code {
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
        },
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
                app.streams = v;
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
            format!(" {dot} share-terminal "),
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
             • to watch a private stream, pass its code on the CLI:\n    share-terminal-watch <relay> <code>",
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

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" read-only ")
        .border_style(Style::new().fg(Color::DarkGray));
    let inner = block.inner(chunks[1]);
    f.render_widget(block, chunks[1]);

    if let Some(parser) = app.parser.as_ref() {
        let pseudo = tui_term::widget::PseudoTerminal::new(parser.screen());
        f.render_widget(pseudo, inner);
    } else {
        f.render_widget(Paragraph::new("waiting for output…"), inner);
    }

    if app.paused {
        let overlay = centered_rect(60, 20, chunks[1]);
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

    let footer = Line::from(vec![Span::styled(
        " q/Esc back to list · Ctrl-C quit ",
        Style::new().fg(Color::Black).bg(Color::DarkGray),
    )]);
    f.render_widget(Paragraph::new(footer), chunks[2]);
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
    mut cmd_rx: mpsc::UnboundedReceiver<WatchToRelay>,
    ui_tx: mpsc::UnboundedSender<Net>,
) {
    let mut backoff = 1u64;
    loop {
        match connect_once(&url, &mut cmd_rx, &ui_tx).await {
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
