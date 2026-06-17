# share-terminal

**Twitch/Kick, but for terminals.** A streamer shares their live terminal; spectators
open a CLI, browse the live streams (or join a private one by code) and watch in
real time — read-only.

```
┌─ HOST (streamer) ─┐        ┌─ RELAY (server) ─┐        ┌─ WATCH (spectator) ─┐
│ your shell in a   │  wss   │ registry + fan-  │  wss   │ ratatui browser +   │
│ PTY, mirrored     │ ─────▶ │ out + vt100      │ ─────▶ │ vt100/tui-term view │
│ locally + sent up │        │ snapshots        │        │ (read-only)         │
└───────────────────┘        └──────────────────┘        └─────────────────────┘
```

## Why it's safe to watch (and to stream)

- **Read-only by construction.** The wire protocol has *no* message that carries a
  spectator's keystrokes toward a host. A viewer literally cannot type into your
  shell — it's a property of the types, not a runtime check.
- **Privacy toggle.** Press `Ctrl-O p` in the host to pause what viewers see
  (e.g. while typing a password); press again to resume.
- **Private by default.** A stream is reachable only by its generated code unless
  you pass `--public` to list it in the global directory.
- **Optional host auth.** Operators can require a shared key (`--auth-key`) so not
  just anyone can stream through your relay.

## Workspace layout

| crate      | what it is                                                              |
|------------|------------------------------------------------------------------------|
| `protocol` | shared message types + MessagePack codec (the wire contract)           |
| `host`     | spawns your shell in a PTY, mirrors output locally and to the relay     |
| `relay`    | central server: stream registry, fan-out, late-join screen snapshots    |
| `watch`    | TUI spectator: browse streams, watch one rendered read-only             |

## Build

### Windows
Needs the MSVC toolchain **with the Windows SDK** (for `kernel32.lib` etc.):

```powershell
# one-time, if missing:
winget install --id Microsoft.WindowsSDK.10.0.26100 -e

# build (loads the VS dev environment first):
. .\tools\msvcenv.ps1
cargo build --release
```

> A GNU-toolchain fallback (`rust-toolchain.toml` + `.cargo/config.toml`) is included,
> but it needs a MinGW assembler (`as`) for the Windows-API crates; MSVC is the
> supported path on Windows.

### Linux / macOS (typically where the relay runs)
```bash
cargo build --release -p relay   # the server
cargo build --release            # everything
```

## Run it locally (three terminals)

```bash
# 1) relay
cargo run -p relay                      # listens on 0.0.0.0:4455

# 2) host (streamer) — starts your shell, now being streamed
cargo run -p host -- ws://127.0.0.1:4455 --public
#   prints a join code and share instructions

# 3) watch (spectator)
cargo run -p watch -- ws://127.0.0.1:4455
#   browse the list, ↑/↓ + Enter to watch; or join a private code:
cargo run -p watch -- ws://127.0.0.1:4455 <code>
```

## CLI

**host**
```
share-terminal-host [RELAY_URL] [--name NAME] [--shell SHELL] [--public] [--auth-key KEY]
  RELAY_URL   ws:// or wss:// base (default ws://127.0.0.1:4455)
  --public    list in the global directory (default: private, code only)
```
Hotkeys: `Ctrl-O p` privacy toggle · `Ctrl-O q` quit · `Ctrl-O Ctrl-O` literal Ctrl-O.

**watch**
```
share-terminal-watch [RELAY_URL] [CODE_OR_ID]
```
Keys: `↑/↓` move · `Enter` watch · `r` refresh · `q`/`Esc` back/quit · `Ctrl-C` quit.

## Deploy the relay on a VPS (internet, with TLS)

The relay speaks plain HTTP/WS and expects TLS to be terminated by a reverse proxy.

1. Build on the server: `cargo build --release -p relay`.
2. Put [Caddy](https://caddyserver.com) in front for automatic TLS — see
   [`deploy/Caddyfile`](deploy/Caddyfile). Caddy proxies WebSocket upgrades transparently.
3. Run the relay under systemd — see
   [`deploy/share-terminal-relay.service`](deploy/share-terminal-relay.service).
   Set `SHARE_TERMINAL_AUTH_KEY` to require a host key.

Then everyone uses `wss://relay.example.com`:
```
share-terminal-host  wss://relay.example.com --public --auth-key change-me
share-terminal-watch wss://relay.example.com
```

`GET /api/streams` returns the public list as JSON (handy for monitoring / a future web UI).

## Status / roadmap

- [x] Read-only CLI streaming over wss, public list + private codes, late-join snapshots,
      privacy toggle, live viewer counts, resize handling.
- [x] Robustness: watcher auto-reconnect with backoff (and auto-rejoin), frame-size limits,
      stream-count cap, constant-time auth-key check, private streams joinable only by code.
- [ ] Known follow-ups: per-IP connection limiting, coalesced viewer-count updates to the
      host, watcher scrollback, in-TUI private-code entry, optional accounts, a web viewer.
