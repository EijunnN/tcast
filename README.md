# tcast

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

There are two programs:

- **`tcast`** — the client everyone installs. One command, with subcommands to
  stream (`tcast stream`) or watch (`tcast watch`).
- **`tcast-relay`** — the server. The operator runs one on a VPS; end users never
  install it.

## Install (end users)

Prebuilt binaries, no Rust toolchain needed:

```sh
# Linux
curl --proto '=https' --tlsv1.2 -LsSf https://raw.githubusercontent.com/EijunnN/tcast/main/install.sh | sh
```
```powershell
# Windows (PowerShell)
irm https://raw.githubusercontent.com/EijunnN/tcast/main/install.ps1 | iex
```

The installer drops `tcast` on your `PATH`. Then point it at a relay once:

```sh
tcast config set-relay wss://relay.example.com
```

> No prebuilt binary for your platform? Install with Cargo instead:
> `cargo install --git https://github.com/EijunnN/tcast tcast`

**Updating:** once installed, run `tcast upgrade` to replace your binary in place
with the latest release (Linux and Windows; on macOS, build from source).

## Use it

```sh
tcast                       # open the stream browser (read-only viewer)
tcast watch                 # same as above
tcast watch <code>          # join a private stream directly by its code
tcast list                  # print the public directory (add --json for scripts)
tcast stream                # stream your terminal (private, code-only)
tcast stream --public       # …and list it in the public directory
tcast stream --chat         # …and let viewers send chat
tcast stream --voice        # …and talk to viewers (Ctrl-] t = push-to-talk)
tcast chat                  # (while streaming) read viewer chat + reply
```

Per-command relay override: `tcast --relay wss://other.example.com watch`, or set
the `TCAST_RELAY` environment variable. Precedence: `--relay` / `TCAST_RELAY` →
saved config (`tcast config set-relay`) → built-in default → `ws://127.0.0.1:4455`.

**Host hotkeys** (prefix defaults to `Ctrl-]`; change it with `tcast stream --prefix <letter>`): `Ctrl-] p` privacy · `Ctrl-] q` quit · `Ctrl-] Ctrl-]` literal. Or just type `exit` / `Ctrl-D` in the shell to end the stream — handy when an inner app (Claude Code, nano…) uses your prefix key.
**Watch keys:** `↑/↓` move · `Enter` watch · `r` refresh · `q`/`Esc` back/quit · `Ctrl-C` quit.
While watching, press `c` to chat (if the host enabled it): type, `Enter` to send, `Esc` to cancel.
Press `m` to mute/unmute the host's voice.

## Why it's safe to watch (and to stream)

- **Read-only by construction.** No spectator input ever reaches your shell — there
  is no path from any protocol message into the host's PTY, only your own local
  keystrokes. A viewer literally cannot type into your shell; it's a property of the
  types, not a runtime check.
- **Chat is display-only & opt-in.** Viewer chat exists only if you pass `--chat`, is
  sanitized by the relay (control/ANSI bytes stripped), and is never fed to a shell —
  you read it in a separate `tcast chat` window.
- **Voice is one-way & opt-in.** With `--voice` the host can talk (push-to-talk) and
  viewers only listen; there is no viewer→host audio channel.
- **Privacy toggle.** Press `Ctrl-] p` in the host to pause what viewers see
  (e.g. while typing a password); press again to resume.
- **Private by default.** A stream is reachable only by its generated code unless
  you pass `--public` to list it in the global directory.
- **Optional host auth.** Operators can require a shared key (`--auth-key`) so not
  just anyone can stream through your relay.

## Workspace layout

| crate      | what it is                                                              |
|------------|-------------------------------------------------------------------------|
| `protocol` | shared message types + MessagePack codec (the wire contract)            |
| `host`     | library: spawns your shell in a PTY, mirrors output locally and up      |
| `watch`    | library: spectator TUI + non-interactive `list`                         |
| `audio`    | real-time mono PCM capture/playback (cpal) for push-to-talk voice        |
| `cli`      | the `tcast` binary — clap front-end dispatching to `host` / `watch`     |
| `relay`    | the `tcast-relay` binary: stream registry, fan-out, late-join snapshots |

## Build from source

### Windows
Needs the MSVC toolchain **with the Windows SDK** (for `kernel32.lib` etc.):

```powershell
# one-time, if missing:
winget install --id Microsoft.WindowsSDK.10.0.26100 -e

# build (loads the VS dev environment first):
. .\tools\msvcenv.ps1
cargo build --release
```

### Linux / macOS
```bash
cargo build --release            # everything
cargo build --release -p tcast   # just the client
cargo build --release -p relay   # just the server (binary: tcast-relay)
```

On Linux the client links ALSA for voice: `sudo apt install libasound2-dev`. The
relay needs neither ALSA nor OpenSSL, so a VPS build (`-p relay`) stays minimal.

This produces `target/release/tcast` and `target/release/tcast-relay`. TLS uses
`native-tls` (SChannel on Windows, Secure Transport / OpenSSL elsewhere); voice
uses `cpal` (pure Rust) and raw PCM — both avoid the C toolchain that
rustls/aws-lc or libopus would pull in.

## Run it locally (three terminals)

```bash
# 1) relay
cargo run -p relay                              # listens on 0.0.0.0:4455

# 2) host (streamer) — starts your shell, now being streamed
cargo run -p tcast -- stream --relay ws://127.0.0.1:4455 --public
#   prints a join code and share instructions

# 3) watch (spectator)
cargo run -p tcast -- watch --relay ws://127.0.0.1:4455
#   browse the list, ↑/↓ + Enter to watch; or join a private code:
cargo run -p tcast -- watch --relay ws://127.0.0.1:4455 <code>
```

## CLI reference

**tcast**
```
tcast [--relay URL] [--config PATH] [COMMAND]
  (no command)             open the watch browser
  stream [--name NAME] [--shell SHELL] [--public] [--auth-key KEY] [--prefix LETTER] [--chat] [--voice]
  watch  [CODE_OR_ID] [--name NAME]
  chat                     open chat for your own running stream (read + reply)
  list   [--json]
  upgrade                  self-update to the latest GitHub release
  config set-relay <URL> | set-auth-key <KEY> | set-name <NAME> | show [--path]
```

**tcast-relay** (operator only)
```
tcast-relay [--bind ADDR] [--auth-key KEY]
  --bind       default 0.0.0.0:4455
  --auth-key   shared host secret (or env TCAST_AUTH_KEY)
```

## Deploy the relay on a VPS (operator guide)

Only the **relay** runs on the server — end users never touch the VPS. You need
a VPS and a **domain**: the relay requires TLS for `wss://`, and Let's Encrypt
issues certificates for domain names. Before you start, point a DNS **A record**
(e.g. `relay.example.com`) at your VPS's public IP.

### 1. Prerequisites

```bash
# Rust (the project uses edition 2024 → needs Rust ≥ 1.85)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# a linker (cc), needed even for pure-Rust crates
sudo apt update && sudo apt install -y build-essential
```

The relay does **not** use native-tls/OpenSSL, so building *only the relay*
needs no `libssl-dev`. On a VPS with < 1 GB RAM, add swap (release LTO can
exhaust memory) or build the binary on another machine and `scp` it over.

### 2. Build

```bash
git clone https://github.com/EijunnN/tcast && cd tcast
cargo build --release -p relay      # → target/release/tcast-relay
```

### 3. TLS via Caddy

[Caddy](https://caddyserver.com) obtains and renews a Let's Encrypt certificate
automatically and proxies WebSocket upgrades transparently. Edit
[`deploy/Caddyfile`](deploy/Caddyfile) with your domain:

```
relay.example.com {
    reverse_proxy 127.0.0.1:4455
}
```

Open ports **80** (ACME validation) and **443** in your firewall. The relay
itself stays bound to localhost — do not expose 4455.

### 4. Run under systemd

The relay binds to `127.0.0.1:4455` (behind Caddy). Install the unit from
[`deploy/tcast-relay.service`](deploy/tcast-relay.service):

```bash
sudo cp target/release/tcast-relay /usr/local/bin/tcast-relay
sudo cp deploy/tcast-relay.service /etc/systemd/system/
echo 'TCAST_AUTH_KEY=change-me' | sudo tee /etc/tcast-relay.env   # optional
sudo systemctl daemon-reload
sudo systemctl enable --now tcast-relay
```

`TCAST_AUTH_KEY` is **optional** and gates *streaming* only (viewers
never need it). Omit the env file to run an open relay.

### 5. Users connect

```sh
tcast config set-relay wss://relay.example.com   # once
tcast watch                                       # spectate
tcast stream --public --auth-key change-me        # stream (if a key is set)
```

`GET https://relay.example.com/api/streams` returns the public list as JSON
(handy for monitoring / a future web UI).

### Good to know

- **No database.** Streams live in memory; restarting the relay drops active
  sessions (clients auto-reconnect). Nothing to back up.
- **Version lock-step.** Client and relay share `PROTOCOL_VERSION`; after pulling
  repo updates, rebuild and redeploy the relay.

## Releases

Tagging a commit `vX.Y.Z` triggers [`.github/workflows/release.yml`](.github/workflows/release.yml),
which builds `tcast` for Linux (x86_64/aarch64, glibc) and Windows (x86_64), and
uploads the archives + sha256 checksums to a GitHub Release. The `install.sh` /
`install.ps1` one-liners above download from there. (macOS isn't prebuilt — build
it from source with `cargo build --release`.)

To bake a default relay into the released binaries (so a fresh `tcast` works with
zero config), set the repository variable `TCAST_DEFAULT_RELAY` (e.g.
`wss://relay.example.com`).

## Status / roadmap

- [x] Read-only CLI streaming over wss, public list + private codes, late-join snapshots,
      privacy toggle, live viewer counts, resize handling.
- [x] Unified `tcast` client (stream/watch/list/config) + curl/PowerShell installers.
- [x] Opt-in viewer chat (`--chat`, display-only, relay-sanitized, rate-limited) read via
      `tcast chat`; configurable hotkey prefix; guard against watching your own stream.
- [x] One-way push-to-talk voice (`--voice`): raw 48 kHz PCM over the same WS, lock-free
      jitter buffer, viewer mute — pure Rust (cpal), no C toolchain.
- [x] Robustness: watcher auto-reconnect with backoff (and auto-rejoin), frame-size limits,
      stream-count cap, constant-time auth-key check, private streams joinable only by code.
- [ ] Known follow-ups: opus voice codec (Phase 2, ~8× less bandwidth, needs a C build) +
      adaptive jitter buffer / 🎙 indicator (Phase 3); static **musl** Linux build for
      Alpine/old-glibc (needs vendored OpenSSL or an rustls switch); per-IP connection
      limiting; watcher scrollback; in-TUI private-code entry; optional accounts; a web viewer.
```
