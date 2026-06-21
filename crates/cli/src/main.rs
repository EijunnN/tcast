//! `tcast` — the unified client: share or watch terminals from one command.
//!
//! Subcommands dispatch into the `host` and `watch` libraries:
//!   tcast stream [--public …]   stream your terminal (host)
//!   tcast watch  [TARGET]       browse streams, or join a code/id (watch)
//!   tcast list   [--json]       print the public directory and exit
//!   tcast config …              save the relay URL / auth key / name
//!   tcast                       (bare) opens the watch browser
//!
//! The relay server itself is a separate, operator-only binary (`tcast-relay`).

mod config;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "tcast",
    version,
    about = "Share or watch terminals live — a Twitch/Kick for terminals"
)]
struct Cli {
    /// Relay base URL (ws:// or wss://). Overrides the config file.
    #[arg(long, global = true, env = "TCAST_RELAY")]
    relay: Option<String>,
    /// Use a specific config file (default: <OS config dir>/tcast/config.toml).
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Stream your terminal live (read-only for viewers).
    Stream(StreamArgs),
    /// Watch a stream: opens the browser TUI, or joins a code/id directly.
    Watch(WatchArgs),
    /// Open chat for your own running stream (read viewer messages + reply).
    Chat,
    /// Print the live public stream directory and exit (non-interactive).
    List {
        /// Emit JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Update tcast in place to the latest GitHub release.
    Upgrade,
    /// Manage saved settings (relay URL, auth key, name).
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },
}

#[derive(Args)]
struct StreamArgs {
    /// Display name shown to viewers (default: your username, or saved config).
    #[arg(long)]
    name: Option<String>,
    /// Shell to launch (default: powershell.exe on Windows, $SHELL on Unix).
    #[arg(long)]
    shell: Option<String>,
    /// List this stream in the public directory (default: private, code only).
    #[arg(long)]
    public: bool,
    /// Auth key, if the relay requires one (default: saved config).
    #[arg(long)]
    auth_key: Option<String>,
    /// Hotkey prefix key — a letter mapped to its Ctrl- code (default: Ctrl-]).
    #[arg(long)]
    prefix: Option<char>,
    /// Accept viewer chat on this stream (view it with `tcast chat`).
    #[arg(long)]
    chat: bool,
    /// Capture your mic for push-to-talk voice (prefix + `t` toggles it).
    #[arg(long)]
    voice: bool,
}

#[derive(Args)]
struct WatchArgs {
    /// Join this code / stream id directly, skipping the browser.
    target: Option<String>,
    /// Display name for your chat messages (default: saved config name).
    #[arg(long)]
    name: Option<String>,
}

#[derive(Subcommand)]
enum ConfigCmd {
    /// Save the default relay URL, e.g. `tcast config set-relay wss://relay.example.com`.
    SetRelay { url: String },
    /// Save the host auth key (a shared secret).
    SetAuthKey { key: String },
    /// Save the default streaming display name.
    SetName { name: String },
    /// Print the resolved configuration and where each value comes from.
    Show {
        /// Print only the config file path.
        #[arg(long)]
        path: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg_path = config::config_path(cli.config.clone());
    let cfg = cfg_path
        .as_ref()
        .map(|p| config::load(p))
        .unwrap_or_default();

    match cli.cmd {
        // Bare `tcast` opens the watch browser — the most common first action.
        None => {
            let relay = config::resolve_relay(cli.relay, &cfg);
            watch::run(watch::WatchConfig {
                relay,
                target: None,
                name: cfg.name.clone(),
                allow_self: false,
                chat_open: false,
                chat_only: false,
            })
            .await
        }
        Some(Command::Watch(a)) => {
            let relay = config::resolve_relay(cli.relay, &cfg);
            watch::run(watch::WatchConfig {
                relay,
                target: a.target,
                name: a.name.or_else(|| cfg.name.clone()),
                allow_self: false,
                chat_open: false,
                chat_only: false,
            })
            .await
        }
        Some(Command::Chat) => {
            let relay = config::resolve_relay(cli.relay, &cfg);
            let target = protocol::owned::list()
                .into_iter()
                .next()
                .map(|(_, code)| code)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no local stream is running here — start one with `tcast stream --chat`"
                    )
                })?;
            watch::run(watch::WatchConfig {
                relay,
                target: Some(target),
                name: cfg.name.clone(),
                allow_self: true,
                chat_open: true,
                chat_only: true,
            })
            .await
        }
        Some(Command::List { json }) => {
            let relay = config::resolve_relay(cli.relay, &cfg);
            watch::list(relay, json).await
        }
        Some(Command::Upgrade) => tokio::task::spawn_blocking(run_upgrade).await?,
        Some(Command::Stream(a)) => {
            let relay = config::resolve_relay(cli.relay, &cfg);
            let prefix = match a.prefix {
                Some(c) if c.is_ascii_alphabetic() => (c.to_ascii_uppercase() as u8) & 0x1f,
                Some(c) => anyhow::bail!("--prefix must be a letter a-z (got {c:?})"),
                None => host::DEFAULT_PREFIX,
            };
            host::run(host::StreamConfig {
                relay,
                name: a.name.or_else(|| cfg.name.clone()),
                shell: a.shell,
                public: a.public,
                auth_key: a.auth_key.or_else(|| cfg.auth_key.clone()),
                chat: a.chat,
                voice: a.voice,
                prefix,
            })
            .await
        }
        Some(Command::Config { cmd }) => run_config(cmd, cfg_path, cfg, cli.relay),
    }
}

fn run_config(
    cmd: ConfigCmd,
    cfg_path: Option<PathBuf>,
    mut cfg: config::Config,
    relay_flag: Option<String>,
) -> Result<()> {
    let path =
        cfg_path.ok_or_else(|| anyhow::anyhow!("could not determine an OS config directory"))?;
    match cmd {
        ConfigCmd::SetRelay { url } => {
            cfg.relay = Some(url.clone());
            config::save(&path, &cfg)?;
            println!("saved relay = {url}\n  ({})", path.display());
        }
        ConfigCmd::SetAuthKey { key } => {
            cfg.auth_key = Some(key);
            config::save(&path, &cfg)?;
            println!("saved auth key  ({})", path.display());
        }
        ConfigCmd::SetName { name } => {
            cfg.name = Some(name.clone());
            config::save(&path, &cfg)?;
            println!("saved name = {name}  ({})", path.display());
        }
        ConfigCmd::Show { path: path_only } => {
            if path_only {
                println!("{}", path.display());
                return Ok(());
            }
            let relay = config::resolve_relay(relay_flag.clone(), &cfg);
            let src = config::relay_source(relay_flag.as_deref(), &cfg);
            println!("config file : {}", path.display());
            println!("relay       : {relay}  (from {src})");
            println!(
                "name        : {}",
                cfg.name.as_deref().unwrap_or("<your username at stream time>")
            );
            println!(
                "auth key    : {}",
                if cfg.auth_key.is_some() { "set" } else { "not set" }
            );
        }
    }
    Ok(())
}

/// Replace the running binary with the latest GitHub release for this platform.
fn run_upgrade() -> Result<()> {
    let bin = if cfg!(windows) { "tcast.exe" } else { "tcast" };
    let status = self_update::backends::github::Update::configure()
        .repo_owner("EijunnN")
        .repo_name("tcast")
        .bin_name(bin)
        .show_download_progress(true)
        .current_version(env!("CARGO_PKG_VERSION"))
        .build()?
        .update()?;
    if status.updated() {
        println!("tcast updated to {}", status.version());
    } else {
        println!("tcast is already up to date ({})", status.version());
    }
    Ok(())
}
