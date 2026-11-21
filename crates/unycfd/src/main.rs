mod cli;
mod config;
mod notify;
mod proxy;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use cli::CliOptions;
use config::{Config, IngressRule};
use notify::{notify_ready, notify_stopping};
use proxy::IngressOrigin;

use libcfd::edge::{EdgeConnector, EdgeOptions, RemoteConfiguration, Transport};
use libcfd::origin::Origin;
use libcfd::tunnel::{NamedTunnel, Tunnel};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Tune memory allocator behavior on Linux to keep memory footprint under 3-4 MB
    #[cfg(target_os = "linux")]
    unsafe {
        // Optimize glibc malloc arenas
        std::env::set_var("MALLOC_ARENA_MAX", "1");
    }

    let cli_opts = CliOptions::parse()?;

    if cli_opts.show_help {
        CliOptions::print_help();
        return Ok(());
    }

    if cli_opts.show_version {
        println!("unycfd {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    // Initialize tracing
    let default_level = cli_opts.log_level.as_deref().unwrap_or("info");
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();

    tracing::info!("starting unycfd (Cloudflare Tunnel client in pure Rust)");

    // Initialize minimal single-threaded Tokio runtime for lowest possible memory footprint
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async_main(cli_opts))
}

async fn async_main(cli: CliOptions) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Resolve configuration
    let mut config = if let Some(ref config_path) = cli.config_path {
        tracing::info!("loading config from {}", config_path.display());
        Config::from_file(config_path)?
    } else if std::path::Path::new("/etc/cloudflared/config.yml").is_file() {
        tracing::info!("loading default config from /etc/cloudflared/config.yml");
        Config::from_file("/etc/cloudflared/config.yml")?
    } else {
        Config::default()
    };

    // CLI overrides
    if let Some(token) = cli.token {
        config.token = Some(token);
    }
    if let Some(protocol) = cli.protocol {
        config.protocol = Some(protocol);
    }
    if let Some(region) = cli.region {
        config.region = Some(region);
    }

    // Quick tunnel mode (--url http://...)
    let (tunnel, default_ingress) = if let Some(ref url) = cli.url {
        tracing::info!("requesting quick tunnel for URL: {}", url);
        let quick = libcfd::create_quick_tunnel(&Default::default()).await?;
        println!("+--------------------------------------------------------------------------------------------+");
        println!("| Your quick tunnel has been created! Visit it at:                                           |");
        println!("|   https://{:<80} |", quick.hostname);
        println!("+--------------------------------------------------------------------------------------------+");
        (
            Tunnel::quick(quick),
            vec![IngressRule {
                hostname: None,
                path: None,
                service: url.clone(),
            }],
        )
    } else if let Some(ref token) = config.token {
        tracing::info!("loading named tunnel from token");
        let named = NamedTunnel::from_token(token)?;
        tracing::info!(
            "loaded named tunnel {} (account {})",
            named.tunnel_identifier,
            named.account_tag
        );
        (Tunnel::named(named), Vec::new())
    } else if let Some(ref cred_path) = config.credentials_file {
        tracing::info!("loading named tunnel credentials from {}", cred_path.display());
        let named = NamedTunnel::from_credentials_file(cred_path)?;
        tracing::info!(
            "loaded named tunnel {} (account {})",
            named.tunnel_identifier,
            named.account_tag
        );
        (Tunnel::named(named), Vec::new())
    } else if let Some(ref tid) = config.tunnel {
        // Look for matching credentials file in standard paths
        let standard_path = format!("/etc/cloudflared/{tid}.json");
        let alt_path = format!("/var/qyc/cloudflare/tunnels/{tid}.json");
        let cred_path = if std::path::Path::new(&standard_path).is_file() {
            Some(standard_path)
        } else if std::path::Path::new(&alt_path).is_file() {
            Some(alt_path)
        } else {
            None
        };

        if let Some(path) = cred_path {
            tracing::info!("found credentials for tunnel {} at {}", tid, path);
            let named = NamedTunnel::from_credentials_file(path)?;
            (Tunnel::named(named), Vec::new())
        } else {
            return Err(format!(
                "Tunnel ID {} specified, but no credentials-file found in config or standard paths",
                tid
            )
            .into());
        }
    } else {
        return Err("No tunnel specified. Provide --config, --token, or --url.".into());
    };

    let ingress_rules = if config.ingress.is_empty() {
        default_ingress
    } else {
        config.ingress.clone()
    };

    let ingress_json = config.to_cloudflared_json();

    // Determine transport protocol
    let transport = match config.protocol.as_deref().unwrap_or("auto") {
        "quic" => Transport::Quic,
        "h2" => Transport::H2,
        _ => Transport::Auto,
    };

    tracing::info!("using transport: {:?}", transport);

    // Setup edge options and callbacks
    let on_remote_cfg = Arc::new(|cfg: RemoteConfiguration| {
        tracing::info!(
            "edge pushed remote configuration (version {}): hostnames {:?}",
            cfg.version,
            cfg.hostnames
        );
    });

    let on_connected = Arc::new(|| {
        tracing::info!("tunnel connection registered with Cloudflare edge!");
        notify_ready();
    });

    let options = EdgeOptions {
        transport,
        region: config.region,
        configuration_json: ingress_json.into_bytes(),
        on_remote_configuration: Some(on_remote_cfg),
        on_connected: Some(on_connected),
        ..EdgeOptions::default()
    };

    // Setup ingress origin
    let ingress_origin = IngressOrigin::new(ingress_rules);
    let origin = Origin::http(ingress_origin.clone()).with_websocket(ingress_origin);

    let connector = EdgeConnector::new(options);

    // Setup graceful shutdown signal
    let shutdown = async {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("SIGINT received, beginning shutdown");
            }
            _ = async {
                if let Some(ref mut s) = sigterm {
                    s.recv().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                tracing::info!("SIGTERM received, beginning shutdown");
            }
        }
        notify_stopping();
    };

    connector.run(tunnel, origin, shutdown).await?;
    tracing::info!("unycfd stopped cleanly");
    Ok(())
}
