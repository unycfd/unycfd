use std::path::PathBuf;

#[derive(Debug, Clone, Default)]
pub struct CliOptions {
    pub config_path: Option<PathBuf>,
    pub token: Option<String>,
    pub url: Option<String>,
    pub protocol: Option<String>,
    pub edge_ip_version: Option<String>,
    pub region: Option<String>,
    pub log_level: Option<String>,
    pub tunnel_id: Option<String>,
    pub show_help: bool,
    pub show_version: bool,
}

impl CliOptions {
    pub fn parse() -> Result<Self, lexopt::Error> {
        let mut options = CliOptions::default();
        let mut parser = lexopt::Parser::from_env();

        while let Some(arg) = parser.next()? {
            use lexopt::prelude::*;
            match arg {
                Short('c') | Long("config") => {
                    options.config_path = Some(parser.value()?.parse()?);
                }
                Short('t') | Long("token") => {
                    options.token = Some(parser.value()?.parse()?);
                }
                Short('u') | Long("url") => {
                    options.url = Some(parser.value()?.parse()?);
                }
                Long("protocol") => {
                    options.protocol = Some(parser.value()?.parse()?);
                }
                Long("edge-ip-version") => {
                    options.edge_ip_version = Some(parser.value()?.parse()?);
                }
                Long("region") => {
                    options.region = Some(parser.value()?.parse()?);
                }
                Long("loglevel") | Long("log-level") => {
                    options.log_level = Some(parser.value()?.parse()?);
                }
                Short('h') | Long("help") => {
                    options.show_help = true;
                }
                Short('V') | Long("version") => {
                    options.show_version = true;
                }
                Value(val) => {
                    let s = val.to_string_lossy();
                    // Ignore common cloudflared subcommands like "tunnel" and "run"
                    if s == "tunnel" || s == "run" {
                        continue;
                    }
                    // Could be tunnel id or name positional argument
                    if options.tunnel_id.is_none() {
                        options.tunnel_id = Some(s.into_owned());
                    }
                }
                _ => return Err(arg.unexpected()),
            }
        }

        Ok(options)
    }

    pub fn print_help() {
        println!(
            r#"unycfd {} - Tiny, high-performance Cloudflare Tunnel daemon

USAGE:
    unycfd [tunnel] [run] [OPTIONS] [TUNNEL_ID]

OPTIONS:
    -c, --config <FILE>           Path to configuration YAML file
    -t, --token <TOKEN>           Cloudflare Tunnel connector token
    -u, --url <URL>               Run a quick tunnel pointing to URL (e.g. http://localhost:8080)
        --protocol <quic|h2|auto> Transport protocol (default: auto, prefers quic)
        --edge-ip-version <auto|4|6>
                                  Edge IP address version preference
        --region <REGION>         Edge region override
        --loglevel <LEVEL>        Log level: error, warn, info, debug, trace (default: info)
    -h, --help                    Print help information
    -V, --version                 Print version information
"#,
            env!("CARGO_PKG_VERSION")
        );
    }
}
