use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub tunnel: Option<String>,

    #[serde(rename = "credentials-file", default)]
    pub credentials_file: Option<PathBuf>,

    #[serde(default)]
    pub token: Option<String>,

    #[serde(rename = "edge-ip-version", default)]
    pub edge_ip_version: Option<String>,

    #[serde(default)]
    pub protocol: Option<String>,

    #[serde(default)]
    pub region: Option<String>,

    #[serde(default)]
    pub ingress: Vec<IngressRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngressRule {
    #[serde(default)]
    pub hostname: Option<String>,

    #[serde(default)]
    pub path: Option<String>,

    pub service: String,
}

impl Config {
    /// Loads configuration from a YAML file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// Converts ingress rules into the JSON format expected by Cloudflare edge for updateConfiguration.
    pub fn to_cloudflared_json(&self) -> String {
        #[derive(Serialize)]
        struct EdgeConfig<'a> {
            ingress: &'a [IngressRule],
            #[serde(rename = "warp-routing")]
            warp_routing: serde_json::Value,
        }

        let edge_config = EdgeConfig {
            ingress: &self.ingress,
            warp_routing: serde_json::json!({}),
        };

        serde_json::to_string(&edge_config).unwrap_or_else(|_| {
            r#"{"ingress":[{"hostname":"","service":"http_status:404"}],"warp-routing":{}}"#.to_string()
        })
    }
}
