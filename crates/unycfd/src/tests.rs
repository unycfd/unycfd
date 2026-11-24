use crate::config::{ingress_to_cloudflared_json, Config, IngressRule};
use crate::proxy::{IngressOrigin, PrefixedStream};
use futures_util::io::{AsyncReadExt, AsyncWriteExt, Cursor};

#[test]
fn test_yaml_config_parsing_format() {
    let yaml_str = r#"
tunnel: 4c3e387c-658b-4b2a-a9a7-96a1a1f332ab
credentials-file: /etc/cloudflared/my-account.json
edge-ip-version: "auto"
protocol: "quic"

ingress:
  - hostname: example.com
    service: unix:/run/openresty/nginx.sock
  - hostname: www.example.com
    service: unix:/run/openresty/nginx.sock
  - hostname: "*.sub.example.com"
    service: http://127.0.0.1:8080
  - service: http_status:404
"#;

    let config: Config = serde_yaml::from_str(yaml_str).expect("parse yaml");
    assert_eq!(
        config.tunnel.as_deref(),
        Some("4c3e387c-658b-4b2a-a9a7-96a1a1f332ab")
    );
    assert_eq!(
        config.credentials_file.as_deref().unwrap().to_str().unwrap(),
        "/etc/cloudflared/my-account.json"
    );
    assert_eq!(config.edge_ip_version.as_deref(), Some("auto"));
    assert_eq!(config.protocol.as_deref(), Some("quic"));
    assert_eq!(config.ingress.len(), 4);

    assert_eq!(config.ingress[0].hostname.as_deref(), Some("example.com"));
    assert_eq!(config.ingress[0].service, "unix:/run/openresty/nginx.sock");

    assert_eq!(config.ingress[2].hostname.as_deref(), Some("*.sub.example.com"));
    assert_eq!(config.ingress[2].service, "http://127.0.0.1:8080");

    assert_eq!(config.ingress[3].hostname, None);
    assert_eq!(config.ingress[3].service, "http_status:404");

    let json_edge = config.to_cloudflared_json();
    assert!(json_edge.contains("unix:/run/openresty/nginx.sock"));
    assert!(json_edge.contains("http_status:404"));
    // Ensure null fields are not serialized
    assert!(!json_edge.contains("\"hostname\":null"));
    assert!(!json_edge.contains("\"path\":null"));
}

#[test]
fn test_ingress_origin_rule_matching() {
    let rules = vec![
        IngressRule {
            hostname: Some("example.com".into()),
            path: None,
            service: "unix:/run/openresty/nginx.sock".into(),
        },
        IngressRule {
            hostname: Some("*.api.example.com".into()),
            path: None,
            service: "http://127.0.0.1:3000".into(),
        },
        IngressRule {
            hostname: Some("example.org".into()),
            path: Some("/admin".into()),
            service: "http://127.0.0.1:9000".into(),
        },
        IngressRule {
            hostname: None,
            path: None,
            service: "http_status:404".into(),
        },
    ];

    let origin = IngressOrigin::new(rules);

    // Exact host match
    let rule = origin.match_rule("example.com", "/index.html").unwrap();
    assert_eq!(rule.service, "unix:/run/openresty/nginx.sock");

    // Case insensitivity
    let rule = origin.match_rule("EXAMPLE.COM", "/").unwrap();
    assert_eq!(rule.service, "unix:/run/openresty/nginx.sock");

    // Wildcard match (subdomain of wildcard)
    let rule = origin.match_rule("v1.api.example.com", "/users").unwrap();
    assert_eq!(rule.service, "http://127.0.0.1:3000");

    // Wildcard does NOT match apex domain
    let rule = origin.match_rule("api.example.com", "/users").unwrap();
    assert_eq!(rule.service, "http_status:404");

    // Path prefix match
    let rule = origin.match_rule("example.org", "/admin/settings").unwrap();
    assert_eq!(rule.service, "http://127.0.0.1:9000");

    // Path mismatch falls through to catch-all 404
    let rule = origin.match_rule("example.org", "/public").unwrap();
    assert_eq!(rule.service, "http_status:404");

    // Unknown host falls through to catch-all 404
    let rule = origin.match_rule("unknown.com", "/").unwrap();
    assert_eq!(rule.service, "http_status:404");
}

#[test]
fn test_ingress_json_serialization_edge_format() {
    let rules = vec![
        IngressRule {
            hostname: Some("sub.domain.com".into()),
            path: None,
            service: "http://localhost:8080".into(),
        },
        IngressRule {
            hostname: None,
            path: None,
            service: "http_status:404".into(),
        },
    ];

    let json = ingress_to_cloudflared_json(&rules);
    assert!(!json.contains("null"));
    assert!(json.contains("sub.domain.com"));
    assert!(json.contains("http_status:404"));
    assert!(json.contains("warp-routing"));
}

#[tokio::test]
async fn test_prefixed_stream_reads_prefix_and_writes_through() {
    let prefix = b"PRE_DATA:".to_vec();
    let inner = Cursor::new(b"STREAM_REST".to_vec());
    let mut prefixed = PrefixedStream::new(prefix, inner);

    let mut read_all = Vec::new();
    prefixed.read_to_end(&mut read_all).await.unwrap();
    assert_eq!(read_all, b"PRE_DATA:STREAM_REST");

    // Test write pass-through
    let mut buf = Vec::new();
    let inner_sink = Cursor::new(&mut buf);
    let mut prefixed_sink = PrefixedStream::new(Vec::new(), inner_sink);
    prefixed_sink.write_all(b"WRITE_TEST").await.unwrap();
    assert_eq!(buf, b"WRITE_TEST");
}
