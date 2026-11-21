use std::path::PathBuf;
use std::sync::Arc;
use futures_util::io::{AsyncReadExt as FuturesAsyncReadExt, Cursor};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use libcfd::origin::http::body::{Body, Request, Response};
use libcfd::origin::stream::{Stream, StreamOrigin, WebSocketConnection};
use libcfd::origin::{HttpOrigin, HttpResponder, WebSocketResponder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::config::IngressRule;

/// Ingress dispatcher that routes traffic according to configured ingress rules.
#[derive(Clone)]
pub struct IngressOrigin {
    rules: Arc<Vec<IngressRule>>,
}

impl IngressOrigin {
    pub fn new(rules: Vec<IngressRule>) -> Self {
        Self {
            rules: Arc::new(rules),
        }
    }

    /// Finds the ingress rule matching the requested host and path.
    pub(crate) fn match_rule(&self, host: &str, path: &str) -> Option<&IngressRule> {
        let host = host.to_ascii_lowercase();
        for rule in self.rules.iter() {
            if let Some(ref rule_host) = rule.hostname {
                let rule_host_clean = rule_host.to_ascii_lowercase();
                if rule_host_clean.starts_with("*.") {
                    let suffix = &rule_host_clean[1..]; // e.g. ".example.com"
                    if !host.ends_with(suffix) && host != &rule_host_clean[2..] {
                        continue;
                    }
                } else if host != rule_host_clean {
                    continue;
                }
            }

            if let Some(ref rule_path) = rule.path {
                if !path.starts_with(rule_path) {
                    continue;
                }
            }

            return Some(rule);
        }
        None
    }
}

impl HttpOrigin for IngressOrigin {
    fn handle(&self, request: Request, respond: HttpResponder) {
        let origin = self.clone();
        tokio::spawn(async move {
            origin.handle_async(request, respond).await;
        });
    }
}

impl IngressOrigin {
    async fn handle_async(&self, request: Request, respond: HttpResponder) {
        let host = request
            .headers
            .get("host")
            .and_then(|h| h.to_str().ok())
            .or_else(|| request.uri.host())
            .unwrap_or("")
            .split(':')
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();

        let path = request.uri.path();

        let rule = self.match_rule(&host, path);
        let service = match rule {
            Some(r) => &r.service,
            None => "http_status:404",
        };

        if let Some(status_str) = service.strip_prefix("http_status:") {
            let status = status_str
                .parse::<u16>()
                .ok()
                .and_then(|c| StatusCode::from_u16(c).ok())
                .unwrap_or(StatusCode::NOT_FOUND);

            let msg = format!("{} {}\n", status.as_u16(), status.canonical_reason().unwrap_or(""));
            respond.send(Response::new(status, HeaderMap::new(), Body::from_bytes(msg.into_bytes())));
            return;
        }

        if let Some(unix_path) = service.strip_prefix("unix:") {
            let socket_path = PathBuf::from(unix_path);
            match tokio::net::UnixStream::connect(&socket_path).await {
                Ok(stream) => {
                    forward_http(stream, request, respond).await;
                }
                Err(err) => {
                    tracing::error!("failed to connect to unix socket {}: {}", socket_path.display(), err);
                    respond.send(Response::new(
                        StatusCode::BAD_GATEWAY,
                        HeaderMap::new(),
                        Body::from_bytes(format!("502 Bad Gateway: connect unix socket: {err}\n").into_bytes()),
                    ));
                }
            }
            return;
        }

        if let Some(target) = service.strip_prefix("http://").or_else(|| service.strip_prefix("tcp://")) {
            let addr = if target.contains(':') {
                target.to_string()
            } else {
                format!("{target}:80")
            };

            match tokio::net::TcpStream::connect(&addr).await {
                Ok(stream) => {
                    forward_http(stream, request, respond).await;
                }
                Err(err) => {
                    tracing::error!("failed to connect to upstream {}: {}", addr, err);
                    respond.send(Response::new(
                        StatusCode::BAD_GATEWAY,
                        HeaderMap::new(),
                        Body::from_bytes(format!("502 Bad Gateway: connect upstream: {err}\n").into_bytes()),
                    ));
                }
            }
            return;
        }

        // Fallback for unsupported service schemas
        tracing::warn!("unsupported service rule: {}", service);
        respond.send(Response::new(
            StatusCode::NOT_FOUND,
            HeaderMap::new(),
            Body::from_bytes(b"404 Not Found: unconfigured service\n".to_vec()),
        ));
    }
}

/// Forwards an HTTP/1.1 request to a duplex stream (Unix socket or TCP stream) and streams the response back.
async fn forward_http<S>(mut stream: S, request: Request, respond: HttpResponder)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // Build HTTP/1.1 request wire bytes
    let method = request.method.as_str();
    let uri_path_and_query = request
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");

    let mut req_bytes = Vec::with_capacity(512);
    req_bytes.extend_from_slice(method.as_bytes());
    req_bytes.push(b' ');
    req_bytes.extend_from_slice(uri_path_and_query.as_bytes());
    req_bytes.extend_from_slice(b" HTTP/1.1\r\n");

    let mut has_host = false;
    for (name, val) in request.headers.iter() {
        if name.as_str().eq_ignore_ascii_case("host") {
            has_host = true;
        }
        // Skip Connection: keep-alive or close adjustments
        req_bytes.extend_from_slice(name.as_str().as_bytes());
        req_bytes.extend_from_slice(b": ");
        req_bytes.extend_from_slice(val.as_bytes());
        req_bytes.extend_from_slice(b"\r\n");
    }

    if !has_host {
        if let Some(host) = request.uri.host() {
            req_bytes.extend_from_slice(b"Host: ");
            req_bytes.extend_from_slice(host.as_bytes());
            req_bytes.extend_from_slice(b"\r\n");
        }
    }

    // Always inform upstream of original protocol
    if !request.headers.contains_key("x-forwarded-proto") {
        req_bytes.extend_from_slice(b"X-Forwarded-Proto: https\r\n");
    }

    req_bytes.extend_from_slice(b"\r\n");

    if let Err(e) = stream.write_all(&req_bytes).await {
        tracing::error!("error writing request header to upstream: {}", e);
        respond.fail(format!("upstream write error: {e}"));
        return;
    }

    // Stream request body if present
    let mut req_body_compat = tokio_util::compat::FuturesAsyncReadCompatExt::compat(request.body);
    let _ = tokio::io::copy(&mut req_body_compat, &mut stream).await;

    // Read upstream response headers
    let mut header_buf = Vec::with_capacity(4096);
    let mut temp = [0u8; 2048];
    let header_len;

    loop {
        let n = match stream.read(&mut temp).await {
            Ok(0) => {
                respond.fail("upstream connection closed before sending response headers");
                return;
            }
            Ok(n) => n,
            Err(e) => {
                respond.fail(format!("upstream read error: {e}"));
                return;
            }
        };

        header_buf.extend_from_slice(&temp[..n]);

        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut parsed_resp = httparse::Response::new(&mut headers);
        match parsed_resp.parse(&header_buf) {
            Ok(httparse::Status::Complete(len)) => {
                header_len = len;
                break;
            }
            Ok(httparse::Status::Partial) => {
                if header_buf.len() > 65536 {
                    respond.fail("upstream response headers exceed 64KB limit");
                    return;
                }
            }
            Err(e) => {
                respond.fail(format!("malformed upstream response: {e}"));
                return;
            }
        }
    }

    // Parse the completed response
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut parsed_resp = httparse::Response::new(&mut headers);
    let _ = parsed_resp.parse(&header_buf);

    let status_code = parsed_resp
        .code
        .and_then(|c| StatusCode::from_u16(c).ok())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    let mut response_headers = HeaderMap::new();
    for h in parsed_resp.headers.iter() {
        if let Ok(name) = HeaderName::from_bytes(h.name.as_bytes()) {
            if let Ok(val) = HeaderValue::from_bytes(h.value) {
                response_headers.append(name, val);
            }
        }
    }

    // Any remaining bytes in header_buf belong to the response body
    let leftover = header_buf[header_len..].to_vec();

    // Stream the body using tokio-util compat
    let stream_reader = stream.compat();
    let body_reader = Cursor::new(leftover).chain(stream_reader);
    let response_body = Body::from_reader(body_reader);

    respond.send(Response::new(status_code, response_headers, response_body));
}

impl StreamOrigin<WebSocketResponder> for IngressOrigin {
    fn connect(&self, request: Request, respond: WebSocketResponder) {
        let origin = self.clone();
        tokio::spawn(async move {
            origin.handle_ws_async(request, respond).await;
        });
    }
}

impl IngressOrigin {
    async fn handle_ws_async(&self, request: Request, respond: WebSocketResponder) {
        let host = request
            .headers
            .get("host")
            .and_then(|h| h.to_str().ok())
            .or_else(|| request.uri.host())
            .unwrap_or("")
            .split(':')
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();

        let path = request.uri.path();
        let rule = self.match_rule(&host, path);
        let service = match rule {
            Some(r) => &r.service,
            None => {
                respond.fail("no matching ingress rule for websocket");
                return;
            }
        };

        if let Some(unix_path) = service.strip_prefix("unix:") {
            match tokio::net::UnixStream::connect(unix_path).await {
                Ok(stream) => {
                    upgrade_ws_duplex(stream, request, respond).await;
                }
                Err(e) => {
                    respond.fail(format!("connect unix socket failed: {e}"));
                }
            }
            return;
        }

        if let Some(target) = service.strip_prefix("http://").or_else(|| service.strip_prefix("tcp://")) {
            let addr = if target.contains(':') {
                target.to_string()
            } else {
                format!("{target}:80")
            };

            match tokio::net::TcpStream::connect(&addr).await {
                Ok(stream) => {
                    upgrade_ws_duplex(stream, request, respond).await;
                }
                Err(e) => {
                    respond.fail(format!("connect tcp socket failed: {e}"));
                }
            }
            return;
        }

        respond.fail("unsupported service for websocket upgrade");
    }
}

async fn upgrade_ws_duplex<S>(mut stream: S, request: Request, respond: WebSocketResponder)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // Write the websocket upgrade request to origin
    let uri_pq = request.uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    let mut req_bytes = format!("GET {} HTTP/1.1\r\n", uri_pq).into_bytes();

    for (name, val) in request.headers.iter() {
        req_bytes.extend_from_slice(name.as_str().as_bytes());
        req_bytes.extend_from_slice(b": ");
        req_bytes.extend_from_slice(val.as_bytes());
        req_bytes.extend_from_slice(b"\r\n");
    }
    req_bytes.extend_from_slice(b"\r\n");

    if let Err(e) = stream.write_all(&req_bytes).await {
        respond.fail(format!("write ws handshake failed: {e}"));
        return;
    }

    // Read 101 response from upstream
    let mut header_buf = Vec::with_capacity(2048);
    let mut temp = [0u8; 1024];

    loop {
        let n = match stream.read(&mut temp).await {
            Ok(0) => {
                respond.fail("upstream closed during ws handshake");
                return;
            }
            Ok(n) => n,
            Err(e) => {
                respond.fail(format!("read ws handshake error: {e}"));
                return;
            }
        };

        header_buf.extend_from_slice(&temp[..n]);
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut parsed_resp = httparse::Response::new(&mut headers);
        if let Ok(httparse::Status::Complete(_len)) = parsed_resp.parse(&header_buf) {
            let mut response_headers = HeaderMap::new();
            for h in parsed_resp.headers.iter() {
                if let Ok(name) = HeaderName::from_bytes(h.name.as_bytes()) {
                    if let Ok(val) = HeaderValue::from_bytes(h.value) {
                        response_headers.append(name, val);
                    }
                }
            }

            let response = Response::new(
                StatusCode::SWITCHING_PROTOCOLS,
                response_headers,
                Body::empty(),
            );

            let origin_stream = Stream::from_io(stream.compat());
            respond.upgrade(WebSocketConnection {
                response,
                origin: origin_stream,
            });
            return;
        }
    }
}
