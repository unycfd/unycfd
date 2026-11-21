use std::env;
use std::os::unix::net::UnixDatagram;

/// Sends a state notification string to systemd via the `$NOTIFY_SOCKET` socket if configured.
pub fn notify(state: &str) {
    let Ok(socket_path) = env::var("NOTIFY_SOCKET") else {
        return;
    };
    if socket_path.is_empty() {
        return;
    }

    // Abstract socket address support on Linux (starts with @)
    let path_bytes = socket_path.as_bytes();
    let addr = if path_bytes[0] == b'@' {
        let mut abstract_addr = path_bytes.to_vec();
        abstract_addr[0] = 0;
        abstract_addr
    } else {
        path_bytes.to_vec()
    };

    if let Ok(sock) = UnixDatagram::unbound() {
        use std::os::unix::ffi::OsStrExt;
        let os_str = std::ffi::OsStr::from_bytes(&addr);
        let _ = sock.send_to(state.as_bytes(), os_str);
    }
}

/// Informs systemd that the service has successfully started and is ready.
pub fn notify_ready() {
    notify("READY=1\nSTATUS=Tunnel registered and operational\n");
}

/// Informs systemd that the service is stopping.
pub fn notify_stopping() {
    notify("STOPPING=1\nSTATUS=Tunnel shutting down\n");
}
