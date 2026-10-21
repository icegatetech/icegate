//! The one listener every server binds: plain TCP, or TLS completed off the accept path.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

#[cfg(feature = "grpc")]
use futures::{Stream, StreamExt as _, future::Either};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
#[cfg(feature = "grpc")]
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};

use crate::CancellationToken;
use crate::error::Result;
use crate::listener::{Connection, ListenerRuntime};

/// How long an accepted connection may take to complete its TLS handshake before it is
/// dropped.
///
/// A handshake is a few round trips, so a client still short of one after this long is not
/// handshaking at all: it has stalled, or it does not speak TLS. Every such connection holds
/// a socket and a task until the bound, so the bound, times the rate at which they arrive,
/// caps what they can tie up. A shorter bound would start dropping clients on slow links,
/// whose few round trips this one still covers many times over.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How many connections whose handshake completed may wait for the server to take them.
///
/// The server takes a connection as soon as it can start serving it, so this only absorbs
/// handshakes that complete together. A full queue drops nothing: the handshake task keeps
/// its connection until a slot frees, holding nothing it did not hold already. The capacity
/// therefore decides only how many of them hand over without waiting, which a small queue
/// already covers.
const READY_CONNECTION_CAPACITY: usize = 32;

/// What a listener's clients speak, and so the one protocol its TLS handshake offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationProtocol {
    /// HTTP/1.1, the protocol the axum servers speak.
    Http1,
    /// gRPC, which runs over HTTP/2.
    Grpc,
}

impl ApplicationProtocol {
    /// The protocol's identifier in the IANA ALPN registry.
    const fn alpn_identifier(self) -> &'static [u8] {
        match self {
            Self::Http1 => b"http/1.1",
            Self::Grpc => b"h2",
        }
    }
}

/// `host` without one pair of surrounding brackets, so an address such as `::1` binds
/// whether it is written bare or bracketed (`[::1]`, the form a `host:port` string takes).
fn strip_host_brackets(host: &str) -> &str {
    host.strip_prefix('[').and_then(|inner| inner.strip_suffix(']')).unwrap_or(host)
}

/// Give an accepted connection the socket options its listener's `protocol` serves with.
///
/// A connection whose option cannot be set is still usable, only without the option, so
/// the failure is logged and the connection served anyway.
fn set_socket_options(tcp_stream: &TcpStream, protocol: ApplicationProtocol) {
    // gRPC listeners need TCP_NODELAY so small frames are not held back waiting to
    // coalesce, and the incoming-stream path that tonic serves them from does not set it,
    // so the listener does. The axum listeners keep the socket defaults they always had.
    if protocol == ApplicationProtocol::Grpc
        && let Err(error) = tcp_stream.set_nodelay(true)
    {
        tracing::debug!(%error, "cannot set TCP_NODELAY on an accepted gRPC connection; serving it without");
    }
}

/// A connection ready to be served, with the address of its peer.
type ReadyConnection = (Connection, SocketAddr);

/// Where a [`ServerListener`] gets its next connection from.
#[derive(Debug)]
enum IncomingConnections {
    /// The bound socket itself: the server accepts from it directly.
    Plain(TcpListener),
    /// The accept task, which hands over each connection once its handshake completed.
    Tls(mpsc::Receiver<ReadyConnection>),
}

/// A bound listener that hands each server its connections, plain or TLS, through the one
/// [`Connection`] type: to axum as an [`axum::serve::Listener`], to tonic through
/// `into_incoming` (feature `grpc`).
///
/// Without a certificate in its [`ListenerRuntime`] the server accepts straight from the
/// socket, exactly as from a [`TcpListener`], and the port is released when this listener
/// is dropped.
///
/// With one, a background task accepts connections and runs each TLS handshake in a task of
/// its own, bounded by a handshake timeout. A connection reaches the server only once its
/// handshake completed, so a client that stalls, or does not speak TLS, holds up no one
/// else; a handshake that fails or times out drops its connection and is logged at `debug`.
/// The task stops accepting, and releases the port, once the runtime's token is cancelled
/// or this listener is dropped. A handshake already running is not cut short: it still
/// completes or times out on its own.
///
/// Every connection a gRPC listener accepts, plain or TLS, is served with `TCP_NODELAY`
/// set, before any TLS handshake; an HTTP/1.1 listener leaves the socket options as
/// accepted.
#[derive(Debug)]
pub struct ServerListener {
    local_addr: SocketAddr,
    protocol: ApplicationProtocol,
    incoming: IncomingConnections,
}

impl ServerListener {
    /// Bind `host:port`, serving TLS that offers only `protocol` when `runtime` carries a
    /// certificate, and plain TCP otherwise.
    ///
    /// `host` is a name or an IP literal, and a literal such as `::1` may also be written in
    /// brackets (`[::1]`); it is resolved as [`TcpListener::bind`] resolves a `(host, port)`
    /// pair. Port `0` binds a free port, which [`Self::local_addr`] reports.
    ///
    /// # Errors
    /// * [`CommonError::Io`](crate::Error::Io) when `host` does not resolve or no address
    ///   can be bound.
    /// * [`CommonError::Tls`](crate::Error::Tls) when the TLS configuration cannot be
    ///   built; nothing is bound then.
    pub async fn bind(host: &str, port: u16, protocol: ApplicationProtocol, runtime: &ListenerRuntime) -> Result<Self> {
        // Built before binding, so a TLS setup that fails never holds the port.
        let tls_acceptor = runtime
            .certificate()
            .map(|certificate| certificate.build_server_config(&[protocol.alpn_identifier()]))
            .transpose()?
            .map(TlsAcceptor::from);
        let tcp_listener = TcpListener::bind((strip_host_brackets(host), port)).await?;
        let local_addr = tcp_listener.local_addr()?;

        let incoming = match tls_acceptor {
            None => IncomingConnections::Plain(tcp_listener),
            Some(tls_acceptor) => {
                let (ready_tx, ready_rx) = mpsc::channel(READY_CONNECTION_CAPACITY);
                tokio::spawn(accept_tls_connections(
                    tcp_listener,
                    protocol,
                    tls_acceptor,
                    ready_tx,
                    runtime.cancel_token().clone(),
                ));
                IncomingConnections::Tls(ready_rx)
            }
        };
        Ok(Self {
            local_addr,
            protocol,
            incoming,
        })
    }

    /// The address the listener is bound to.
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The connections of this listener as the stream tonic serves from.
    ///
    /// Plain TCP yields every accept error of the socket as it comes, for the server to
    /// judge. TLS yields only connections whose handshake completed, and the stream ends
    /// once the accept task has stopped, every handshake it started has finished, and every
    /// connection they handed over was taken.
    #[cfg(feature = "grpc")]
    pub fn into_incoming(self) -> impl Stream<Item = io::Result<Connection>> + Send + 'static {
        // `Either` makes the two different stream types one type, as an `impl` return
        // requires.
        let protocol = self.protocol;
        match self.incoming {
            IncomingConnections::Plain(tcp_listener) => {
                Either::Left(TcpListenerStream::new(tcp_listener).map(move |accepted| {
                    accepted.map(|tcp_stream| {
                        set_socket_options(&tcp_stream, protocol);
                        Connection::Plain(tcp_stream)
                    })
                }))
            }
            IncomingConnections::Tls(ready_rx) => {
                Either::Right(ReceiverStream::new(ready_rx).map(|(connection, _peer_addr)| Ok(connection)))
            }
        }
    }
}

impl axum::serve::Listener for ServerListener {
    type Io = Connection;
    type Addr = SocketAddr;

    /// The next connection and its peer's address.
    ///
    /// Once a TLS listener's accept task has stopped and every handshake it started has
    /// finished, no connection can arrive, so the future stays pending; the server stops on
    /// the runtime's token, which is what stopped the task.
    async fn accept(&mut self) -> (Connection, SocketAddr) {
        match &mut self.incoming {
            IncomingConnections::Plain(tcp_listener) => {
                let (tcp_stream, peer_addr) = axum::serve::Listener::accept(tcp_listener).await;
                set_socket_options(&tcp_stream, self.protocol);
                (Connection::Plain(tcp_stream), peer_addr)
            }
            IncomingConnections::Tls(ready_rx) => match ready_rx.recv().await {
                Some(ready_connection) => ready_connection,
                None => std::future::pending().await,
            },
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }
}

/// Accept connections and start a handshake task for each, until `cancel_token` is
/// cancelled or the listener receiving from `ready_tx` is gone.
///
/// Accepts through axum's `Listener` implementation for [`TcpListener`], whose signature
/// returns no error, so a socket error is handled the way every axum server here already
/// handles it.
async fn accept_tls_connections(
    mut tcp_listener: TcpListener,
    protocol: ApplicationProtocol,
    tls_acceptor: TlsAcceptor,
    ready_tx: mpsc::Sender<ReadyConnection>,
    cancel_token: CancellationToken,
) {
    loop {
        tokio::select! {
            () = cancel_token.cancelled() => return,
            () = ready_tx.closed() => return,
            (tcp_stream, peer_addr) = axum::serve::Listener::accept(&mut tcp_listener) => {
                set_socket_options(&tcp_stream, protocol);
                // Detached: a handshake ends on its own, within its timeout, and its send
                // fails at once when the listener is gone, so it needs no tracking here.
                tokio::spawn(complete_handshake(
                    tls_acceptor.clone(),
                    tcp_stream,
                    peer_addr,
                    ready_tx.clone(),
                ));
            }
        }
    }
}

/// Run the TLS handshake of one accepted connection and hand the connection over once it
/// completed; a handshake that fails or exceeds [`TLS_HANDSHAKE_TIMEOUT`] drops it.
async fn complete_handshake(
    tls_acceptor: TlsAcceptor,
    tcp_stream: TcpStream,
    peer_addr: SocketAddr,
    ready_tx: mpsc::Sender<ReadyConnection>,
) {
    let tls_stream = match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, tls_acceptor.accept(tcp_stream)).await {
        Ok(Ok(tls_stream)) => tls_stream,
        Ok(Err(error)) => {
            tracing::debug!(%peer_addr, %error, "TLS handshake failed; dropping the connection");
            return;
        }
        Err(_elapsed) => {
            tracing::debug!(%peer_addr, "TLS handshake timed out; dropping the connection");
            return;
        }
    };
    // The send fails only once the listener is gone, and then no one is left to serve the
    // connection, so dropping it is all there is to do.
    let _ = ready_tx.send((Connection::Tls(Box::new(tls_stream)), peer_addr)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_host_brackets_removes_exactly_one_surrounding_pair() {
        let cases = [
            ("[::1]", "::1"),
            ("::1", "::1"),
            ("[[::1]]", "[::1]"),
            ("[::1", "[::1"),
            ("::1]", "::1]"),
            ("localhost", "localhost"),
            ("0.0.0.0", "0.0.0.0"),
        ];

        for (host, expected) in cases {
            assert_eq!(strip_host_brackets(host), expected, "host {host:?}");
        }
    }
}
