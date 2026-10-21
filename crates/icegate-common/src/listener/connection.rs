//! One accepted connection, plain or TLS, behind a single I/O type.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

/// A connection handed out by a [`ServerListener`](crate::listener::ServerListener): plain
/// TCP, or TLS whose handshake has already completed.
///
/// Every read and write goes straight to the inner stream, so a server is handed the same
/// I/O type whether or not TLS is on.
#[derive(Debug)]
pub enum Connection {
    /// Plain TCP, from a listener without a certificate.
    Plain(TcpStream),
    /// TLS over TCP. Boxed so a plain connection does not carry the size of the TLS state.
    Tls(Box<TlsStream<TcpStream>>),
}

// The poll methods receive `self` pinned (`Pin<&mut Self>`): a promise that the value will
// not move while an operation is in flight. Both inner streams are `Unpin` (moving them is
// always safe), so `get_mut` may take the plain `&mut` back out and `Pin::new` re-pin the
// inner stream, with no `unsafe`.
impl AsyncRead for Connection {
    fn poll_read(self: Pin<&mut Self>, context: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(context, buffer),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(context, buffer),
        }
    }
}

impl AsyncWrite for Connection {
    fn poll_write(self: Pin<&mut Self>, context: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(context, buffer),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(context, buffer),
        }
    }

    // Delegated, with `is_write_vectored`, so the connection offers exactly the vectored
    // writes its inner stream does, as that stream did when it was served directly.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write_vectored(context, buffers),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write_vectored(context, buffers),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(stream) => stream.is_write_vectored(),
            Self::Tls(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(context),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(context),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(context),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(context),
        }
    }
}

/// The addresses of the TCP connection under either variant, so a gRPC handler reads the
/// peer address the same way whether or not TLS is on.
#[cfg(feature = "grpc")]
impl tonic::transport::server::Connected for Connection {
    type ConnectInfo = tonic::transport::server::TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        let tcp_stream = match self {
            Self::Plain(stream) => stream,
            Self::Tls(stream) => stream.get_ref().0,
        };
        tcp_stream.connect_info()
    }
}
