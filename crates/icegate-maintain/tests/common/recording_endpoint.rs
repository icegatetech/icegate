//! A loopback HTTP/1.1 endpoint that answers every request from a fixed
//! function and records what it was sent.
//!
//! It stands in for a remote service when the assertion is about the requests
//! themselves — which credentials signed them, which token they presented — so
//! it keeps the method, the request target and the header fields of each one.
//! It serves on threads of its own rather than on the test's runtime, so it
//! keeps answering while the test waits on a child process, and it listens on
//! `127.0.0.1` only.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// How long a connection may stay silent before the endpoint closes it, so a
/// client that never hangs up cannot keep [`RecordingEndpoint`]'s drop waiting
/// on its thread.
const CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// One request as the endpoint received it.
#[derive(Debug, Clone)]
pub struct RecordedRequest {
    /// Request method as sent (`GET`, `HEAD`, ...).
    pub method: String,
    /// Request target: the path and any query string.
    pub target: String,
    /// Header fields in arrival order. Names are lower-cased on receipt: field
    /// names are case-insensitive, so a lookup compares lower-case names only.
    pub headers: Vec<(String, String)>,
}

/// How an endpoint answers a request: the status, extra header fields, and the
/// body. The endpoint adds `content-length` itself.
pub type Answer = fn(&RecordedRequest) -> (u16, Vec<(String, String)>, Vec<u8>);

/// The request log shared by the endpoint and its connection threads.
type RequestLog = Arc<Mutex<Vec<RecordedRequest>>>;

/// A running endpoint. Dropping it stops the listener and joins every thread
/// it started; a thread that panicked fails the test from the drop.
pub struct RecordingEndpoint {
    address: SocketAddr,
    requests: RequestLog,
    is_stopping: Arc<AtomicBool>,
    /// Hands back the connection threads once the accept loop has stopped.
    acceptor: Option<JoinHandle<Vec<JoinHandle<()>>>>,
}

impl RecordingEndpoint {
    /// Bind `127.0.0.1` on a free port and answer every request with `answer`.
    ///
    /// The listener is bound before this returns, so a client may connect at
    /// once: the kernel queues the connection until the accept loop takes it.
    pub fn start(answer: Answer) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind the recording endpoint to 127.0.0.1");
        let address = listener.local_addr().expect("address of the recording endpoint");
        let requests = RequestLog::default();
        let is_stopping = Arc::new(AtomicBool::new(false));
        let acceptor = {
            let requests = Arc::clone(&requests);
            let is_stopping = Arc::clone(&is_stopping);
            thread::spawn(move || accept_connections(&listener, answer, &requests, &is_stopping))
        };
        Self {
            address,
            requests,
            is_stopping,
            acceptor: Some(acceptor),
        }
    }

    /// The base URL clients reach the endpoint at, without a trailing slash.
    pub fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    /// Every request received so far, in the order the endpoint finished
    /// reading them.
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().expect("request log lock").clone()
    }
}

impl Drop for RecordingEndpoint {
    fn drop(&mut self) {
        self.is_stopping.store(true, Ordering::SeqCst);
        // The accept loop blocks until a client connects; this connection is
        // what wakes it up to see the flag. It fails only when the loop has
        // already ended, and the join below reports why it did.
        drop(TcpStream::connect(self.address));
        let Some(acceptor) = self.acceptor.take() else {
            return;
        };
        let failure = acceptor.join().map_or_else(
            |_| Some("the accept thread of the recording endpoint panicked".to_string()),
            |connections| {
                let panicked = connections.into_iter().map(JoinHandle::join).filter(Result::is_err).count();
                (panicked > 0).then(|| format!("{panicked} connection thread(s) of the recording endpoint panicked"))
            },
        );
        // A second panic while the test is already unwinding would abort the
        // process and hide the first failure.
        if !thread::panicking()
            && let Some(failure) = failure
        {
            panic!("{failure}");
        }
    }
}

/// Accept connections until the endpoint is dropped, serving each on a thread
/// of its own; returns those threads so the drop can join them.
fn accept_connections(
    listener: &TcpListener,
    answer: Answer,
    requests: &RequestLog,
    is_stopping: &AtomicBool,
) -> Vec<JoinHandle<()>> {
    let mut connections = Vec::new();
    for stream in listener.incoming() {
        if is_stopping.load(Ordering::SeqCst) {
            break;
        }
        let stream = stream.expect("accept a connection on the recording endpoint");
        let requests = Arc::clone(requests);
        connections.push(thread::spawn(move || serve_connection(stream, answer, &requests)));
    }
    connections
}

/// Serve the requests of one keep-alive connection until the client hangs up
/// or stays silent past [`CONNECTION_IDLE_TIMEOUT`].
fn serve_connection(stream: TcpStream, answer: Answer, requests: &Mutex<Vec<RecordedRequest>>) {
    stream
        .set_read_timeout(Some(CONNECTION_IDLE_TIMEOUT))
        .expect("set the idle timeout of a connection");
    let mut reader = BufReader::new(stream.try_clone().expect("clone the connection for reading"));
    let mut writer = stream;
    while let Some(request) = read_request(&mut reader) {
        requests.lock().expect("request log lock").push(request.clone());
        let (status, headers, body) = answer(&request);
        // A client that hung up before reading its answer has nothing left to
        // send either.
        if write_response(&mut writer, &request, status, &headers, &body).is_err() {
            return;
        }
    }
}

/// Read one request, discarding its body, or `None` once the client has hung
/// up (or stayed silent past the idle timeout).
///
/// A request this endpoint cannot parse panics: the clients under test speak
/// HTTP/1.1, so a malformed request means the endpoint itself is wrong.
fn read_request(reader: &mut impl BufRead) -> Option<RecordedRequest> {
    let request_line = read_line(reader)?;
    let mut parts = request_line.split(' ');
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else {
        panic!("malformed request line {request_line:?}");
    };

    let mut headers = Vec::new();
    loop {
        let line = read_line(reader)?;
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').unwrap_or_else(|| panic!("malformed header line {line:?}"));
        headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
    }

    assert!(
        !headers.iter().any(|(name, _)| name == "transfer-encoding"),
        "the recording endpoint reads only `content-length` bodies, got {method} {target} with a transfer coding"
    );
    let body_length = headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .map_or(0, |(_, value)| value.parse::<u64>().expect("numeric content-length"));
    let discarded = io::copy(&mut reader.take(body_length), &mut io::sink()).ok()?;
    if discarded < body_length {
        return None;
    }

    Some(RecordedRequest {
        method: method.to_string(),
        target: target.to_string(),
        headers,
    })
}

/// One line without its CRLF terminator, or `None` once the client has hung up
/// or the read timed out.
fn read_line(reader: &mut impl BufRead) -> Option<String> {
    let mut line = String::new();
    match reader.read_line(&mut line) {
        Ok(0) | Err(_) => None,
        Ok(_) => Some(line.trim_end_matches(['\r', '\n']).to_string()),
    }
}

/// Write one response. A `HEAD` answer states the length of `body` but never
/// carries it, as HTTP requires.
fn write_response(
    writer: &mut impl Write,
    request: &RecordedRequest,
    status: u16,
    headers: &[(String, String)],
    body: &[u8],
) -> io::Result<()> {
    // Buffered so the status line and fields leave in one write. The reason
    // phrase is optional (RFC 9112, section 4) and left empty.
    let mut response = Vec::with_capacity(128 + body.len());
    write!(response, "HTTP/1.1 {status} \r\n")?;
    for (name, value) in headers {
        write!(response, "{name}: {value}\r\n")?;
    }
    write!(response, "content-length: {}\r\n\r\n", body.len())?;
    if request.method != "HEAD" {
        response.extend_from_slice(body);
    }
    writer.write_all(&response)?;
    writer.flush()
}
