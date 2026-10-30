//! Transports, dialing and frame I/O for the blocking client.

use std::io::{self, Read, Write};
use std::net::Shutdown;
#[cfg(feature = "tcp")]
use std::net::{TcpStream, ToSocketAddrs};
#[cfg(feature = "uds")]
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use crate::codec::{CodecID, CodecImpl};
use crate::codec_registry::codec_by_id;
use crate::error::Error;
use crate::frame::{build_header, frame_header_len_for_version, parse_header, FRAME_HEADER_LEN_V3};
use crate::protocol::{client_hello, parse_server_hello, HandshakeConfig, HANDSHAKE_HEADER_LEN};
use crate::transport::Endpoint;

pub(super) enum Socket {
    #[cfg(feature = "tcp")]
    Tcp(TcpStream),
    #[cfg(feature = "uds")]
    Uds(UnixStream),
}

macro_rules! each_socket {
    ($socket:expr, $s:ident => $body:expr) => {
        match $socket {
            #[cfg(feature = "tcp")]
            Socket::Tcp($s) => $body,
            #[cfg(feature = "uds")]
            Socket::Uds($s) => $body,
        }
    };
}

impl Socket {
    pub(super) fn try_clone(&self) -> io::Result<Self> {
        match self {
            #[cfg(feature = "tcp")]
            Socket::Tcp(s) => s.try_clone().map(Socket::Tcp),
            #[cfg(feature = "uds")]
            Socket::Uds(s) => s.try_clone().map(Socket::Uds),
        }
    }

    pub(super) fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        each_socket!(self, s => s.shutdown(how))
    }

    pub(super) fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        each_socket!(self, s => s.set_read_timeout(timeout))
    }

    pub(super) fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        each_socket!(self, s => s.set_write_timeout(timeout))
    }
}

impl Read for Socket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        each_socket!(self, s => s.read(buf))
    }
}

impl Write for Socket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        each_socket!(self, s => s.write(buf))
    }

    fn flush(&mut self) -> io::Result<()> {
        each_socket!(self, s => s.flush())
    }
}

/// What the handshake settled for a connection.
pub(super) struct Config {
    pub version: u8,
    pub codec_id: CodecID,
    pub codec: &'static dyn CodecImpl,
    pub max_frame: u32,
    /// Frame header length for `version`.
    pub header_len: usize,
}

impl Config {
    pub(super) fn new(negotiated: HandshakeConfig) -> Result<Self, Error> {
        Ok(Self {
            version: negotiated.version,
            codec_id: negotiated.codec_id,
            codec: codec_by_id(negotiated.codec_id)
                .ok_or(Error::UnsupportedCodec(negotiated.codec_id.0))?,
            max_frame: negotiated.max_frame,
            header_len: frame_header_len_for_version(negotiated.version)?,
        })
    }
}

/// The time until `deadline`, or `None` once it has passed.
pub(super) fn time_left(deadline: Instant) -> Option<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
}

/// Dials `addr`. Returns [`Error::ConnectTimeout`] if `deadline` passes
/// first.
pub(super) fn dial(addr: &str, deadline: Option<Instant>) -> Result<Socket, Error> {
    // Checked for every transport: it is what ends the retries in connect.
    #[cfg_attr(not(feature = "tcp"), allow(unused_variables))]
    let limit = match deadline {
        Some(deadline) => Some(time_left(deadline).ok_or(Error::ConnectTimeout(None))?),
        None => None,
    };
    match Endpoint::parse(addr) {
        #[cfg(feature = "tcp")]
        Endpoint::Tcp(addr) => {
            let stream = match limit {
                Some(limit) => connect_tcp_within(addr, limit).map_err(connect_timeout_io)?,
                None => TcpStream::connect(addr)?,
            };
            stream.set_nodelay(true)?;
            Ok(Socket::Tcp(stream))
        }
        // Connecting to a Unix socket does not wait for the server.
        #[cfg(feature = "uds")]
        Endpoint::Uds(path) => Ok(Socket::Uds(connect_uds(path)?)),
        #[allow(unreachable_patterns)]
        endpoint => Err(endpoint.not_enabled()),
    }
}

/// `connect_timeout` takes one resolved address; try each, as `connect` does.
#[cfg(feature = "tcp")]
fn connect_tcp_within(addr: &str, limit: Duration) -> io::Result<TcpStream> {
    let mut last_err = None;
    for addr in addr.to_socket_addrs()? {
        match TcpStream::connect_timeout(&addr, limit) {
            Ok(stream) => return Ok(stream),
            Err(err) => last_err = Some(err),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "address resolved to nothing")
    }))
}

/// Connects to a socket path, or with `@name` to an abstract socket.
#[cfg(feature = "uds")]
fn connect_uds(path: &str) -> io::Result<UnixStream> {
    match path.strip_prefix('@') {
        #[cfg(target_os = "linux")]
        Some(name) => {
            use std::os::linux::net::SocketAddrExt;
            let addr = std::os::unix::net::SocketAddr::from_abstract_name(name)?;
            UnixStream::connect_addr(&addr)
        }
        #[cfg(not(target_os = "linux"))]
        Some(_) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "abstract UDS is only supported on linux",
        )),
        None => UnixStream::connect(path),
    }
}

/// Whether a dial failure may succeed on a later attempt. Malformed or
/// unsupported addresses never will.
pub(super) fn is_dial_error(err: &Error) -> bool {
    matches!(
        err,
        Error::Io(io) if !matches!(
            io.kind(),
            io::ErrorKind::InvalidInput | io::ErrorKind::Unsupported
        )
    )
}

/// Limits each read and write on `socket` to the time left until
/// `deadline`, or lifts the limit for `None`.
pub(super) fn limit_io(socket: &Socket, deadline: Option<Instant>) -> Result<(), Error> {
    let limit = match deadline {
        Some(deadline) => Some(time_left(deadline).ok_or(Error::ConnectTimeout(None))?),
        None => None,
    };
    socket.set_read_timeout(limit)?;
    socket.set_write_timeout(limit)?;
    Ok(())
}

fn is_timeout(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

fn connect_timeout_io(err: io::Error) -> Error {
    if is_timeout(&err) {
        Error::ConnectTimeout(None)
    } else {
        Error::Io(err)
    }
}

/// Reports a read or write that ran past [`limit_io`] as
/// [`Error::ConnectTimeout`].
pub(super) fn connect_timeout(err: Error) -> Error {
    match err {
        Error::Io(io) => connect_timeout_io(io),
        err => err,
    }
}

/// Reports a read that ran past the heartbeat timeout as
/// [`Error::RecvTimeout`].
pub(super) fn as_recv_timeout(err: Error) -> Error {
    match err {
        Error::Io(io) if is_timeout(&io) => Error::RecvTimeout,
        err => err,
    }
}

pub(super) fn handshake(
    socket: &mut Socket,
    codecs: &[CodecID],
    max_frame: u32,
    version: u8,
) -> Result<HandshakeConfig, Error> {
    socket.write_all(&client_hello(codecs, max_frame, version)?)?;
    let mut response = [0u8; HANDSHAKE_HEADER_LEN];
    socket.read_exact(&mut response)?;
    parse_server_hello(&response, codecs, version)
}

/// A received frame: stream ID, sequence number, flags and payload.
pub(super) type Frame = (u32, u64, u8, Vec<u8>);

/// Reads the next frame, or `None` if the peer closed the transport at a frame
/// boundary.
pub(super) fn read_frame(config: &Config, reader: &mut impl Read) -> Result<Option<Frame>, Error> {
    let header_len = config.header_len;
    let mut header = [0u8; FRAME_HEADER_LEN_V3];
    let read = loop {
        match reader.read(&mut header[..header_len]) {
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            result => break result?,
        }
    };
    if read == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut header[read..header_len])?;
    let (stream_id, seq, flags, payload_len) = parse_header(&header[..header_len], config.version)?;
    if payload_len as u32 > config.max_frame {
        return Err(Error::FrameTooLarge(payload_len));
    }
    // Read into reserved capacity, which needs no zeroing first.
    let mut payload = Vec::with_capacity(payload_len);
    reader.take(payload_len as u64).read_to_end(&mut payload)?;
    if payload.len() < payload_len {
        return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
    }
    Ok(Some((stream_id, seq, flags, payload)))
}

/// Writes a frame without flushing.
pub(super) fn write_frame(
    config: &Config,
    writer: &mut impl Write,
    stream_id: u32,
    seq: u64,
    flags: u8,
    payload: &[u8],
) -> Result<(), Error> {
    let header = build_header(
        config.version,
        stream_id,
        seq,
        flags,
        payload.len(),
        config.max_frame,
    )?;
    writer.write_all(&header[..config.header_len])?;
    writer.write_all(payload)?;
    Ok(())
}
