use std::{
    collections::BTreeMap,
    io::{Error as IoError, ErrorKind},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use futures::{future::BoxFuture, FutureExt};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader, ReadBuf},
    net::UdpSocket,
    sync::mpsc::{self, Receiver, Sender},
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxResult<T> = Result<T, BoxError>;

const MAX_MESSAGE_LEN: usize = 1000;

pub async fn main(addr: SocketAddr) -> BoxResult<()> {
    Server::bind(addr).await?.run().await?;
    Ok(())
}

// struct Clock {}

/// similar to TcpStream, to send data using lrcp over udp
#[derive(Debug)]
struct LrcpStream {
    tx: mpsc::Sender<SocketMessage>,
}

impl LrcpStream {
    /// Create a LRCP socket connected to the given address and ready to send
    /// and receive data from it
    /// the session is the LRCP session number
    async fn connect(peer_addr: SocketAddr, session: u32) -> BoxResult<LrcpStream> {
        let (stream, mut conn) = LrcpStream::construct(peer_addr).await;
        let local_addr = conn.sock.local_addr()?;
        tracing::debug!("connecting to {peer_addr} from {}", local_addr);
        conn.connect(session).await?;
        conn.spawn_run();
        Ok(stream)
    }

    /// low level, internal method used by `connect`. This doesn't spawn any tokio task
    /// or attempt to initiate a connection
    async fn construct(peer_addr: SocketAddr) -> (LrcpStream, Connection) {
        let (_local_addr, socket) = get_socket().await;
        let (tx, rx) = mpsc::channel(1);
        let conn = Connection::new(socket, peer_addr, None, rx);
        let stream = LrcpStream { tx };
        (stream, conn)
    }

    fn construct_from_socket(
        socket: Arc<UdpSocket>,
        peer_addr: SocketAddr,
        server_rx: Receiver<(Bytes, Instant)>,
    ) -> (LrcpStream, Connection) {
        let (tx, rx) = mpsc::channel(1);
        let conn = Connection::new(socket, peer_addr, Some(server_rx), rx);
        let stream = LrcpStream { tx };
        (stream, conn)
    }

    fn split(self) -> (LrcpStreamRead, LrcpStreamWrite) {
        (
            LrcpStreamRead {
                tx: self.tx.clone(),
                fut: None,
            },
            LrcpStreamWrite { tx: self.tx },
        )
    }
}

struct Server {
    sock: Arc<UdpSocket>,
}

impl Server {
    async fn bind<A>(bind_addr: A) -> BoxResult<Server>
    where
        A: tokio::net::ToSocketAddrs,
    {
        let sock = UdpSocket::bind(bind_addr).await?;
        Ok(Server {
            sock: Arc::new(sock),
        })
    }

    async fn run(&self) -> BoxResult<()> {
        let mut clients = BTreeMap::new();
        loop {
            // the spec says that LRCP messages must be smaller than 1000 bytes.
            // make space for 1 more byte so that we can detect messages too big
            // and act accordingly (shut down the peer)
            let mut buf = [0; MAX_MESSAGE_LEN + 1];
            let (n, peer_addr) = self.sock.recv_from(&mut buf).await?;
            tracing::trace!("server got {n} bytes from {peer_addr}");
            if n == 0 {
                continue;
            }

            let tx_conn = clients.entry(peer_addr).or_insert_with(|| {
                let (tx, rx) = mpsc::channel(10);
                let (stream, conn) =
                    LrcpStream::construct_from_socket(Arc::clone(&self.sock), peer_addr, rx);
                conn.spawn_run();
                Reversal::new(stream).spawn_run();
                tx
            });

            if let Err(_) = tx_conn
                .send((Bytes::copy_from_slice(&buf[0..n]), Instant::now()))
                .await
            {
                // channel closed means the other side has dropped/closed, in this case
                // the connection is closed, so drop it from the map
                clients.remove(&peer_addr);
            }
        }
    }
}

/// low level connection handling timeouts, retransmission and connection state
struct Connection {
    sock: Arc<UdpSocket>,
    peer_addr: SocketAddr,

    /// when the connection is managed by a listener, it cannot simply call
    /// sock.recv_from since this would potentially yield bytes from other
    /// connections. In this case, the connection will be managed by the listener
    /// which will poll the socket, and dispatch the received bytes through the
    /// corresponding Sender.
    /// For Connection created through `Connect`, they act as clients, and this
    /// channel will be a dummy one, they can directly call sock.recv since they're bound
    /// through sock.connect
    /// Having it as an Option is messing the tokio::select! macro unfortunately :/
    server_rx: mpsc::Receiver<(Bytes, Instant)>,
    is_client_connection: bool,

    state: ConnectionState,

    /// The data received alongside an offset. Once the application
    /// has read some of it, it can be discarded, but the length of data
    /// should be preserved to keep the ACK position
    recv_buffer: BytesMut,
    recv_offset: u32,

    /// when a high level stream request to read but there's nothing in the
    /// recv_buffer and the connection isn't closing/closed, that means we need
    /// to park the request for read until we get some data, and *then* we respond.
    recv_chan: Option<(oneshot::Sender<Bytes>, usize)>,

    /// the data to send, along with an offset.
    /// Once the data we sent has been acknowledge, we can drop
    /// it to free the memory, but since the ACK are based on the
    /// position from the start of the stream, we need to keep in memory
    /// how many char we already sent and got acked
    send_buffer: BytesMut,
    sent_offset: u32,

    /// to communicate with a high level socket
    lrcp_socket_chan: Receiver<SocketMessage>,
}

impl Connection {
    fn new(
        sock: Arc<UdpSocket>,
        peer_addr: SocketAddr,
        server_rx: Option<Receiver<(Bytes, Instant)>>,
        lrcp_socket_chan: mpsc::Receiver<SocketMessage>,
    ) -> Self {
        let is_client_connection = server_rx.is_none();
        let server_rx = match server_rx {
            Some(rx) => rx,
            None => mpsc::channel(1).1,
        };
        Connection {
            sock,
            peer_addr,
            server_rx,
            is_client_connection,
            state: ConnectionState::Opening,
            recv_buffer: BytesMut::new(),
            recv_offset: 0,
            recv_chan: None,
            send_buffer: BytesMut::new(),
            sent_offset: 0,
            lrcp_socket_chan,
        }
    }

    /// attempt to establish a connection, sending the initial connect message
    /// and waiting for the acknowledgement, retrying if necessary before giving up
    /// This will also connect the socket to the peer_address
    async fn connect(&mut self, session: u32) -> BoxResult<()> {
        // so that sock.recv only gets stuff from the peer address
        self.sock.connect(self.peer_addr).await?;
        let connect_msg = Message::Connect { session }.to_bytes();

        if let ConnectionState::Open { .. } = self.state {
            return Err("Cannot call `connect` on an already connected session".into());
        }

        // do the handshake and wait for the ack
        let mut buf = [0; 40];
        for i in 0..3 {
            self.sock.send_to(&connect_msg, self.peer_addr).await?;
            match timeout(Duration::from_millis(5000), self.sock.recv(&mut buf)).await {
                Ok(n) => {
                    let n = n?;
                    let msg = parser::parse_message(&buf[0..n]).map_err(|e| e.clone_input())?;
                    match msg {
                        Message::Ack { session: s, len } if s == session && len == 0 => {
                            self.state = ConnectionState::Open { session };
                            tracing::debug!("connection to {} is now opened", self.peer_addr);
                            return Ok(());
                        }
                        _ => {
                            return Err(format!("Expected an initial ack but got {msg:?}").into());
                        }
                    }
                }
                Err(_) => {
                    tracing::trace!("timeout waiting for peer to ack CONNECT (attempt {i})");
                }
            };
        }
        Err("timeout waiting for peer to answer connect message".into())
    }

    /// start the inner loop for the connection in its own async task
    fn spawn_run(self) -> JoinHandle<()> {
        let addr = self.peer_addr;
        tokio::task::spawn(async move {
            match self.run().await {
                Ok(_) => (),
                Err(err) => tracing::error!("error while running connection for {addr} {err:?}"),
            }
        })
    }

    async fn run(mut self) -> BoxResult<()> {
        tracing::debug!("[{}] starting connection", self.peer_addr);
        self.sock.connect(self.peer_addr).await?;

        let mut lrcp_sock_closed = false;
        let mut buf = [0; MAX_MESSAGE_LEN + 1];

        loop {
            tokio::select! {
                // no server_rx means that the connection is in client mode
                // so calling sock.recv is guaranteed to read bytes intended for this socket.
                stuff = self.sock.recv(&mut buf), if self.is_client_connection => {
                    let len = match stuff {
                        Ok(x) => x,
                        Err(err) => {
                            tracing::error!("Error reading socket data: {err:?}");
                            self.state = ConnectionState::Closed;
                            break;
                        }
                    };

                    if len == 0 {
                        break;
                    };

                    self.on_message(Bytes::copy_from_slice(&buf[..len])).await?;
                    if let ConnectionState::Closed = self.state {
                        break;
                    }
                },
                x = self.server_rx.recv(), if !self.is_client_connection => {
                    let (bs, _now) = match x {
                        Some(x) => x,
                        None => {
                            tracing::warn!("Connection managed by a server but it has dropped, closing");
                            break
                        }
                    };
                    if bs.is_empty() {
                        break;
                    }
                    self.on_message(bs).await?;
                    if let ConnectionState::Closed = self.state {
                        break;
                    }
                }
                msg = self.lrcp_socket_chan.recv(), if !lrcp_sock_closed => {
                    match msg {
                        Some(msg) => self.on_socket_message(msg).await?,
                        None => {
                            tracing::trace!("[{}] lrcp socket chan is closed", self.peer_addr);
                            lrcp_sock_closed = true;
                            continue
                        },
                    }
                },
            }
        }

        tracing::debug!("Terminating connection for {}", self.peer_addr);
        Ok(())
    }

    async fn on_message(&mut self, raw: Bytes) -> BoxResult<()> {
        let message = parser::parse_message(&raw);
        tracing::debug!("[{}] Processing message {message:?}", self.peer_addr);

        match message {
            Err(err) => {
                tracing::debug!("invalid message received {err:?}");
                self.initiate_close().await
            }
            Ok(Message::Connect { session }) => {
                self.state = ConnectionState::Open { session };
                self.send_ack_data().await?;
                Ok(())
            }
            // TODO! not sure about this can_send business, it's probably better
            // to manually require a session when it makes sense.
            // Ok(_) if !self.state.can_send() => self.force_close().await,
            Ok(Message::Ack { session, len }) => {
                self.check_session(session).await?;
                let offset = self.sent_offset as usize;
                match (len as usize).cmp(&(self.send_buffer.len() + offset)) {
                    std::cmp::Ordering::Less => {
                        // all the data up to `len` have been received and can be discarded
                        // from the internal buffer of stuff to send
                        let start = match len.checked_sub(self.sent_offset) {
                            Some(x) => x,
                            None => {
                                // received an old ACK, pointing to some data that already
                                // has been ack'ed and discarded
                                return Ok(());
                            }
                        };

                        let x = self.send_buffer.split_to(start as usize);
                        match self.sent_offset.checked_add(x.len() as u32) {
                            Some(offset) => {
                                self.sent_offset = offset;
                                self.send_data().await
                            }
                            None => {
                                tracing::error!(
                                    "Connection sent more the max number of bytes ({}), closing it down",
                                    u32::MAX
                                );
                                self.initiate_close().await
                            }
                        }
                    }
                    std::cmp::Ordering::Equal => {
                        // everything we sent has been acked, so we can drop our internal buffer
                        self.send_buffer.clear();
                        self.sent_offset = len;
                        Ok(())
                    }
                    std::cmp::Ordering::Greater => {
                        // peer is misbehaving, acknoledging stuff we never sent
                        self.initiate_close().await
                    }
                }
            }
            Ok(Message::Data { session, pos, data }) => {
                self.check_session(session).await?;
                self.receive_data(pos, data).await?;
                Ok(())
            }
            Ok(Message::Close { session }) => {
                self.check_session(session).await?;
                let msg = Message::Close { session };
                self.sock.send_to(&msg.to_bytes(), self.peer_addr).await?;
                self.state = ConnectionState::Closed;
                Ok(())
            }
        }
    }

    async fn send_ack_data(&mut self) -> BoxResult<()> {
        let ack = Message::Ack {
            session: self.get_session()?,
            len: self.recv_offset + self.recv_buffer.len() as u32,
        };
        tracing::trace!("[{}] sending {ack:?}", self.peer_addr);
        self.sock.send_to(&ack.to_bytes(), self.peer_addr).await?;
        Ok(())
    }

    /// for when a session is required
    fn get_session(&self) -> BoxResult<u32> {
        match &self.state {
            ConnectionState::Open { session } => Ok(*session),
            s => Err(format!("Session is required but connection is in state {s:?}").into()),
        }
    }

    /// when this connection wants to close the lrcp stream for whatever reason.
    async fn initiate_close(&mut self) -> BoxResult<()> {
        if let Ok(session) = self.get_session() {
            self.sock
                .send_to(&Message::Close { session }.to_bytes(), self.peer_addr)
                .await?;
            self.state = ConnectionState::Closing { session };
        } else {
            self.state = ConnectionState::Closed;
        };

        Ok(())
    }

    async fn check_session(&mut self, session: u32) -> BoxResult<()> {
        match &self.state {
            ConnectionState::Open { session: s } | ConnectionState::Closing { session: s } => {
                if *s != session {
                    self.initiate_close().await?;
                    Err(format!("Invalid session for {}", self.peer_addr).into())
                } else {
                    Ok(())
                }
            }
            st => Err(format!("Requires a session but currently in state {st:?}").into()),
        }
    }

    /// send as much data as we can from the internal buffer
    async fn send_data(&mut self) -> BoxResult<()> {
        tracing::trace!(
            "[{}] sending {} bytes of data",
            self.peer_addr,
            self.send_buffer.len()
        );
        if self.send_buffer.is_empty() {
            return Ok(());
        }

        // 10 is the maximum length that a u32 in string format can take
        // if we happen not to send everything in the buffer it's okay
        // because this packet will generate an ACK at some point, and when
        // we receive this ACK, we'll attempt to send the remaining.
        let max_data_len = MAX_MESSAGE_LEN - 2 * 10 - "/data///".len();
        let len = std::cmp::min(max_data_len, self.send_buffer.len());
        let msg = Message::Data {
            session: self.get_session()?,
            pos: self.sent_offset,
            data: Bytes::copy_from_slice(&self.send_buffer[0..len]),
        };
        self.sock.send_to(&msg.to_bytes(), self.peer_addr).await?;
        // TODO setup the timers, we expect some ACKs for this data and may need to
        // retransmit
        Ok(())
    }

    async fn receive_data(&mut self, pos: u32, data: Bytes) -> BoxResult<()> {
        // usize -> u32 is OK because a LRCP messages are under 1000 bytes
        // (and the parser checks for that)
        assert!(data.len() < u32::MAX as usize);
        let data_len = data.len() as u32;
        match self.recv_offset.cmp(&pos) {
            std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
                if pos + data_len > self.recv_offset {
                    let start = self.recv_offset - pos;
                    let new_data = &data[start as usize..data.len()];
                    self.recv_buffer.extend_from_slice(new_data);
                }
                // else: we got stale data, simply resend an ack for the latest data
                self.send_ack_data().await?;
                // and also notify the attached stream of any potential available read
                self.yield_bytes().await?;
                Ok(())
            }
            std::cmp::Ordering::Greater => {
                // there are some missed data inbetween, so simply completely
                // ignore this chunk of data
                tracing::trace!(
                    "[{:?}] received data too far ahead, got {} but current offset is at {}. Ignoring packet.",
                    self.state,
                    pos,
                    self.recv_offset
                );
                Ok(())
            }
        }
    }

    async fn on_socket_message(&mut self, msg: SocketMessage) -> BoxResult<()> {
        tracing::debug!("[{}] processing socket message: {msg:?}", self.peer_addr);
        match msg {
            SocketMessage::SendData { data } => {
                self.send_buffer.extend_from_slice(&data);
                self.send_data().await
            }
            SocketMessage::Recv { result_chan, limit } => {
                self.recv_chan = Some((result_chan, limit));
                self.yield_bytes().await
            }
            SocketMessage::Shutdown => self.initiate_close().await,
        }
    }

    /// process a read request to a high level stream
    async fn yield_bytes(&mut self) -> BoxResult<()> {
        if self.recv_buffer.is_empty() {
            return Ok(());
        }

        let (chan, limit) = match self.recv_chan.take() {
            Some(x) => x,
            None => return Ok(()),
        };

        let slice_len = std::cmp::min(limit, self.recv_buffer.len());
        let result = self.recv_buffer.split_to(slice_len).freeze();
        self.recv_offset += slice_len as u32;
        match chan.send(result) {
            Ok(_) => Ok(()),
            Err(_) => {
                tracing::debug!(
                    "an lrcp socket requested some data but went away before we could send it back"
                );
                Ok(())
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ConnectionState {
    Opening,
    Open { session: u32 },
    Closing { session: u32 },
    Closed,
}

impl ConnectionState {
    fn is_open(&self) -> bool {
        match self {
            ConnectionState::Open { .. } => true,
            _ => false,
        }
    }
}

async fn get_socket() -> (SocketAddr, Arc<UdpSocket>) {
    // 0 asks the OS to assign a port
    let socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("can bind on localhost");
    let addr = socket
        .local_addr()
        .expect("bound socket has a local address");
    (addr, Arc::new(socket))
}

#[derive(Debug, PartialEq)]
pub enum Message {
    Connect { session: u32 },
    Ack { session: u32, len: u32 },
    Data { session: u32, pos: u32, data: Bytes },
    Close { session: u32 },
}

impl Message {
    fn to_bytes(&self) -> Vec<u8> {
        let mut result = vec![b'/'];
        match self {
            Message::Connect { session } => {
                result.extend_from_slice(b"connect/");
                result.extend_from_slice(session.to_string().as_bytes());
            }
            Message::Ack { session, len } => {
                result.extend_from_slice(b"ack/");
                result.extend_from_slice(session.to_string().as_bytes());
                result.push(b'/');
                result.extend_from_slice(len.to_string().as_bytes());
            }
            Message::Data { session, pos, data } => {
                result.extend_from_slice(b"data/");
                result.extend_from_slice(session.to_string().as_bytes());
                result.push(b'/');
                result.extend_from_slice(pos.to_string().as_bytes());
                result.push(b'/');
                result.reserve(data.len());
                for c in data {
                    if *c == b'/' || *c == b'\\' {
                        result.push(b'\\');
                    };
                    result.push(*c);
                }
            }
            Message::Close { session } => {
                result.extend_from_slice(b"close/");
                result.extend_from_slice(session.to_string().as_bytes());
            }
        };
        result.push(b'/');
        result
    }
}

struct LrcpStreamRead {
    tx: Sender<SocketMessage>,
    fut: Option<BoxFuture<'static, std::io::Result<Bytes>>>,
}

impl AsyncRead for LrcpStreamRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Some(fut) = self.fut.as_mut() {
            let bs = futures::ready!(fut.poll_unpin(cx))?;
            buf.put_slice(&bs);
            Poll::Ready(Ok(()))
        } else {
            let limit = buf.remaining();
            let tx = self.tx.clone();
            let fut = async move {
                let (otx, orx) = oneshot::channel();
                let msg = SocketMessage::Recv {
                    result_chan: otx,
                    limit,
                };
                tx.send(msg).await.map_err(|e| {
                    tracing::error!("cannot send data to underlying connection");
                    let kind = ErrorKind::ConnectionAborted;
                    IoError::new(kind, e)
                })?;
                let resp = orx.await.map_err(|e| {
                    tracing::error!("cannot receive data from underlying connection");
                    let kind = ErrorKind::ConnectionAborted;
                    IoError::new(kind, e)
                })?;
                let r: std::io::Result<Bytes> = Ok(resp);
                r
            };
            self.fut = Some(Box::pin(fut));
            self.poll_read(cx, buf)
        }
    }
}

/// messages used to communicate between the high level LrcpSocket and
/// the low level Connection
enum SocketMessage {
    SendData {
        data: Bytes,
    },
    /// request some data from the connection. At most `limit` bytes.
    Recv {
        result_chan: oneshot::Sender<Bytes>,
        limit: usize,
    },
    Shutdown,
}

impl std::fmt::Debug for SocketMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SendData { data } => f.debug_struct("SendData").field("data", data).finish(),
            Self::Recv {
                result_chan: _,
                limit,
            } => f
                .debug_struct("Recv")
                // .field("result_chan", result_chan)
                .field("limit", limit)
                .finish(),
            Self::Shutdown => write!(f, "Shutdown"),
        }
    }
}

impl LrcpStreamRead {
    /// Read some bytes from the underlying Connection
    /// blocks until there is some data to read
    /// if the returned Bytes object is empty, that means the connection
    /// is closed and no more data will be read
    async fn recv(&self) -> BoxResult<Bytes> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(SocketMessage::Recv {
                result_chan: tx,
                limit: usize::MAX,
            })
            .await?;
        Ok(rx.await?)
    }
}

struct LrcpStreamWrite {
    tx: Sender<SocketMessage>,
}

impl LrcpStreamWrite {
    async fn send(&self, data: Bytes) -> BoxResult<()> {
        self.tx.send(SocketMessage::SendData { data }).await?;
        Ok(())
    }

    /// close the underlying Connection
    async fn shutdown(&self) -> BoxResult<()> {
        self.tx.send(SocketMessage::Shutdown).await?;
        Ok(())
    }
}

struct Reversal {
    stream: LrcpStream,
}

impl Reversal {
    fn new(stream: LrcpStream) -> Self {
        Self { stream }
    }

    fn spawn_run(self) -> JoinHandle<()> {
        tokio::spawn(async {
            match self.run().await {
                Ok(_) => (),
                Err(err) => tracing::error!("{err:?}"),
            }
        })
    }

    async fn run(self) -> BoxResult<()> {
        let (rdr, wrt) = self.stream.split();
        let rdr = BufReader::new(rdr);
        let mut lines = rdr.split(b'\n');
        while let Some(line) = lines.next_segment().await? {
            tracing::trace!("reversal got a line! {line:?}");
            let mut data = BytesMut::with_capacity(line.len() + 1);
            for chr in line.into_iter().rev() {
                data.put_u8(chr);
            }
            data.put_u8(b'\n');
            wrt.send(data.freeze()).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use pretty_assertions::assert_eq;

    /// make sure to abort a task on drop, so that it's easier to
    /// do assertion in tests with less worry about cleaning up
    struct AbortHdl<T>(JoinHandle<T>);
    impl<T> Drop for AbortHdl<T> {
        fn drop(&mut self) {
            self.0.abort()
        }
    }

    #[test]
    fn test_serialization() {
        let msg = Message::Data {
            session: 123,
            pos: 1,
            data: Bytes::copy_from_slice(b"coucou/blah"),
        };

        // convert to string so that any errors is more legible
        assert_eq!(
            String::from_utf8(msg.to_bytes()),
            Ok(r#"/data/123/1/coucou\/blah/"#.to_string()),
            "correctly escape slashes"
        );
    }

    #[tokio::test]
    async fn test_reversal() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = Server::bind("127.0.0.1:0").await.expect("bind server");
        let server_addr = server
            .sock
            .local_addr()
            .expect("server has a local address");
        tracing::info!("server has address {server_addr}");
        let _hdl = AbortHdl(tokio::spawn(async move {
            server.run().await.expect("server ran properly");
        }));
        let client = LrcpStream::connect(server_addr, 1234)
            .await
            .expect("client connected");
        let (client_rdr, client_wrt) = client.split();
        client_wrt
            .send(Bytes::from_static(b"hello\n"))
            .await
            .expect("send data");
        let resp = client_rdr.recv().await.expect("receive response");
        client_wrt.shutdown().await.expect("shutdown");
        assert_eq!(&resp, b"olleh\n".as_slice(), "message is reversed");
    }
}

mod parser {
    use super::{Message, MAX_MESSAGE_LEN};
    use bytes::Bytes;
    use nom::{
        branch::alt,
        bytes::complete::{escaped, tag, take_while1},
        character::complete::{self, one_of},
        combinator::{eof, map},
        error::{ErrorKind, ParseError},
        sequence::{delimited, preceded, separated_pair, tuple},
        Err, Finish, IResult,
    };

    #[derive(Debug, PartialEq, thiserror::Error, Clone)]
    pub enum InvalidMessage<I> {
        #[error("Message too big. Max len is {max_len} but got {len}")]
        MessageTooBig { max_len: usize, len: usize },
        #[error("Invalid number: {0}")]
        InvalidNumber(u32),
        #[error("Nom error")]
        Nom(I, ErrorKind),
    }

    impl<I> ParseError<I> for InvalidMessage<I> {
        fn from_error_kind(input: I, kind: ErrorKind) -> Self {
            InvalidMessage::Nom(input, kind)
        }

        fn append(input: I, kind: ErrorKind, other: Self) -> Self {
            other
        }
    }

    /// when returning a Result there is often a problem of lifetime where the local
    /// buffer escape the scope, so copy the input captured in the error into an owned type
    impl<'input> InvalidMessage<&'input [u8]> {
        pub fn clone_input(self) -> InvalidMessage<Bytes> {
            use InvalidMessage::*;
            match self {
                MessageTooBig { max_len, len } => MessageTooBig { max_len, len },
                InvalidNumber(n) => InvalidNumber(n),
                Nom(i, k) => Nom(Bytes::copy_from_slice(i), k),
            }
        }
    }

    type ParseResult<'input, T> = IResult<&'input [u8], T, InvalidMessage<&'input [u8]>>;

    pub fn parse_message(input: &[u8]) -> Result<Message, InvalidMessage<&[u8]>> {
        if input.len() > MAX_MESSAGE_LEN {
            return Err(InvalidMessage::MessageTooBig {
                max_len: MAX_MESSAGE_LEN,
                len: input.len(),
            });
        }

        delimited(tag("/"), inner_message, tuple((tag("/"), eof)))(input)
            .finish()
            .map(|(_remaining, result)| result)
    }

    fn inner_message(input: &[u8]) -> ParseResult<Message> {
        alt((connect, ack, close, data))(input)
    }

    fn connect(input: &[u8]) -> ParseResult<Message> {
        map(preceded(tag("connect/"), number), |session| {
            Message::Connect { session }
        })(input)
    }

    fn ack(input: &[u8]) -> ParseResult<Message> {
        map(
            preceded(tag("ack/"), separated_pair(number, tag("/"), number)),
            |(session, len)| Message::Ack { session, len },
        )(input)
    }
    fn data(input: &[u8]) -> ParseResult<Message> {
        map(
            preceded(
                tag("data/"),
                tuple((number, tag("/"), number, tag("/"), escaped_string)),
            ),
            |(session, _, pos, _, data)| Message::Data { session, pos, data },
        )(input)
    }

    fn close(input: &[u8]) -> ParseResult<Message> {
        map(preceded(tag("close/"), number), |session| Message::Close {
            session,
        })(input)
    }

    fn number(input: &[u8]) -> ParseResult<u32> {
        match complete::u32(input) {
            Ok((rest, x)) => {
                if x > 2147483648 {
                    Err(nom::Err::Failure(InvalidMessage::InvalidNumber(x)))
                } else {
                    Ok((rest, x))
                }
            }
            // when we expect a u32 and we encounter an error, this is a fatal
            // error (typically, overflowing the u32)
            Err(Err::Error(err)) => Err(Err::Failure(err)),
            Err(err) => Err(err),
        }
    }

    /// recognize an ASCII string
    fn escaped_string(input: &[u8]) -> ParseResult<Bytes> {
        let normal = take_while1(|c: u8| c.is_ascii() && c != b'/' && c != b'\\');
        map(escaped(normal, '\\', one_of(r#"/\"#)), |s: &[u8]| {
            // munge the escaped chars: \/ => /
            let mut bs = Vec::with_capacity(s.len());
            let mut escaped = false;
            for c in s {
                if escaped {
                    bs.push(*c);
                    escaped = false;
                } else {
                    if *c == b'\\' {
                        escaped = true;
                    } else {
                        bs.push(*c);
                    }
                }
            }
            Bytes::copy_from_slice(&bs)
        })(input)
    }

    #[cfg(test)]
    mod test {
        use super::*;
        use bytes::Bytes;
        use pretty_assertions::assert_eq;

        #[test]
        fn test_connect() {
            assert_eq!(
                parse_message(b"/connect/123456/"),
                Ok(Message::Connect { session: 123456 })
            );
        }

        #[test]
        fn test_overflow() {
            assert_eq!(
                parse_message(b"/connect/2147483649/"),
                Err(InvalidMessage::InvalidNumber(2147483649)),
                "valid u32 but too big for this protocol"
            );

            let too_big = "4294967296/"; // u32::MAX + 1
            assert_eq!(
                parse_message(format!("/connect/{}", too_big).as_bytes()),
                Err(InvalidMessage::Nom(too_big.as_bytes(), ErrorKind::Digit)),
                "invalid u32, too big"
            );
        }

        #[test]
        fn test_close() {
            assert_eq!(
                parse_message(b"/close/123456/"),
                Ok(Message::Close { session: 123456 })
            )
        }

        #[test]
        fn test_data() {
            assert_eq!(
                parse_message(b"/data/123456/12/coucou/"),
                Ok(Message::Data {
                    session: 123456,
                    pos: 12,
                    data: Bytes::from_static(b"coucou")
                })
            )
        }

        #[test]
        fn test_data_escaped() {
            assert_eq!(
                parse_message(br#"/data/123456/12/cou\/cou/"#),
                Ok(Message::Data {
                    session: 123456,
                    pos: 12,
                    data: Bytes::from_static(b"cou/cou"),
                }),
                "escaped slash"
            );

            assert_eq!(
                parse_message(br#"/data/123456/12/cou\\cou/"#),
                Ok(Message::Data {
                    session: 123456,
                    pos: 12,
                    data: Bytes::from_static(br#"cou\cou"#),
                }),
                "escaped backslash"
            );
        }

        #[test]
        fn test_message_too_big() {
            let data: String = std::iter::repeat('a').take(MAX_MESSAGE_LEN).collect();
            let msg = format!("/data/123456/0/{}/", data);
            assert_eq!(
                parse_message(msg.as_bytes()),
                Err(InvalidMessage::MessageTooBig {
                    max_len: MAX_MESSAGE_LEN,
                    len: msg.len()
                })
            );
        }
    }
}
