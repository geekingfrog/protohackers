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
use futures::{future::BoxFuture, Future, FutureExt};
use pin_project::pin_project;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader, ReadBuf},
    net::{ToSocketAddrs, UdpSocket},
    sync::mpsc::{self, Receiver, Sender},
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxResult<T> = Result<T, BoxError>;

const MAX_MESSAGE_LEN: usize = 1000;

pub async fn main(addr: SocketAddr) -> BoxResult<()> {
    // let sock = UdpSocket::bind(addr).await?;
    // let mut buf = BytesMut::with_capacity(1024);
    // buf.extend_from_slice(b"/connect/123456/");
    // let stuff = buf.split().freeze();
    // let stuff = std::str::from_utf8(&stuff).unwrap();
    // println!("stuff: {stuff:?}");
    // let parsed = parser::parse_message(&stuff);
    // println!("{parsed:#?}");
    Server::new(addr).await?.run().await?;
    Ok(())
}

struct Server {
    sock: Arc<UdpSocket>,
}

impl Server {
    /// binds to the given address
    async fn new(addr: SocketAddr) -> BoxResult<Server> {
        let sock = Arc::new(UdpSocket::bind(addr).await?);
        Ok(Server { sock })
    }

    /// listen to the socket and manage connections: create, forward messages
    /// and send timer events.
    async fn run(&mut self) -> BoxResult<()> {
        let mut clients = BTreeMap::new();
        loop {
            // the spec says that LRCP messages must be smaller than 1000 bytes.
            // make space for 1 more byte so that we can detect messages too big
            // and act accordingly (shut down the peer)
            let mut buf = [0; MAX_MESSAGE_LEN + 1];
            let (n, peer_addr) = self.sock.recv_from(&mut buf).await?;
            if n == 0 {
                continue;
            }

            let tx_conn = clients.entry(peer_addr).or_insert_with(|| {
                let (conn, tx, _) = Connection::new(Arc::clone(&self.sock), peer_addr);
                conn.spawn_run();
                tx
            });

            if let Err(_) = tx_conn
                .send(ServerMessage::SocketData {
                    data: Bytes::copy_from_slice(&buf[0..n]),
                    now: Instant::now(),
                })
                .await
            {
                // channel closed means the other side has dropped/closed, in this case
                // the connection is closed, so drop it from the map
                clients.remove(&peer_addr);
            }
        }
    }
}

#[derive(Debug)]
enum ServerMessage {
    SocketData { data: Bytes, now: Instant },
    // Timer { time: Instant },
}

#[derive(Debug)]
struct Connection {
    sock: Arc<UdpSocket>,
    msg_chan: Receiver<ServerMessage>,
    state: ConnectionState,
    session: Option<u32>,

    /// The data received alongside an offset. Once the application
    /// has read some of it, it can be discarded, but the length of data
    /// should be preserved to keep the ACK position
    recv_buffer: BytesMut,
    recv_offset: u32,

    /// the data to send, along with an offset.
    /// Once the data we sent has been acknowledge, we can drop
    /// it to free the memory, but since the ACK are based on the
    /// position from the start of the stream, we need to keep in memory
    /// how many char we already sent and got acked
    send_buffer: BytesMut,
    sent_offset: u32,

    peer_addr: SocketAddr,

    /// to communicate with a high level socket
    lrcp_socket_chan: Receiver<SocketMessage>,
}

#[derive(Debug, PartialEq)]
enum ConnectionState {
    Open,
    Closed,
}

impl ConnectionState {
    fn can_send(&self) -> bool {
        matches!(self, ConnectionState::Open)
    }
}

impl Connection {
    /// The Connection can be used for spawn_run in its own tokio task.
    /// The Sender<ServerMessage> is to communicate from the server and pass
    /// bytes read from the socket + timer events
    /// the LrcpSocket is the higher level abstraction that can be used
    fn new(
        sock: Arc<UdpSocket>,
        peer_addr: SocketAddr,
    ) -> (Self, Sender<ServerMessage>, LrcpSocket) {
        let (tx, msg_chan) = mpsc::channel(1);
        let (lrcp_tx, lrcp_rx) = mpsc::channel(1);
        let lrcp_socket = LrcpSocket { tx: lrcp_tx };
        let conn = Connection {
            sock,
            msg_chan,
            state: ConnectionState::Open,
            session: None,
            recv_buffer: BytesMut::new(),
            recv_offset: 0,
            send_buffer: BytesMut::new(),
            sent_offset: 0,
            peer_addr,
            lrcp_socket_chan: lrcp_rx,
        };
        (conn, tx, lrcp_socket)
    }

    /// to create a client to connect to a given server
    /// this will bind and connect to the peer_addr, send the first Connect
    /// message, and wait until it receive the corresponding ACK, with some
    /// retry logic.
    async fn connect(
        sock: UdpSocket,
        peer_addr: SocketAddr,
        session: u32,
    ) -> BoxResult<(Connection, Sender<ServerMessage>, LrcpSocket)> {
        sock.connect(peer_addr).await?;
        let sock = Arc::new(sock);
        let (conn, tx, lrcp_socket) = Connection::new(Arc::clone(&sock), peer_addr);
        let connect_msg = Message::Connect { session }.to_bytes();

        // do the handshake and wait for the ack
        let mut buf = [0; 40];
        for _ in 0..3 {
            sock.send_to(&connect_msg, peer_addr).await?;
            match timeout(Duration::from_millis(1000), sock.recv(&mut buf)).await {
                Ok(n) => {
                    let n = n?;
                    let msg = parser::parse_message(&buf[0..n]).map_err(|e| e.clone_input())?;
                    match msg {
                        Message::Ack { session: s, len } if s == session && len == 0 => {
                            return Ok((conn, tx, lrcp_socket));
                        }
                        _ => {
                            return Err(format!("Expected an initial ack but got {msg:?}").into());
                        }
                    }
                }
                Err(_) => {
                    tracing::trace!("timeout waiting for peer to ack CONNECT");
                }
            };
        }
        Err("timeout waiting for peer to answer connect message".into())
    }

    // start the inner loop for the connection in its own async task
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
        tracing::debug!("starting connection for {}", self.peer_addr);
        println!("starting connection for {}", self.peer_addr);
        let mut lrcp_sock_closed = false;

        loop {
            tokio::select! {
                msg = self.msg_chan.recv() => {
                    match msg {
                        Some(ServerMessage::SocketData{data, now}) => {
                            self.on_message(data, now).await?;
                            if self.state == ConnectionState::Closed {
                                break;
                            }
                        },
                        None => {
                            tracing::warn!("server channel is closed, shutting down the connection {}", self.peer_addr);
                            break;
                        }
                    }
                },
                msg = self.lrcp_socket_chan.recv(), if !lrcp_sock_closed => {
                    match msg {
                        Some(msg) => self.on_socket_message(msg).await?,
                        None => {
                            tracing::trace!("lrcp socket chan is closed {}", self.peer_addr);
                            lrcp_sock_closed = true;
                            continue
                        },
                    }
                }
            }
        }

        tracing::debug!("Terminating connection for {}", self.peer_addr);
        Ok(())
    }

    async fn on_message(&mut self, raw: Bytes, _now: Instant) -> BoxResult<()> {
        let message = parser::parse_message(&raw);
        tracing::trace!("[{}] Processing message {message:?}", self.peer_addr);

        match message {
            Err(err) => {
                tracing::debug!("invalid message received {err:?}");
                self.force_close().await
            }
            Ok(_) if !self.state.can_send() => self.force_close().await,
            Ok(Message::Connect { session }) => {
                self.session = Some(session);
                self.send_ack_data().await?;
                Ok(())
            }
            Ok(Message::Ack { session, len }) => {
                self.set_and_check_session(session).await?;
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
                                self.force_close().await
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
                        self.force_close().await
                    }
                }
            }
            Ok(Message::Data { session, pos, data }) => {
                self.set_and_check_session(session).await?;
                self.receive_data(pos, data).await?;
                Ok(())
            }
            Ok(Message::Close { session }) => {
                self.set_and_check_session(session).await?;
                self.force_close().await
            }
        }
    }

    async fn on_socket_message(&mut self, msg: SocketMessage) -> BoxResult<()> {
        match msg {
            SocketMessage::SendData { data } => {
                self.send_buffer.extend_from_slice(&data);
                self.send_data().await
            }
            SocketMessage::Recv { result_chan, limit } => {
                let slice_len = std::cmp::min(limit, self.recv_buffer.len());
                let result = self.recv_buffer.split_to(slice_len).freeze();
                self.recv_offset += slice_len as u32;
                match result_chan.send(result) {
                    Ok(_) => Ok(()),
                    Err(_) => {
                        tracing::debug!("an lrcp socket requested some data but went away before we could send it back");
                        Ok(())
                    }
                }
            }
            SocketMessage::Close => self.force_close().await,
        }
    }

    async fn force_close(&mut self) -> BoxResult<()> {
        self.state = ConnectionState::Closed;
        let session = self.get_session()?;

        self.sock
            .send_to(&Message::Close { session }.to_bytes(), self.peer_addr)
            .await?;

        Ok(())
    }

    async fn set_and_check_session(&mut self, session: u32) -> BoxResult<()> {
        match self.session {
            Some(s) => {
                if s != session {
                    self.force_close().await?;
                    Err(format!("Invalid session for {}", self.peer_addr).into())
                } else {
                    Ok(())
                }
            }
            None => {
                self.session = Some(session);
                Ok(())
            }
        }
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
                    // match self.recv_offset.checked_add(new_data.len() as u32) {
                    //     Some(offset) => self.recv_offset = offset,
                    //     None => {
                    //         tracing::error!(
                    //             "Received more than the maximum limit of bytes: {}, closing the connection",
                    //             u32::MAX
                    //         );
                    //         return self.force_close().await;
                    //     }
                    // };
                }
                // else: we got stale data, simply resend an ack for the latest data
                self.send_ack_data().await?;
                Ok(())
            }
            std::cmp::Ordering::Greater => {
                // there are some missed data inbetween, so simply ignore completely this
                // chunk of data
                tracing::trace!(
                    "[{:?}] received data too far ahead, got {} but current offset is at {}. Ignoring packet.",
                    self.session,
                    pos,
                    self.recv_offset
                );
                Ok(())
            }
        }
    }

    async fn send_ack_data(&mut self) -> BoxResult<()> {
        let ack = Message::Ack {
            session: self.get_session()?,
            len: self.recv_offset + self.recv_buffer.len() as u32,
        };
        self.sock.send_to(&ack.to_bytes(), self.peer_addr).await?;
        Ok(())
    }

    /// for when a session is required
    fn get_session(&self) -> BoxResult<u32> {
        self.session
            .ok_or("Session is required but set to None".into())
    }

    /// send as much data as we can from the internal buffer
    async fn send_data(&mut self) -> BoxResult<()> {
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

struct LrcpSocket {
    tx: Sender<SocketMessage>,
}

struct LrcpSocketRead {
    tx: Sender<SocketMessage>,
    fut: Option<BoxFuture<'static, std::io::Result<Bytes>>>,
    // fut: Pin<Option<Box<dyn Future<Output = std::io::Result<Bytes>>>>>,
}

struct LrcpSocketWrite {
    tx: Sender<SocketMessage>,
}

impl AsyncRead for LrcpSocketRead {
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
#[derive(Debug)]
enum SocketMessage {
    SendData {
        data: Bytes,
    },
    /// request some data from the connection. At most `limit` bytes.
    Recv {
        result_chan: oneshot::Sender<Bytes>,
        limit: usize,
    },
    Close,
}

impl LrcpSocket {
    fn split(self) -> (LrcpSocketRead, LrcpSocketWrite) {
        (
            LrcpSocketRead {
                tx: self.tx.clone(),
                fut: None,
            },
            LrcpSocketWrite { tx: self.tx },
        )
    }
}

impl LrcpSocketRead {
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

impl LrcpSocketWrite {
    async fn send(&self, data: Bytes) -> BoxResult<()> {
        self.tx.send(SocketMessage::SendData { data }).await?;
        Ok(())
    }

    /// close the underlying Connection
    async fn shutdown(&self) -> BoxResult<()> {
        self.tx.send(SocketMessage::Close).await?;
        Ok(())
    }
}

struct Reversal {
    sock: LrcpSocket,
}

impl Reversal {
    fn new(sock: LrcpSocket) -> Self {
        Self { sock }
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
        let (rdr, wrt) = self.sock.split();
        let rdr = BufReader::new(rdr);
        let mut lines = rdr.split(b'\n');
        while let Some(line) = lines.next_segment().await? {
            let mut data = BytesMut::with_capacity(line.len());
            for chr in line.into_iter().rev() {
                data.put_u8(chr);
            }
            wrt.send(data.freeze()).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;
    use pretty_assertions::assert_eq;
    use std::net::ToSocketAddrs;
    use tokio::time::timeout;

    async fn get_socket() -> (SocketAddr, Arc<UdpSocket>) {
        for port in 8000..9000 {
            let addr = format!("127.0.0.1:{port}")
                .to_socket_addrs()
                .unwrap()
                .next()
                .unwrap();
            match UdpSocket::bind(addr).await {
                Ok(l) => return (addr, Arc::new(l)),
                Err(_) => continue,
            }
        }
        panic!("Couldn't find an available port !");
    }

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
    #[ignore]
    async fn test_example_session() {
        let _ = tracing_subscriber::fmt::try_init();
        let (addr, sock) = get_socket().await;
        let (conn, tx, _) = Connection::new(Arc::clone(&sock), addr);
        let _hdl = AbortHdl(conn.spawn_run());

        let session = 123;
        let msg = Message::Connect { session };
        tx.send(ServerMessage::SocketData {
            data: Bytes::copy_from_slice(msg.to_bytes().as_slice()),
            now: Instant::now(),
        })
        .await
        .unwrap();

        let mut buf = [0; 1024];
        let n = timeout(Duration::from_millis(100), sock.recv(&mut buf))
            .await
            .expect("no timeout")
            .expect("can receive data");
        let response = parser::parse_message(&buf[0..n]).expect("valid response");
        assert_eq!(Message::Ack { session, len: 0 }, response);

        let data = b"Hello, world!";
        let msg = Message::Data {
            session,
            pos: 0,
            data: Bytes::copy_from_slice(data),
        };
        tx.send(ServerMessage::SocketData {
            data: msg.to_bytes().into(),
            now: Instant::now(),
        })
        .await
        .expect("can send data");

        let n = timeout(Duration::from_millis(100), sock.recv(&mut buf))
            .await
            .expect("no timeout")
            .expect("can receive data");
        let response = parser::parse_message(&buf[0..n]).expect("valid response");
        assert_eq!(
            Message::Ack {
                session,
                len: data.len() as u32
            },
            response
        );

        let msg = Message::Close { session };
        tx.send(ServerMessage::SocketData {
            data: msg.to_bytes().into(),
            now: Instant::now(),
        })
        .await
        .expect("can send message");

        let n = timeout(Duration::from_millis(100), sock.recv(&mut buf))
            .await
            .expect("no timeout")
            .expect("can receive data");
        let response = parser::parse_message(&buf[0..n]).expect("valid response");
        assert_eq!(Message::Close { session }, response);

        assert!(tx.is_closed(), "channel should be closed now");
    }

    #[tokio::test]
    async fn test_reversal() {
        let _ = tracing_subscriber::fmt::try_init();
        let (addr, sock) = get_socket().await;
        let (conn, tx, lrcp_sock) = Connection::new(Arc::clone(&sock), addr);
        let _hdl_conn = AbortHdl(conn.spawn_run());
        let _hdl_sock = AbortHdl(Reversal::new(lrcp_sock).spawn_run());
        let _hdl_server = AbortHdl(tokio::spawn(async move {
            Server{sock}.run().await.expect("run server");
        }));

        tracing::info!("receiving socket bound to {addr}");
        let (_, test_sock) = get_socket().await;
        let session = 12345;
        let (test_conn, _, test_lrcp) =
            Connection::connect(Arc::into_inner(test_sock).unwrap(), addr, session)
                .await
                .expect("can connect");

        todo!("finish test");
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
