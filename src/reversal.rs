// allowing dead code since many methods are used only in tests
// (all the clients) and it's a pain to annotate everything only
// for tests
#![allow(dead_code)]
use std::{
    cmp,
    collections::BTreeMap,
    io::{Error as IoError, ErrorKind},
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::Poll,
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use futures::{future::BoxFuture, stream, FutureExt, StreamExt};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader, ReadBuf},
    net::UdpSocket,
    sync::mpsc::{self, Receiver, Sender},
    sync::oneshot,
    sync::Mutex as AsyncMutex,
    task::JoinHandle,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxResult<T> = Result<T, BoxError>;

const MAX_MESSAGE_LEN: usize = 1000;
const SESSION_TIMEOUT: Duration = Duration::from_secs(60);

pub async fn main(addr: SocketAddr) -> BoxResult<()> {
    let clock = Arc::new(Clock::new());
    Server::bind(addr, clock).await?.run().await?;
    Ok(())
}

// because I had some issues with multiple simultaneous clients
pub async fn test_main(addr: SocketAddr) -> BoxResult<()> {
    let clock = Arc::new(Clock::new());
    Arc::clone(&clock).tick_every(Duration::from_millis(100));
    let conn1 = Connection::connect(Arc::clone(&clock), addr).await?;
    let conn2 = Connection::connect(Arc::clone(&clock), addr).await?;
    let _client1 = conn1.start_session().await?;
    let _client2 = conn2.start_session().await?;
    Ok(())
}

/// way to have some control over time, useful for tests
struct Clock {
    micros_offset: AtomicI64,
    next_conn_id: AtomicU64,
    connections: Arc<Mutex<BTreeMap<u64, Sender<Instant>>>>,
}

impl Clock {
    fn new() -> Clock {
        Clock {
            micros_offset: AtomicI64::new(0),
            next_conn_id: AtomicU64::new(0),
            connections: Default::default(),
        }
    }

    fn now(&self) -> Instant {
        let offset = self.micros_offset.load(Ordering::SeqCst);
        let now = Instant::now();
        match offset.cmp(&0) {
            cmp::Ordering::Less => now - Duration::from_micros((-offset) as u64),
            cmp::Ordering::Equal => now,
            cmp::Ordering::Greater => now + Duration::from_micros(offset as u64),
        }
    }

    fn register_connection(&self) -> Receiver<Instant> {
        let (tx, rx) = mpsc::channel(1);
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::SeqCst);
        self.connections.lock().unwrap().insert(conn_id, tx);
        rx
    }

    /// send the current time according to this clock to all the registered connection
    async fn tick(&self) {
        // don't hold the lock over await points
        let it = self
            .connections
            .lock()
            .unwrap()
            .iter()
            .map(|(id, tx)| (*id, tx.clone()))
            .collect::<Vec<_>>();
        let now = self.now();

        let dropped_conn = stream::iter(it)
            .map(|(id, tx)| async move {
                match tx.send(now).await {
                    Ok(_) => None,
                    // the connection dropped the receiver, so remove it
                    Err(_) => Some(id),
                }
            })
            .buffer_unordered(10)
            .filter_map(|x| async move { x })
            .collect::<Vec<_>>()
            .await;

        let mut conns = self.connections.lock().unwrap();
        for id in dropped_conn {
            conns.remove(&id);
        }
    }

    // spawn a task which will notify all connections every d
    fn tick_every(self: Arc<Self>, d: Duration) {
        let this = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(d).await;
                this.tick().await;
            }
        });
    }
}

/// similar to TcpStream, to send data using lrcp over udp
#[derive(Debug)]
struct LrcpStream {
    tx: mpsc::Sender<SocketMessage>,
}

impl LrcpStream {
    /// Create a LRCP socket connected to the given address and ready to send
    /// and receive data from it
    async fn connect(&self, session: u32) -> BoxResult<()> {
        let (tx, rx) = oneshot::channel();
        if let Err(err) = self
            .tx
            .send(SocketMessage::Connect {
                session,
                connected: tx,
            })
            .await
        {
            return Err(
                format!("Cannot send a Connect message to underlying connection! {err:?}").into(),
            );
        };
        match rx.await {
            Ok(_) => Ok(()),
            Err(_) => Err("Could not connect".into()),
        }
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

/// A Server manages an UDP socket bound to a local port and accepting connections
/// It will read bytes from the socket from multiple clients, and pass them for processing
/// to Connection objects. One Connections handles one peer address, and many sessions
/// can be multiplexed onto that same connection.
struct Server {
    sock: Arc<UdpSocket>,
    clock: Arc<Clock>,
}

impl Server {
    async fn bind<A>(bind_addr: A, clock: Arc<Clock>) -> BoxResult<Server>
    where
        A: tokio::net::ToSocketAddrs,
    {
        let sock = UdpSocket::bind(bind_addr).await?;
        tracing::info!("udp listening on {}", sock.local_addr().unwrap());
        Ok(Server {
            sock: Arc::new(sock),
            clock,
        })
    }

    async fn run(&self) -> BoxResult<()> {
        let mut clients = BTreeMap::new();
        Arc::clone(&self.clock).tick_every(Duration::from_millis(100));
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
                tracing::info!("starting a new connection for {peer_addr}");
                let (tx, rx) = mpsc::channel(10);
                let clock = Arc::clone(&self.clock);
                let conn = Connection {
                    sock: Arc::clone(&self.sock),
                    clock,
                    peer_addr,
                    sessions: Default::default(),
                    server_rx: rx.into(),
                    is_client_connection: false,
                    next_session_id: 0.into(),
                    is_running: false.into(),
                };

                Arc::new(conn).spawn_run();
                tx
            });

            if let Err(_) = tx_conn.send(Bytes::copy_from_slice(&buf[0..n])).await {
                // channel closed means the other side has dropped/closed, in this case
                // the connection is closed, so drop it from the map
                clients.remove(&peer_addr);
            }
        }
    }
}

struct Connection {
    sock: Arc<UdpSocket>,
    clock: Arc<Clock>,
    peer_addr: SocketAddr,
    sessions: Mutex<BTreeMap<u32, mpsc::Sender<Message>>>,

    /// when the connection is managed by a listener, it cannot simply call
    /// sock.recv_from since this would potentially yield bytes from other
    /// connections. In this case, the connection will be managed by the listener
    /// which will poll the socket, and dispatch the received bytes through the
    /// corresponding Sender.
    /// For Connection created through `Connect`, they act as clients, and this
    /// channel will be a dummy one, they can directly call sock.recv since they're bound
    /// through sock.connect
    /// Having it as an Option is messing the tokio::select! macro unfortunately :/
    server_rx: AsyncMutex<mpsc::Receiver<Bytes>>,
    is_client_connection: bool,

    /// used when the connection is in Client mode
    next_session_id: AtomicU32,
    is_running: AtomicBool,
}

impl Connection {
    fn spawn_run(self: Arc<Self>) {
        let already_running = self
            .is_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .unwrap_or(true);
        if !already_running {
            tokio::task::spawn(async move { self.run().await });
        } else {
            tracing::warn!("Attempting to spawn_run a connection twice");
        }
    }

    /// create a client connection connected to the given peer_addr (doesn't spawn any task)
    async fn construct(clock: Arc<Clock>, peer_addr: SocketAddr) -> BoxResult<Connection> {
        let (_local_addr, sock) = get_socket().await;
        sock.connect(peer_addr).await?;
        let conn = Connection {
            sock: Arc::clone(&sock),
            clock,
            peer_addr,
            sessions: Default::default(),
            server_rx: mpsc::channel(1).1.into(),
            is_client_connection: true,
            next_session_id: 0.into(),
            is_running: false.into(),
        };
        Ok(conn)
    }

    /// create and start a connection in client mode (not managed by a server).
    /// It will create and bind a local socket and read to/from that
    async fn connect(clock: Arc<Clock>, peer_addr: SocketAddr) -> BoxResult<Arc<Connection>> {
        let conn = Connection::construct(clock, peer_addr).await?;
        let conn = Arc::new(conn);
        Arc::clone(&conn).spawn_run();
        Ok(conn)
    }

    /// returns a client stream to read and write for a single session
    /// internally, manage the underlying connection and sessions
    async fn start_session(&self) -> BoxResult<LrcpStream> {
        let session = self.next_session_id.fetch_add(1, Ordering::SeqCst);
        let (message_tx, message_rx) = mpsc::channel(1);
        let (sess_tx, sess_rx) = mpsc::channel(1);
        let lrcp_session = LrcpSession::new(
            Arc::clone(&self.clock),
            Arc::clone(&self.sock),
            self.peer_addr,
            session,
            message_rx,
            sess_rx,
        );
        lrcp_session.spawn_run();
        self.sessions.lock().unwrap().insert(session, message_tx);
        let stream = LrcpStream { tx: sess_tx };
        stream.connect(session).await?;
        Ok(stream)
    }

    async fn run(&self) -> BoxResult<()> {
        tracing::info!("[{}] starting connection", self.peer_addr);

        let mut buf = [0; MAX_MESSAGE_LEN + 1];

        loop {
            let mut server_rx = self.server_rx.lock().await;
            let bs = tokio::select! {
                len = self.sock.recv(&mut buf), if self.is_client_connection => {
                    let len = match len {
                        Ok(x) => x,
                        Err(err) => {
                            tracing::error!("Error reading socket data: {err:?}");
                            break;
                        }
                    };
                    Bytes::copy_from_slice(&buf[..len])
                },
                x = server_rx.recv(), if !self.is_client_connection => {
                    let bs = match x {
                        Some(x) => x,
                        None => {
                            tracing::warn!("Connection managed by a server but it has dropped, closing");
                            break
                        }
                    };
                    if bs.is_empty() {
                        break;
                    }
                    bs
                },
            };

            let message = match parser::parse_message(&bs) {
                Ok(msg) => msg,
                // silently ignoring invalid messages
                Err(err) => {
                    tracing::trace!("invalid message, silently ignoring: {err:?}");
                    continue;
                }
            };

            let session = message.session();

            let sess_tx = {
                let mut sessions = self.sessions.lock().unwrap();
                let tx = sessions.entry(session).or_insert_with(|| {
                    let (message_tx, message_rx) = mpsc::channel(1);
                    let (sess_tx, sess_rx) = mpsc::channel(1);
                    let lrcp_session = LrcpSession::new(
                        Arc::clone(&self.clock),
                        Arc::clone(&self.sock),
                        self.peer_addr,
                        session,
                        message_rx,
                        sess_rx,
                    );
                    Reversal {
                        sess: LrcpStream { tx: sess_tx },
                    }
                    .spawn_run();
                    lrcp_session.spawn_run();
                    message_tx
                });
                tx.clone()
            };

            match sess_tx.send(message).await {
                Ok(_) => (),
                Err(_) => {
                    self.sessions.lock().unwrap().remove(&session);
                }
            }
        }

        tracing::debug!("[{}] Terminating connection", self.peer_addr);
        Ok(())
    }
}

/// low level stream, bound to a session
/// handling timeouts, retransmission and connection state
struct LrcpSession {
    sock: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    session: u32,

    /// this channel is managed by the associated connection, and the messages
    /// received there are guaranteed to match the session for this stream
    message_rx: mpsc::Receiver<Message>,

    state: StreamState,

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

    /// injected dependency to control time (useful for testing)
    clock: Arc<Clock>,
    timer_chan: Receiver<Instant>,

    /// set when sending data, if we haven't seen an ACK for at least this length
    /// we need to resend the data
    send_data_rto: Option<(Instant, u32)>,

    /// if we haven't seen anything from the peer after this Instant, assume
    /// it went awol and terminate the connection
    session_timeout: Option<Instant>,

    /// to communicate with a high level socket
    lrcp_socket_chan: Receiver<SocketMessage>,
}

impl LrcpSession {
    fn new(
        clock: Arc<Clock>,
        sock: Arc<UdpSocket>,
        peer_addr: SocketAddr,
        session: u32,
        message_rx: Receiver<Message>,
        lrcp_socket_chan: mpsc::Receiver<SocketMessage>,
    ) -> Self {
        let timer_chan = clock.register_connection();
        LrcpSession {
            sock,
            peer_addr,
            session,
            message_rx,
            state: StreamState::Opening {
                retry_left: 3,
                notify_connected: None,
            },
            recv_buffer: BytesMut::new(),
            recv_offset: 0,
            recv_chan: None,
            send_buffer: BytesMut::new(),
            sent_offset: 0,
            clock,
            timer_chan,
            send_data_rto: None,
            session_timeout: None,
            lrcp_socket_chan,
        }
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
        tracing::info!("[{}] starting session {}", self.peer_addr, self.session);

        let mut lrcp_sock_closed = false;
        let mut has_timer = true;

        loop {
            if let StreamState::Closed = self.state {
                break;
            }

            tokio::select! {
                x = self.message_rx.recv() => {
                    let message = match x {
                        Some(x) => x,
                        None => {
                            tracing::warn!("Connection managed by a server but it has dropped, closing");
                            break
                        }
                    };
                    self.on_message(message).await?;
                },

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

                now = self.timer_chan.recv(), if has_timer => {
                    match now {
                        Some(now) => self.on_tick(now).await?,
                        None => {
                            has_timer = false;
                            tracing::info!("Connection is running without a timer");
                        }
                    }
                }
            }
        }

        tracing::debug!("[{}] Terminating connection", self.peer_addr);
        Ok(())
    }

    async fn on_message(&mut self, message: Message) -> BoxResult<()> {
        tracing::trace!("[{}] Processing message {message:?}", self.peer_addr);
        self.session_timeout = Some(self.clock.now() + SESSION_TIMEOUT);

        match message {
            Message::Connect { session } => {
                tracing::info!(
                    "[{}] New incoming connection with session {session}",
                    self.peer_addr
                );
                self.state = StreamState::Open;
                self.send_ack_data().await?;
                Ok(())
            }

            Message::Ack { session, len } => {
                if let StreamState::Opening { .. } = self.state {
                    if len == 0 {
                        let mut notify_connected =
                            match std::mem::replace(&mut self.state, StreamState::Open) {
                                StreamState::Opening {
                                    notify_connected, ..
                                } => notify_connected,
                                _ => unreachable!(),
                            };
                        if let Some(chan) = notify_connected.take() {
                            chan.send(()).map_err(|_| {
                                tracing::warn!("stream was dropped while waiting for connection");
                                let r: BoxError =
                                    "stream was dropped while waiting for connection".into();
                                r
                            })?;
                        }
                        tracing::debug!("[{}] session opened: {session}", self.peer_addr);
                        return Ok(());
                    } else {
                        self.state = StreamState::Closed;
                        return Err(
                            "Received an ack for length {len} while still waiting for Connect ACK"
                                .into(),
                        );
                    }
                };

                // clear any expectation of a pending ack if the one we just got
                // is for all the data we already sent
                if let Some((_, ack_n)) = &self.send_data_rto {
                    if &len >= ack_n {
                        self.send_data_rto = None;
                    }
                }

                let buf_len = self.send_buffer.len() as u32;
                if len < self.sent_offset {
                    // received an old ACK, pointing to some data that already
                    // has been ack'ed and discarded
                    return Ok(());
                } else if len > self.sent_offset + buf_len {
                    // peer is misbehaving, acknoledging stuff we never sent
                    self.initiate_close().await
                } else {
                    let start = len - self.sent_offset;
                    let _ = self.send_buffer.split_to(start as usize);
                    self.sent_offset = len;
                    if !self.send_buffer.is_empty() {
                        self.send_data().await
                    } else {
                        Ok(())
                    }
                }
            }
            Message::Data {
                session: _,
                pos,
                data,
            } => {
                self.receive_data(pos, data).await?;
                Ok(())
            }
            Message::Close { session } => {
                let msg = Message::Close { session };
                self.sock.send_to(&msg.to_bytes(), self.peer_addr).await?;
                self.state = StreamState::Closed;
                Ok(())
            }
        }
    }

    async fn on_tick(&mut self, now: Instant) -> BoxResult<()> {
        if let Some(session_timeout) = &self.session_timeout {
            if &now > session_timeout {
                tracing::info!("[{}] isn't responding, terminating", self.peer_addr);
                // no point sending a Close message to a peer who went away
                self.state = StreamState::Closed;
                return Ok(());
            }
        };

        if let Some((ack_timeout, ack_len)) = &self.send_data_rto {
            if &now > ack_timeout {
                match &mut self.state {
                    StreamState::Opening {
                        ref mut retry_left, ..
                    } if *retry_left > 0 => {
                        // we're a client attempting to connect, and waiting for the first
                        // ACK
                        let message = Message::Connect {
                            session: self.session,
                        };
                        *retry_left -= 1;
                        self.sock
                            .send_to(&message.to_bytes(), self.peer_addr)
                            .await?;
                    }
                    StreamState::Closed => (),
                    _ => {
                        tracing::trace!(
                            "[{}] ack timeout for len {ack_len}, resending data for session {}",
                            self.peer_addr,
                            self.session
                        );
                        self.send_data().await?
                    }
                };
            }
        }
        Ok(())
    }

    async fn send_ack_data(&mut self) -> BoxResult<()> {
        let ack = Message::Ack {
            session: self.session,
            len: self.recv_offset + self.recv_buffer.len() as u32,
        };
        tracing::trace!(
            "[{}] sending {ack:?} for session {}",
            self.peer_addr,
            self.session
        );
        self.sock.send_to(&ack.to_bytes(), self.peer_addr).await?;
        Ok(())
    }

    /// when this connection wants to close the lrcp stream for whatever reason.
    async fn initiate_close(&mut self) -> BoxResult<()> {
        match self.state {
            StreamState::Opening { .. } => {
                self.state = StreamState::Closed;
                Ok(())
            }
            StreamState::Open => {
                self.state = StreamState::Closing;
                self.sock
                    .send_to(
                        &Message::Close {
                            session: self.session,
                        }
                        .to_bytes(),
                        self.peer_addr,
                    )
                    .await?;
                Ok(())
            }
            StreamState::Closing | StreamState::Closed => Ok(()),
        }
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
        let len = cmp::min(max_data_len, self.send_buffer.len());

        tracing::trace!(
            "[{}] sending {} bytes of data for session {}, offset: {}, len: {}",
            self.peer_addr,
            len,
            self.session,
            self.sent_offset,
            self.send_buffer.len(),
        );

        let msg = Message::Data {
            session: self.session,
            pos: self.sent_offset,
            data: Bytes::copy_from_slice(&self.send_buffer[0..len]),
        };
        self.sock.send_to(&msg.to_bytes(), self.peer_addr).await?;
        let expected_ack = self.sent_offset + len as u32;
        self.send_data_rto = Some((self.clock.now() + Duration::from_secs(3), expected_ack));
        Ok(())
    }

    async fn receive_data(&mut self, pos: u32, data: Bytes) -> BoxResult<()> {
        // usize -> u32 is OK because a LRCP messages are under 1000 bytes
        // (and the parser checks for that)
        assert!(data.len() < u32::MAX as usize);
        let data_len = data.len() as u32;
        // this is also ok because `len` is a u32, which limits the size of data we can send
        // that's a bit more dubious, but :shrug:
        let buf_len = self.recv_buffer.len() as u32;

        if pos + data_len <= self.recv_offset + buf_len {
            tracing::trace!(
                "[{}] got data up to {} but already have {} + {} so resending ack for session {}",
                self.peer_addr,
                pos + data.len() as u32,
                self.recv_offset,
                data_len,
                self.session,
            );
            self.send_ack_data().await
        } else if pos > self.recv_offset + buf_len {
            tracing::trace!("got data starting from {} but only have data until {}, missed something and ignoring",
                pos,
                self.recv_offset + data_len
            );
            Ok(())
        } else {
            // don't grab data we already have
            let start = self.recv_offset + buf_len - pos;
            let new_data = &data[start as usize..];
            self.recv_buffer.extend_from_slice(new_data);
            tracing::trace!(
                "[{}] added {} bytes to recv buffer for session {}, total read: {}",
                self.peer_addr,
                new_data.len(),
                self.session,
                self.recv_offset as usize + self.recv_buffer.len()
            );
            self.send_ack_data().await?;

            // need to notify any potential reader that new data is available
            self.yield_bytes()
        }
    }

    async fn on_socket_message(&mut self, msg: SocketMessage) -> BoxResult<()> {
        tracing::trace!("[{}] processing socket message: {msg:?}", self.peer_addr);
        match msg {
            SocketMessage::Connect { session, connected } => {
                // TODO: maybe prevent calling Connect on a stream that isn't in
                // Opening state
                self.state = StreamState::Opening {
                    retry_left: 3,
                    notify_connected: Some(connected),
                };
                self.sock
                    .send_to(&Message::Connect { session }.to_bytes(), self.peer_addr)
                    .await?;
                Ok(())
            }
            SocketMessage::SendData { data } => {
                self.send_buffer.extend_from_slice(&data);
                self.send_data().await
            }
            SocketMessage::Recv { result_chan, limit } => {
                self.recv_chan = Some((result_chan, limit));
                self.yield_bytes()
            }
        }
    }

    /// process a read request to a high level stream
    fn yield_bytes(&mut self) -> BoxResult<()> {
        if self.recv_buffer.is_empty() {
            return Ok(());
        }

        let (chan, limit) = match self.recv_chan.take() {
            Some(x) => x,
            None => return Ok(()),
        };

        let slice_len = cmp::min(limit, self.recv_buffer.len());
        let result = self.recv_buffer.split_to(slice_len).freeze();
        self.recv_offset += slice_len as u32;
        match chan.send(result) {
            Ok(_) => {
                tracing::trace!(
                    "[{}] notified stream of a read of {} bytes for session {}",
                    self.peer_addr,
                    slice_len,
                    self.session,
                );
                Ok(())
            }
            Err(_) => {
                tracing::debug!(
                    "an lrcp socket requested some data but went away before we could send it back"
                );
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
enum StreamState {
    Opening {
        retry_left: u8,
        notify_connected: Option<oneshot::Sender<()>>,
    },
    Open,
    Closing,
    Closed,
}

impl StreamState {
    fn is_open(&self) -> bool {
        match self {
            StreamState::Open { .. } => true,
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
    fn session(&self) -> u32 {
        match self {
            Message::Connect { session } => *session,
            Message::Ack { session, .. } => *session,
            Message::Data { session, .. } => *session,
            Message::Close { session } => *session,
        }
    }

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
            // somehow, self.fut.take() doesn't work there -_-"
            self.fut = None;
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
                    let kind = ErrorKind::ConnectionAborted;
                    IoError::new(kind, e)
                })?;
                let resp = orx.await.map_err(|e| {
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

/// messages used to communicate between the high level LrcpSocket and
/// the low level Connection
enum SocketMessage {
    Connect {
        session: u32,
        connected: oneshot::Sender<()>,
    },
    SendData {
        data: Bytes,
    },
    /// request some data from the connection. At most `limit` bytes.
    Recv {
        result_chan: oneshot::Sender<Bytes>,
        limit: usize,
    },
}

impl std::fmt::Debug for SocketMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect { .. } => f.debug_struct("Connect").finish(),
            Self::SendData { data } => f.debug_struct("SendData").field("data", data).finish(),
            Self::Recv {
                result_chan: _,
                limit,
            } => f
                .debug_struct("Recv")
                // .field("result_chan", result_chan)
                .field("limit", limit)
                .finish(),
        }
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
}

struct Reversal {
    sess: LrcpStream,
}

impl Reversal {
    fn new(sess: LrcpStream) -> Self {
        Self { sess }
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
        let (rdr, wrt) = self.sess.split();
        let rdr = BufReader::new(rdr);
        let mut lines = rdr.split(b'\n');
        while let Some(line) = lines.next_segment().await.transpose() {
            let line = match line {
                Ok(l) => l,
                Err(err) => {
                    if err.kind() == ErrorKind::ConnectionAborted {
                        // avoid an error when the peer simply closed the connection
                        break;
                    } else {
                        return Err(err.into());
                    }
                }
            };
            tracing::trace!("reversal got a line! {:?}", std::str::from_utf8(&line));
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
    use tokio::time::timeout;

    /// make sure to abort a task on drop, so that it's easier to
    /// do assertion in tests with less worry about cleaning up
    struct AbortHdl<T>(JoinHandle<T>);
    impl<T> Drop for AbortHdl<T> {
        fn drop(&mut self) {
            self.0.abort()
        }
    }

    impl Clock {
        fn advance(&self, d: Duration) {
            self.micros_offset
                .fetch_add(d.as_micros() as i64, Ordering::SeqCst);
        }

        fn retract(&self, d: Duration) {
            self.micros_offset
                .fetch_sub(d.as_micros() as i64, Ordering::SeqCst);
        }
    }

    async fn setup_server() -> (Arc<Clock>, Server, SocketAddr) {
        let clock = Arc::new(Clock::new());
        let server = Server::bind("127.0.0.1:0", Arc::clone(&clock))
            .await
            .expect("bind server");
        let server_addr = server
            .sock
            .local_addr()
            .expect("server has a local address");
        (clock, server, server_addr)
    }

    /// handy function to iniate a connection ready for testing sending/receiving data
    async fn connect_client_socket(server_addr: SocketAddr) -> UdpSocket {
        let client_sock = UdpSocket::bind("127.0.0.1:0").await.expect("local bind");
        client_sock
            .connect(server_addr)
            .await
            .expect("connect socket");
        client_sock
            .send(b"/connect/1234/")
            .await
            .expect("send connect");

        let mut buf = [0; 1024];
        let len = timeout(Duration::from_millis(10), client_sock.recv(&mut buf))
            .await
            .expect("read no timeout after connect")
            .expect("read from socket");

        assert_eq!(Ok("/ack/1234/0/"), std::str::from_utf8(&buf[..len]));
        client_sock
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
        let (clock, server, server_addr) = setup_server().await;
        tracing::info!("server has address {server_addr}");
        let _hdl = AbortHdl(tokio::spawn(async move {
            server.run().await.expect("server ran properly");
        }));
        let client = Connection::connect(clock, server_addr)
            .await
            .expect("connection")
            .start_session()
            .await
            .expect("client connected");
        let (client_rdr, client_wrt) = client.split();

        // send the line in two messages
        client_wrt
            .send(Bytes::from_static(b"hello,"))
            .await
            .expect("send data");
        client_wrt
            .send(Bytes::from_static(b" world!\n"))
            .await
            .expect("send data");
        let resp = client_rdr.recv().await.expect("receive response");
        assert_eq!(&resp, b"!dlrow ,olleh\n".as_slice(), "message is reversed");
    }

    #[tokio::test]
    async fn test_reversal_long_line() {
        let _ = tracing_subscriber::fmt::try_init();
        let (clock, server, server_addr) = setup_server().await;
        tracing::info!("server has address {server_addr}");
        let _hdl = AbortHdl(tokio::spawn(async move {
            server.run().await.expect("server ran properly");
        }));
        let client = Connection::connect(clock, server_addr)
            .await
            .expect("connection")
            .start_session()
            .await
            .expect("client connected");
        let (client_rdr, client_wrt) = client.split();

        let line = format!("{}{}", "a".repeat(1200), "b".repeat(1200));
        let msg = format!("{}\n", line);

        client_wrt
            .send(Bytes::copy_from_slice(msg.as_bytes()))
            .await
            .expect("send data");

        let mut resp = Vec::new();
        let mut client_rdr = BufReader::new(client_rdr);
        client_rdr
            .read_until(b'\n', &mut resp)
            .await
            .expect("receive response");
        let expected = format!("{}\n", line.chars().rev().collect::<String>());
        assert_eq!(
            expected,
            std::str::from_utf8(&resp).unwrap(),
            "very long message is reversed"
        );
    }

    #[tokio::test]
    async fn test_many_reversal_same_message() {
        let _ = tracing_subscriber::fmt::try_init();
        let (clock, server, server_addr) = setup_server().await;
        tracing::info!("server has address {server_addr}");
        let _hdl = AbortHdl(tokio::spawn(async move {
            server.run().await.expect("server ran properly");
        }));
        let client = Connection::connect(clock, server_addr)
            .await
            .expect("connection")
            .start_session()
            .await
            .expect("client connected");
        let (client_rdr, client_wrt) = client.split();

        // send the line in two messages
        client_wrt
            .send(Bytes::from_static(b"hello, world!\ncoucou\n"))
            .await
            .expect("send data");
        let resp = client_rdr.recv().await.expect("receive response");
        assert_eq!(
            Ok("!dlrow ,olleh\n"),
            std::str::from_utf8(&resp),
            "message is reversed"
        );
        let resp = client_rdr.recv().await.expect("receive second response");
        assert_eq!(
            Ok("uocuoc\n"),
            std::str::from_utf8(&resp),
            "message is reversed"
        );
    }

    #[tokio::test]
    async fn test_session_timeout() {
        let _ = tracing_subscriber::fmt::try_init();
        let (clock, server, server_addr) = setup_server().await;
        tracing::info!("server has address {server_addr}");
        let _hdl = AbortHdl(tokio::spawn(async move {
            server.run().await.expect("server ran properly");
        }));
        let client = Connection::connect(Arc::clone(&clock), server_addr)
            .await
            .expect("connection")
            .start_session()
            .await
            .expect("client connected");

        clock.advance(Duration::from_secs(61));
        clock.tick().await;
        let rdr = client.split().0;

        // need to give back control to the underlying connection to process the tick
        // and drop
        tokio::task::yield_now().await;
        assert!(rdr.tx.is_closed(), "connection should be closed");
    }

    #[tokio::test]
    async fn test_ack_timeout_retransmit() {
        let _ = tracing_subscriber::fmt::try_init();
        let (clock, server, server_addr) = setup_server().await;
        let _hdl = AbortHdl(tokio::spawn(async move {
            server.run().await.expect("server ran properly");
        }));

        let mut buf = [0; 1024];
        let client_sock = connect_client_socket(server_addr).await;

        client_sock
            .send(b"/data/1234/0/coucou\n/")
            .await
            .expect("send line");

        let len = timeout(Duration::from_millis(10), client_sock.recv(&mut buf))
            .await
            .expect("read no timeout after data")
            .expect("read from socket");

        assert_eq!(Ok("/ack/1234/7/"), std::str::from_utf8(&buf[..len]));

        let len = timeout(Duration::from_millis(10), client_sock.recv(&mut buf))
            .await
            .expect("read no timeout waiting for reversed data")
            .expect("read from socket");

        // check we get the reversed line for good measure, but DON'T SEND AN ACK
        let expected_line = "/data/1234/0/uocuoc\n/";
        assert_eq!(Ok(expected_line), std::str::from_utf8(&buf[..len]));

        clock.advance(Duration::from_secs(5));
        clock.tick().await;
        tokio::task::yield_now().await;

        let len = timeout(Duration::from_millis(10), client_sock.recv(&mut buf))
            .await
            .expect("server should resend data")
            .expect("read from socket");
        assert_eq!(Ok(expected_line), std::str::from_utf8(&buf[..len]));
    }

    #[tokio::test]
    async fn test_ack_length_with_escape() {
        let _ = tracing_subscriber::fmt::try_init();
        let (_clock, server, server_addr) = setup_server().await;
        let _hdl = AbortHdl(tokio::spawn(async move {
            server.run().await.expect("server ran properly");
        }));

        let client_sock = connect_client_socket(server_addr).await;
        let mut buf = [0; 1024];

        client_sock
            .send(b"/data/1234/0/\\//")
            .await
            .expect("send line");

        let len = timeout(Duration::from_millis(10), client_sock.recv(&mut buf))
            .await
            .expect("read no timeout after data")
            .expect("read from socket");

        assert_eq!(Ok("/ack/1234/1/"), std::str::from_utf8(&buf[..len]));
    }

    #[tokio::test]
    async fn test_receive_overlapping_data() {
        let _ = tracing_subscriber::fmt::try_init();
        let (_clock, server, server_addr) = setup_server().await;
        let _hdl = AbortHdl(tokio::spawn(async move {
            server.run().await.expect("server ran properly");
        }));

        let client_sock = connect_client_socket(server_addr).await;

        let mut buf = [0; 1024];
        client_sock
            .send(b"/data/1234/0/coucou/")
            .await
            .expect("send line");

        let len = timeout(Duration::from_millis(10), client_sock.recv(&mut buf))
            .await
            .expect("read no timeout after data")
            .expect("read from socket");

        assert_eq!(Ok("/ack/1234/6/"), std::str::from_utf8(&buf[..len]));

        client_sock
            .send(b"/data/1234/0/coucou world!/")
            .await
            .expect("send longer line, from start again");

        let len = timeout(Duration::from_millis(10), client_sock.recv(&mut buf))
            .await
            .expect("read no timeout after data")
            .expect("read from socket");

        assert_eq!(Ok("/ack/1234/13/"), std::str::from_utf8(&buf[..len]));
    }

    #[tokio::test]
    async fn test_chunk_long_lines() {
        let _ = tracing_subscriber::fmt::try_init();
        let (_clock, server, server_addr) = setup_server().await;
        let _hdl = AbortHdl(tokio::spawn(async move {
            server.run().await.expect("server ran properly");
        }));

        let client_sock = connect_client_socket(server_addr).await;

        let first_half = "a".repeat(900);
        let second_half = "b".repeat(900);

        let mut buf = [0; 1024];
        client_sock
            .send(format!("/data/1234/0/{}/", first_half).as_bytes())
            .await
            .expect("send first half");

        timeout(Duration::from_millis(10), client_sock.recv(&mut buf))
            .await
            .expect("read no timeout after first half of data")
            .expect("read from socket");

        let mut buf = [0; 1024];
        client_sock
            .send(format!("/data/1234/900/{}\n/", second_half).as_bytes())
            .await
            .expect("send second half");

        timeout(Duration::from_millis(10), client_sock.recv(&mut buf))
            .await
            .expect("read no timeout after first half of data")
            .expect("read from socket");

        let len1 = timeout(Duration::from_millis(10), client_sock.recv(&mut buf))
            .await
            .expect("read no timeout after first read")
            .expect("read from socket");

        assert_eq!(Ok("/data/1234/0/b"), std::str::from_utf8(&buf[..14]));
        client_sock
            .send(format!("/ack/1234/{len1}/").as_bytes())
            .await
            .expect("ok sending ack");

        let _len2 = timeout(Duration::from_millis(10), client_sock.recv(&mut buf))
            .await
            .expect("read no timeout after second read")
            .expect("read from socket");
        assert_eq!(
            format!("/data/1234/{len1}/aaa"),
            std::str::from_utf8(&buf[..18]).unwrap()
        );
    }

    // #[tokio::test]
    // async fn test_multiplexed_streams_same_socket() {
    //     let _ = tracing_subscriber::fmt::try_init();
    //     let session1 = 567;
    //     let session2 = 890;
    //     let (clock, server, server_addr) = setup_server().await;
    //     let _hdl = AbortHdl(tokio::spawn(async move {
    //         server.run().await.expect("server ran properly");
    //     }));
    //     let (_stream, mut conn) = LrcpStream::construct(Arc::clone(&clock), server_addr).await;
    //     todo!()
    // }
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

        fn append(_input: I, _kind: ErrorKind, other: Self) -> Self {
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
