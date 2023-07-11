use std::{
    collections::{BTreeMap, BTreeSet},
    io::ErrorKind,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use time::{ext::NumericalDuration, Date, OffsetDateTime};
use tokio::{
    io::AsyncRead,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxResult<T> = Result<T, BoxError>;

pub async fn main(addr: SocketAddr) -> BoxResult<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("listening on {addr}");

    let server = Server::new();
    server.run(listener).await?;

    Ok(())
}

struct Server {
    roads: BTreeMap<u16, u16>,
    // keep track of tickets sent to (plate, date)
    sent_tickets: BTreeMap<String, BTreeSet<(u16, Date)>>,
    // tickets we should send but no available dispatcher for them yet
    waiting_tickets: Vec<Ticket>,
    dispatchers: Vec<(Vec<u16>, Arc<MessageWriter>)>,
    /// plate spotted at, indexed by the (road ID, plate)
    plates: BTreeMap<(u16, String), Vec<PlateSight>>,
    server_receiver: Receiver<ServerMessage>,
    // to be cloned and given to new clients to give them a way
    // to communicate with the central server.
    server_sender: Sender<ServerMessage>,
}

#[derive(Debug)]
enum ServerMessage {
    RegisterCamera {
        road: u16,
        limit: u16,
    },
    RegisterDispatcher {
        roads: Vec<u16>,
        wrt: Arc<MessageWriter>,
    },
    PlateSight(PlateSight),
}

impl Server {
    fn new() -> Self {
        let (sender_chan, registration_chan) = mpsc::channel(1);
        Self {
            waiting_tickets: Vec::new(),
            roads: Default::default(),
            dispatchers: Default::default(),
            plates: Default::default(),
            server_receiver: registration_chan,
            server_sender: sender_chan,
            sent_tickets: Default::default(),
        }
    }

    async fn run(mut self, listener: TcpListener) -> BoxResult<()> {
        tokio::try_join!(
            Self::listen(listener, self.server_sender.clone()),
            self.receive_messages()
        )?;
        Ok(())
    }

    // avoid self here so that the method handling messages can be exclusive with &mut self
    async fn listen(listener: TcpListener, server_sender: Sender<ServerMessage>) -> BoxResult<()> {
        loop {
            let (stream, socket_addr) = listener.accept().await?;
            tokio::spawn({
                let server_chan = server_sender.clone();
                async move {
                    tracing::info!("incoming connection from {}", socket_addr);
                    let client = UnknownClient {
                        stream,
                        server_chan,
                    };
                    client.spawn_run();
                }
            });
        }
    }

    async fn receive_messages(&mut self) -> BoxResult<()> {
        while let Some(msg) = self.server_receiver.recv().await {
            tracing::debug!("server got message {msg:?}");
            match msg {
                ServerMessage::RegisterCamera { road, limit } => {
                    self.roads.insert(road, limit);
                }
                ServerMessage::RegisterDispatcher { roads, wrt } => {
                    self.dispatchers.push((roads, wrt));
                    self.send_tickets().await?;
                }
                ServerMessage::PlateSight(ps) => {
                    let entry = self.plates.entry((ps.road, ps.name.clone())).or_default();
                    entry.push(ps);
                    entry.sort_unstable_by_key(|s| s.timestamp);
                    self.send_tickets().await?;
                }
            }
        }

        // `self` also own the Sender, and isn't going to drop it as long as
        // it's in scope, so in the loop above.
        unreachable!("how did the sender got dropped???");
    }

    async fn send_tickets(&mut self) -> BoxResult<()> {
        let tickets = self.get_tickets();

        let mut holdover_tickets = Vec::new();

        for ticket in tickets {
            let dispatcher = self.dispatchers.iter().find_map(|(roads, wrt)| {
                if roads.contains(&ticket.road) {
                    Some(wrt)
                } else {
                    None
                }
            });

            // TODO: remove plates at some point

            let plate = ticket.plate.clone();
            match dispatcher {
                Some(wrt) => {
                    if self.should_send_ticket(&ticket) {
                        let mut d = ticket.timestamp1.date();
                        let e = self.sent_tickets.entry(plate.clone()).or_default();
                        while d <= ticket.timestamp2.date() {
                            e.insert((ticket.road, d));
                            d += 1.days();
                        }
                        tracing::info!("sending {ticket:?}");
                        wrt.send(Message::Ticket(ticket)).await?;
                    }
                }
                None => {
                    tracing::info!("No dispatcher available for {ticket:?}");
                    holdover_tickets.push(ticket);
                }
            }
        }
        self.waiting_tickets = holdover_tickets;

        Ok(())
    }

    /// compute the tickets to be sent based on the current plate sightings
    fn get_tickets(&self) -> Vec<Ticket> {
        let mut tickets = self.waiting_tickets.clone();

        for ((road, plate), plate_sights) in self.plates.iter() {
            let limit = *self.roads.get(road).unwrap() as i64;
            for (s1, s2) in std::iter::zip(plate_sights, plate_sights.iter().skip(1)) {
                let d = if s2.mile > s1.mile {
                    s2.mile - s1.mile
                } else {
                    s1.mile - s2.mile
                };
                let t = s2.timestamp - s1.timestamp;
                let speed = (d as i64 * 3600 * 100) / t.whole_seconds();
                if speed > limit * 100 {
                    let ticket = Ticket {
                        plate: plate.clone(),
                        road: *road,
                        mile1: s1.mile,
                        timestamp1: s1.timestamp.clone(),
                        mile2: s2.mile,
                        timestamp2: s2.timestamp.clone(),
                        speed: u16::try_from(speed)
                            .expect(&format!("No overflow on speed {speed}")),
                    };
                    tickets.push(ticket);
                }
            }
        }
        tickets
    }

    fn should_send_ticket(&self, ticket: &Ticket) -> bool {
        let mut d = ticket.timestamp1.date();
        while d <= ticket.timestamp2.date() {
            let already_sent = self
                .sent_tickets
                .get(&ticket.plate)
                .map(|roads_and_dates| roads_and_dates.contains(&(ticket.road, d)))
                .unwrap_or(false);
            if already_sent {
                return false;
            }
            d += 1.days();
        }
        true
    }
}

struct UnknownClient {
    stream: TcpStream,
    server_chan: Sender<ServerMessage>,
}

impl UnknownClient {
    fn spawn_run(self) {
        let addr = self.stream.peer_addr().expect("can get peer addr");
        tokio::spawn(async move {
            match self.run().await {
                Ok(()) => (),
                Err(err) => tracing::info!("{addr} crashed: {err:?}"),
            }
        });
    }

    async fn run(self) -> BoxResult<()> {
        let peer_addr = self.stream.peer_addr().expect("get peer address");
        let (rdr_stream, wrt_stream) = self.stream.into_split();
        // let rdr_stream =
        //     tokio_util::io::InspectReader::new(rdr_stream, |x| tracing::debug!("read {x:x?}"));
        let mut reader = MessageReader::new(rdr_stream);
        let wrt = Arc::new(MessageWriter::new(wrt_stream));
        let mut heartbeat_handle = None;

        while let Some(message) = reader.next_message().await.transpose() {
            match message {
                Ok(Message::IAmCamera { road, mile, limit }) => {
                    self.server_chan
                        .send(ServerMessage::RegisterCamera { road, limit })
                        .await?;
                    let camera = Camera {
                        road,
                        mile,
                        limit,
                        peer_addr,
                        rdr: reader,
                        wrt,
                        send_plate_chan: self.server_chan,
                        heartbeat_handle,
                    };
                    camera.spawn_run();
                    return Ok(());
                }
                Ok(Message::IAmDispatcher { roads }) => {
                    let id: String = itertools::intersperse(
                        roads.iter().map(|x| format!("{x}")),
                        ",".to_string(),
                    )
                    .collect();
                    self.server_chan
                        .send(ServerMessage::RegisterDispatcher {
                            roads,
                            wrt: Arc::clone(&wrt),
                        })
                        .await?;
                    let dispatcher = Dispatcher {
                        id: format!("{id} at {peer_addr}"),
                        rdr_stream: reader,
                        wrt,
                        heartbeat_handle,
                    };
                    dispatcher.spawn_run();
                    return Ok(());
                }
                Ok(Message::WantHeartbeat { interval }) => {
                    handle_heartbeat(&mut heartbeat_handle, &wrt, interval).await?;
                }
                _ => {
                    let msg = "The first client message should be IAmCamera or IAmDispatcher";
                    tracing::error!("{msg} but got {message:?}");
                    let error = Message::Error {
                        msg: msg.to_string(),
                    };
                    wrt.send(error).await?;
                    break;
                }
            }
        }

        if let Some(hdl) = heartbeat_handle.take() {
            hdl.abort();
        }
        tracing::info!("Done with {peer_addr}");
        Ok(())
    }
}

struct Camera {
    road: u16,
    mile: u16,
    limit: u16,
    peer_addr: SocketAddr,
    rdr: MessageReader<OwnedReadHalf>,
    wrt: Arc<MessageWriter>,
    send_plate_chan: Sender<ServerMessage>,
    heartbeat_handle: Option<JoinHandle<()>>,
}

impl Camera {
    fn spawn_run(mut self) {
        let id = format!(
            "{}-{}-{} at {}",
            self.road, self.mile, self.limit, self.peer_addr
        );
        tracing::info!("starting camera {id}");
        tokio::spawn(async move {
            match self.run().await {
                Ok(_) => tracing::info!("Camera {id} disconnected"),
                Err(err) => tracing::error!("Camera {id} crashed with error: {err:?}",),
            };
        });
    }

    async fn run(&mut self) -> BoxResult<()> {
        while let Some(parsed) = self.rdr.next_message().await.transpose() {
            match parsed {
                Ok(Message::Plate { name, timestamp }) => {
                    let ps = PlateSight {
                        road: self.road,
                        mile: self.mile,
                        limit: self.limit,
                        name,
                        timestamp,
                    };
                    self.send_plate_chan
                        .send(ServerMessage::PlateSight(ps))
                        .await?;
                }
                Ok(Message::WantHeartbeat { interval }) => {
                    handle_heartbeat(&mut self.heartbeat_handle, &self.wrt, interval).await?;
                }
                _ => {
                    tracing::error!(
                        "Camera {}-{}-{} got invalid message {parsed:?}",
                        self.road,
                        self.mile,
                        self.limit
                    );
                    let err = Message::Error {
                        msg: "Illegal message".to_string(),
                    };
                    self.wrt.send(err).await?;
                    if let Some(hdl) = self.heartbeat_handle.take() {
                        hdl.abort();
                    }
                    break;
                }
            }
        }
        Ok(())
    }
}

struct Dispatcher {
    id: String,
    rdr_stream: MessageReader<OwnedReadHalf>,
    wrt: Arc<MessageWriter>,
    heartbeat_handle: Option<JoinHandle<()>>,
}

impl Dispatcher {
    fn spawn_run(mut self) {
        tracing::info!("starting dispatcher {}", self.id);
        tokio::spawn(async move {
            match self.run().await {
                Ok(_) => tracing::info!("Dispatcher exited. {}", self.id),
                Err(err) => tracing::error!("Dispatcher crashed. {} - {err:?}", self.id),
            }
        });
    }

    async fn run(&mut self) -> BoxResult<()> {
        while let Some(msg) = self.rdr_stream.next_message().await.transpose() {
            match msg {
                Ok(Message::WantHeartbeat { interval }) => {
                    handle_heartbeat(&mut self.heartbeat_handle, &self.wrt, interval).await?;
                }
                _ => {
                    tracing::error!("Dispatcher {} got invalid message {msg:?}", self.id);
                    let err = Message::Error {
                        msg: "Illegal message".to_string(),
                    };
                    self.wrt.send(err).await?;
                    if let Some(hdl) = self.heartbeat_handle.take() {
                        hdl.abort();
                    }
                    break;
                }
            }
        }

        Ok(())
    }
}

struct MessageWriter {
    chan: Sender<Message>,
}

impl std::fmt::Debug for MessageWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageWriter")
            .field("chan", &"Sender<Message>")
            .finish()
    }
}

impl MessageWriter {
    fn new(mut wrt: OwnedWriteHalf) -> Self {
        let (sender, mut chan): (Sender<Message>, _) = mpsc::channel(1);
        let addr = wrt
            .peer_addr()
            .expect("get peer address of tcp OwnedWriteHalf");
        tokio::spawn(async move {
            while let Some(msg) = chan.recv().await {
                match wrt.write_all(&msg.to_bytes()).await {
                    Ok(_) => (),
                    Err(err) => {
                        // broken pipe means that we're trying to write to an already closed
                        // stream, so we just ignore that.
                        if err.kind() != ErrorKind::BrokenPipe {
                            tracing::error!("Cannot send message to {addr} {err:?}");
                        }
                        break;
                    }
                }
            }
            tracing::info!("shutting down writing stream for {addr}");
            // ignore the error, because the stream is probably already closed
            let _ = wrt.shutdown().await;
        });
        MessageWriter { chan: sender }
    }

    async fn send(&self, message: Message) -> BoxResult<()> {
        self.chan.send(message).await?;
        Ok(())
    }
}

// // custom error to add a variant for aborting control flow elsewhere
// #[derive(Debug, thiserror::Error)]
// enum AbortErr {
//     #[error("Aborting because {0}")]
//     Abort(String),
//     #[error(transparent)]
//     Other { source: BoxError },
// }

/// handle a heartbeat messages. Spawn a task to send the messages when it's required.
/// If a second heartbeat message is seen, it will send an error to the client and
/// return an Abort and stop the underlying task.
/// It is the caller responsability to shutdown the underlying stream.
async fn handle_heartbeat(
    handle: &mut Option<JoinHandle<()>>,
    wrt: &Arc<MessageWriter>,
    interval: u32,
) -> BoxResult<()> {
    match handle {
        Some(hdl) => {
            hdl.abort();
            Err(format!("Received a second WantHeartbeat message").into())
        }
        None => {
            if interval == 0 {
                handle.replace(tokio::spawn(async {}));
            } else {
                handle.replace(tokio::spawn({
                    let wrt = Arc::clone(&wrt);
                    async move {
                        loop {
                            // put the sleep first. The countdown starts at instantiation, and
                            // we want the timer to be consistent, even if sending stuff to the
                            // client takes some time
                            let sleep =
                                tokio::time::sleep(Duration::from_millis(interval as u64 * 100));
                            if let Err(err) = wrt.send(Message::Heartbeat).await {
                                if !wrt.chan.is_closed() {
                                    tracing::warn!("Cannot send heartbeat: {err:?}");
                                }
                                break ();
                            }

                            sleep.await;
                        }
                    }
                }));
            };
            Ok(())
        }
    }
}

struct MessageReader<R: AsyncRead> {
    reader: R,
    buf: BytesMut,
}

impl<R> MessageReader<R>
where
    R: AsyncRead + Unpin,
{
    fn new(reader: R) -> Self {
        let buf = BytesMut::with_capacity(8 * 1024);
        Self { reader, buf }
    }

    #[cfg(test)]
    fn into_inner(self) -> R {
        self.reader
    }

    async fn next_message(&mut self) -> BoxResult<Option<Message>> {
        loop {
            match parser::message(&self.buf) {
                Ok((rest, command)) => {
                    let consumed = self.buf.len() - rest.len();
                    let _ = self.buf.split_to(consumed);
                    break Ok(Some(command));
                }
                Err(nom::Err::Error(err)) | Err(nom::Err::Failure(err)) => {
                    tracing::trace!("parsing error {err:?}");
                    break Err(err.to_owned().into());
                }
                Err(nom::Err::Incomplete(_)) => {
                    let mut tmp_buf = [0; 1024];
                    let n = self.reader.read(&mut tmp_buf).await?;
                    if n == 0 {
                        // reaching EOF and buffer is empty, meaning we're all done
                        if self.buf.len() == 0 {
                            break Ok(None);
                        }

                        // needs more bytes but the reader has reached its end, abort
                        tracing::trace!("unexpected EOF");
                        break Err(parser::InvalidCommand::Nom(
                            Bytes::new(),
                            nom::error::ErrorKind::Eof,
                        )
                        .into());
                    } else {
                        self.buf.extend_from_slice(&tmp_buf[0..n]);
                        continue;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};
    use std::io::Cursor;
    use time::macros::datetime;
    use tokio::{net::TcpListener, time::timeout};

    use super::*;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn test_parse_two_messages() {
        let _ = tracing_subscriber::fmt::try_init();
        // camera
        let mut data = vec![0x80, 0x00, 0x42, 0x00, 0x64, 0x00, 0x3C];
        // want heartbeat
        data.extend_from_slice(&[0x40, 0x00, 0x00, 0x00, 0x0a]);

        let mut reader = MessageReader::new(Cursor::new(data));
        assert_eq!(
            reader.next_message().await.expect("correct parse"),
            Some(Message::IAmCamera {
                road: 66,
                mile: 100,
                limit: 60,
            })
        );
        assert_eq!(
            reader.next_message().await.expect("correct parse"),
            Some(Message::WantHeartbeat { interval: 10 })
        );
        assert_eq!(
            reader.next_message().await.expect("correct parse"),
            None,
            "no more messages"
        );
    }

    /// find a random available port and returns the tcp listener associated to it
    async fn get_listener() -> TcpListener {
        for port in 8000..9000 {
            match TcpListener::bind(format!("127.0.0.1:{port}")).await {
                Ok(l) => return l,
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

    #[tokio::test]
    async fn test_single_car() -> Result<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let listener = get_listener().await;
        let addr = listener.local_addr().unwrap();
        let _hdl = AbortHdl(tokio::spawn(Server::new().run(listener)));
        let road = 123;
        let plate = "plate1".to_string();
        let limit = 1;

        let mut camera1 = TcpStream::connect(addr)
            .await
            .context("camera1 can connect")?;
        camera1
            .write_all(
                &Message::IAmCamera {
                    road,
                    mile: 10,
                    limit,
                }
                .to_bytes(),
            )
            .await?;

        let mut dispatcher = TcpStream::connect(addr)
            .await
            .context("dispatcher can connect")?;
        dispatcher
            .write_all(&Message::IAmDispatcher { roads: vec![road] }.to_bytes())
            .await?;
        dispatcher.flush().await?;

        let mut dispatcher = MessageReader::new(dispatcher);

        camera1
            .write_all(
                &Message::Plate {
                    name: plate.clone(),
                    timestamp: datetime!(2023-01-01 3:00 UTC),
                }
                .to_bytes(),
            )
            .await?;
        camera1.flush().await?;

        let mut camera2 = TcpStream::connect(addr)
            .await
            .context("camera2 can connect")?;

        camera2
            .write_all(
                &Message::IAmCamera {
                    road,
                    mile: 20,
                    limit,
                }
                .to_bytes(),
            )
            .await?;

        camera2
            .write_all(
                &Message::Plate {
                    name: plate.clone(),
                    // 10 miles in 1 minute = 600 m/h > limit = 100
                    timestamp: datetime!(2023-01-01 3:01 UTC),
                }
                .to_bytes(),
            )
            .await?;
        camera2.flush().await?;

        let duration = Duration::from_millis(100);
        let msg = match timeout(duration, dispatcher.next_message()).await {
            Err(_) => panic!("expected a ticket but got nothing in {duration:?}"),
            Ok(Ok(msg)) => msg,
            Ok(x) => panic!("expecting a message but got {x:?}!"),
        };

        let ticket = Ticket {
            plate: plate.clone(),
            road,
            mile1: 10,
            timestamp1: datetime!(2023-01-01 3:00 UTC),
            mile2: 20,
            timestamp2: datetime!(2023-01-01 3:01 UTC),
            speed: 60000,
        };
        assert_eq!(msg, Some(Message::Ticket(ticket)));

        tracing::info!("tearing down test");
        dispatcher.into_inner().shutdown().await?;
        camera2.shutdown().await?;
        camera1.shutdown().await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Ticket {
    pub plate: String,
    pub road: u16,
    pub mile1: u16,
    pub timestamp1: OffsetDateTime,
    pub mile2: u16,
    pub timestamp2: OffsetDateTime,
    pub speed: u16,
}

impl Ticket {
    fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.push(0x21);
        write_str(&mut result, &self.plate);
        result.extend_from_slice(&self.road.to_be_bytes());
        result.extend_from_slice(&self.mile1.to_be_bytes());
        let ts1 = self.timestamp1.unix_timestamp() as u32;
        result.extend_from_slice(&ts1.to_be_bytes());
        result.extend_from_slice(&self.mile2.to_be_bytes());
        let ts2 = self.timestamp2.unix_timestamp() as u32;
        result.extend_from_slice(&ts2.to_be_bytes());
        result.extend_from_slice(&self.speed.to_be_bytes());
        result
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PlateSight {
    road: u16,
    mile: u16,
    limit: u16,
    name: String,
    timestamp: OffsetDateTime,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Message {
    Error {
        msg: String,
    },
    Plate {
        name: String,
        timestamp: OffsetDateTime,
    },
    Ticket(Ticket),
    WantHeartbeat {
        interval: u32,
    },
    Heartbeat,
    IAmCamera {
        road: u16,
        mile: u16,
        limit: u16,
    },
    IAmDispatcher {
        roads: Vec<u16>,
    },
}

impl Message {
    fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();
        match self {
            Message::Error { msg } => {
                result.push(0x10);
                write_str(&mut result, msg);
            }
            Message::Plate { name, timestamp } => {
                result.push(0x20);
                write_str(&mut result, name);
                result.extend_from_slice(&(timestamp.unix_timestamp() as u32).to_be_bytes())
            }
            Message::Ticket(ticket) => {
                result = ticket.to_bytes();
            }
            Message::WantHeartbeat { interval } => {
                result.push(0x40);
                result.extend_from_slice(&interval.to_be_bytes());
            }
            Message::Heartbeat => result.push(0x41),
            Message::IAmCamera { road, mile, limit } => {
                result.push(0x80);
                result.extend_from_slice(&road.to_be_bytes());
                result.extend_from_slice(&mile.to_be_bytes());
                result.extend_from_slice(&limit.to_be_bytes());
            }
            Message::IAmDispatcher { roads } => {
                result.push(0x81);
                result.push(roads.len() as u8);
                result.reserve(roads.len() * 2);
                for road in roads {
                    result.extend_from_slice(&road.to_be_bytes());
                }
            }
        }
        result
    }
}

fn write_str(v: &mut Vec<u8>, s: &str) {
    v.push(s.len() as u8);
    v.extend_from_slice(s.as_bytes());
}

mod parser {
    use std::str::Utf8Error;

    use bytes::Bytes;
    use nom::{
        branch::alt,
        combinator::map,
        error::{ErrorKind, ParseError},
        multi::{length_count, length_data},
        number::{
            self,
            streaming::{be_u16, be_u32},
        },
        sequence::{preceded, tuple},
        IResult,
    };
    use time::OffsetDateTime;

    use super::{Message, Ticket};

    type ParseResult<'input, T> = IResult<&'input [u8], T, InvalidCommand<&'input [u8]>>;

    #[derive(Debug, PartialEq, thiserror::Error)]
    pub enum InvalidCommand<I> {
        #[error("invalid utf8 string {err:?} for slice {slice:?}")]
        InvalidMessage { slice: Bytes, err: Utf8Error },
        #[error("parse error")]
        Nom(I, ErrorKind),
        #[error("invalid timestamp")]
        InvalidTimestamp(#[from] time::error::ComponentRange),
        #[error("Unknown command code: {code}")]
        Unknown { code: u8 },
    }

    impl<'a, I> ParseError<I> for InvalidCommand<I> {
        fn from_error_kind(input: I, kind: ErrorKind) -> Self {
            InvalidCommand::Nom(input, kind)
        }

        fn append(_input: I, _kind: ErrorKind, other: Self) -> Self {
            // TODO that seems a bit dubious to discard some things
            other
        }
    }

    impl<'a> InvalidCommand<&'a [u8]> {
        pub fn to_owned(self) -> InvalidCommand<Bytes> {
            self.into()
        }
    }

    // bit meh. The conversion here is to remove all lifetime parameters so that this
    // can be used as the return value in the impl Stream
    impl<'a> From<InvalidCommand<&'a [u8]>> for InvalidCommand<Bytes> {
        fn from(value: InvalidCommand<&'a [u8]>) -> Self {
            match value {
                InvalidCommand::Nom(i, k) => InvalidCommand::Nom(Bytes::copy_from_slice(i), k),
                InvalidCommand::InvalidMessage { slice, err } => {
                    InvalidCommand::InvalidMessage { slice, err }
                }
                InvalidCommand::InvalidTimestamp(err) => InvalidCommand::InvalidTimestamp(err),
                InvalidCommand::Unknown { code } => InvalidCommand::Unknown { code },
            }
        }
    }

    pub fn message(input: &[u8]) -> ParseResult<Message> {
        use nom::bytes::streaming::tag;
        alt((
            preceded(tag([0x10]), command_error),
            preceded(tag([0x20]), command_plate),
            preceded(tag([0x21]), command_ticket),
            preceded(tag([0x40]), command_want_heartbeat),
            map(tag([0x41]), |_| Message::Heartbeat),
            preceded(tag([0x80]), command_i_am_camera),
            preceded(tag([0x81]), command_i_am_dispatcher),
            command_unknown,
        ))(input)
    }

    fn command_error(input: &[u8]) -> ParseResult<Message> {
        map(binstr, |msg| Message::Error { msg })(input)
    }

    fn command_plate(input: &[u8]) -> ParseResult<Message> {
        map(tuple((binstr, ts)), |(name, timestamp)| Message::Plate {
            name,
            timestamp,
        })(input)
    }

    fn command_ticket(input: &[u8]) -> ParseResult<Message> {
        map(
            tuple((binstr, be_u16, be_u16, ts, be_u16, ts, be_u16)),
            |(plate, road, mile1, timestamp1, mile2, timestamp2, speed)| {
                Message::Ticket(Ticket {
                    plate,
                    road,
                    mile1,
                    timestamp1,
                    mile2,
                    timestamp2,
                    speed,
                })
            },
        )(input)
    }

    fn command_want_heartbeat(input: &[u8]) -> ParseResult<Message> {
        map(be_u32, |interval| Message::WantHeartbeat { interval })(input)
    }

    fn command_i_am_camera(input: &[u8]) -> ParseResult<Message> {
        map(tuple((be_u16, be_u16, be_u16)), |(road, mile, limit)| {
            Message::IAmCamera { road, mile, limit }
        })(input)
    }

    fn command_i_am_dispatcher(input: &[u8]) -> ParseResult<Message> {
        map(length_count(number::streaming::u8, be_u16), |roads| {
            Message::IAmDispatcher { roads }
        })(input)
    }

    fn command_unknown(input: &[u8]) -> ParseResult<Message> {
        let (_rest, code) = number::streaming::u8(input)?;
        Err(nom::Err::Error(InvalidCommand::Unknown { code }))
    }

    fn ts(input: &[u8]) -> IResult<&[u8], OffsetDateTime, InvalidCommand<&[u8]>> {
        let (rest, raw) = be_u32(input)?;
        let ts = OffsetDateTime::from_unix_timestamp(raw as i64)
            .map_err(|e| nom::Err::Error(InvalidCommand::InvalidTimestamp(e)))?;
        Ok((rest, ts))
    }

    fn binstr(input: &[u8]) -> IResult<&[u8], String, InvalidCommand<&[u8]>> {
        let (rest, slice) = length_data(number::streaming::u8)(input)?;
        let s = std::str::from_utf8(slice).map_err(|err| {
            nom::Err::Error(InvalidCommand::InvalidMessage {
                slice: Bytes::copy_from_slice(slice),
                err,
            })
        })?;

        Ok((rest, s.to_string()))
    }

    #[cfg(test)]
    mod test_parser {
        use super::*;
        use hex_literal::hex;
        use nom::Finish;
        use nom::Needed;
        use nonzero_ext::nonzero;
        use pretty_assertions::assert_eq;
        use time::OffsetDateTime;

        pub fn parse_command(data: &[u8]) -> Result<Message, InvalidCommand<&[u8]>> {
            let (_rest, command) = message(data).finish()?;
            Ok(command)
        }

        #[test]
        fn test_error() {
            let msg = "bad".to_string();
            let mut data = vec![0x10, msg.len() as u8];
            data.extend_from_slice(msg.as_bytes());
            assert_eq!(Ok(Message::Error { msg }), parse_command(&data));
        }

        #[test]
        fn test_error_incomplete() {
            let msg = "bad";
            let mut data = vec![0x10, msg.len() as u8];
            data.extend_from_slice(msg.as_bytes());
            let result = message(&data[0..1]);
            assert_eq!(
                Err(nom::Err::Incomplete(Needed::Size(nonzero!(1_usize)))),
                result
            )
        }

        #[test]
        fn test_plate() {
            let data = vec![0x20, 0x04, 0x55, 0x4e, 0x31, 0x58, 0x00, 0x00, 0x03, 0xe8];
            assert_eq!(
                Ok(Message::Plate {
                    name: "UN1X".to_string(),
                    timestamp: OffsetDateTime::from_unix_timestamp(1000).unwrap()
                }),
                parse_command(&data)
            )
        }

        #[test]
        fn test_ticket() {
            let data = hex!("21 04 55 4E 31 58 00 42 00 64 00 01 E2 40 00 6E 00 01 E3 A8 27 10");
            let ticket = Ticket {
                plate: "UN1X".to_string(),
                road: 66,
                mile1: 100,
                timestamp1: OffsetDateTime::from_unix_timestamp(123456).unwrap(),
                mile2: 110,
                timestamp2: OffsetDateTime::from_unix_timestamp(123816).unwrap(),
                speed: 10000,
            };
            assert_eq!(Ok(Message::Ticket(ticket)), parse_command(&data))
        }

        #[test]
        fn test_want_heartbeat() {
            let data = vec![0x40, 0x00, 0x00, 0x00, 0x0a];
            assert_eq!(
                Ok(Message::WantHeartbeat { interval: 10 }),
                parse_command(&data)
            )
        }

        #[test]
        fn test_heartbeat() {
            assert_eq!(Ok(Message::Heartbeat), parse_command(&[0x41]))
        }

        #[test]
        fn test_i_am_camera() {
            let data = vec![0x80, 0x00, 0x42, 0x00, 0x64, 0x00, 0x3C];
            assert_eq!(
                Ok(Message::IAmCamera {
                    road: 66,
                    mile: 100,
                    limit: 60
                }),
                parse_command(&data)
            )
        }

        #[test]
        fn test_i_am_dispatcher() {
            let data = vec![0x81, 0x03, 0x00, 0x42, 0x01, 0x70, 0x13, 0x88];
            assert_eq!(
                Ok(Message::IAmDispatcher {
                    roads: vec![66, 368, 5000]
                }),
                parse_command(&data)
            )
        }
    }
}
