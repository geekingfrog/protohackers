use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Instant};

use bytes::{Bytes, BytesMut};
use std::fmt::Write;
use tokio::{
    net::UdpSocket,
    sync::mpsc::{self, Receiver, Sender},
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
                let (conn, tx) = Connection::new(Arc::clone(&self.sock), peer_addr);
                conn.spawn_run();
                tx
            });

            if let Err(_) = tx_conn
                .send(ServerMessage::SocketData {
                    data: Bytes::copy_from_slice(&buf[0..n]),
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
    SocketData { data: Bytes },
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
    received_data: (String, u32),

    /// the data to send, along with an offset.
    /// Once the data we sent has been acknowledge, we can drop
    /// it to free the memory, but since the ACK are based on the
    /// position from the start of the stream, we need to keep in memory
    /// how many char we already sent and got acked
    sent_data: (String, u32),
    /// last ack seen for the data we sent
    hi_ack_sent: u32,
    peer_addr: SocketAddr,
}

#[derive(Debug, PartialEq)]
enum ConnectionState {
    Open,
    Closing,
    Closed,
}

impl ConnectionState {
    fn can_send(&self) -> bool {
        matches!(self, ConnectionState::Open)
    }
}

impl Connection {
    fn new(sock: Arc<UdpSocket>, peer_addr: SocketAddr) -> (Self, Sender<ServerMessage>) {
        let (tx, msg_chan) = mpsc::channel(1);
        let conn = Connection {
            sock,
            msg_chan,
            state: ConnectionState::Open,
            session: None,
            received_data: (String::new(), 0),
            sent_data: (String::new(), 0),
            hi_ack_sent: 0,
            peer_addr,
        };
        (conn, tx)
    }

    // start the inner loop for the connection in its own async task
    fn spawn_run(self) {
        let addr = self.peer_addr.clone();
        tokio::spawn(async move {
            match self.run().await {
                Ok(_) => (),
                Err(err) => tracing::error!("error while running connection for {addr} {err:?}"),
            }
        });
    }

    async fn run(mut self) -> BoxResult<()> {
        while let Some(server_msg) = self.msg_chan.recv().await {
            match server_msg {
                ServerMessage::SocketData { data } => self.on_message(data).await?,
            };
        }
        todo!()
    }

    async fn on_message(&mut self, raw: Bytes) -> BoxResult<()> {
        let message = std::str::from_utf8(&raw)
            .map_err(|e| {
                let r: BoxError = e.into();
                r
            })
            .and_then(|s| parser::parse_message(s).map_err(|e| e.into()));

        match message {
            Err(err) => {
                tracing::debug!("invalid message received {err:?}");
                self.force_close().await?;
                Ok(())
            }
            Ok(_) if !self.state.can_send() => {
                self.force_close().await?;
                Ok(())
            }
            Ok(Message::Connect { session }) => {
                self.session = Some(session);
                self.send_ack_data().await?;
                Ok(())
            }
            Ok(Message::Ack { session, len }) => {
                self.set_and_check_session(session).await?;
                todo!()
            }
            Ok(Message::Data { session, pos, data }) => {
                self.set_and_check_session(session).await?;
                self.receive_data(session, pos, data).await?;
                Ok(())
            }
            _ => todo!(),
        }
    }

    async fn force_close(&mut self) -> BoxResult<()> {
        if self.state != ConnectionState::Closed {
            self.state = ConnectionState::Closing;
        };
        let session = self.session.unwrap_or(0);

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

    async fn receive_data(&mut self, session: u32, pos: u32, data: String) -> BoxResult<()> {
        let offset = self.received_data.1;
        // usize -> u32 is OK because a LRCP messages are under 1000 bytes
        // (and the parser checks for that)
        assert!(data.len() < u32::MAX as usize);
        let data_len = data.len() as u32;
        match offset.cmp(&pos) {
            std::cmp::Ordering::Less => {
                if pos + data_len <= offset {
                    // stale data, we already got everything, so resend an Ack
                    self.send_ack_data().await?;
                    Ok(())
                } else {
                    todo!()
                }
            }
            std::cmp::Ordering::Equal => {
                self.received_data.0.push_str(&data);
                self.received_data.1 += data_len;
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
                offset
            );
                Ok(())
            }
        }
    }

    async fn send_ack_data(&mut self) -> BoxResult<()> {
        let ack = Message::Ack {
            session: self.get_session()?,
            len: self.received_data.1,
        };
        self.sock.send_to(&ack.to_bytes(), self.peer_addr).await?;
        Ok(())
    }

    /// for when a session is required
    fn get_session(&self) -> BoxResult<u32> {
        self.session
            .ok_or("Session is required but set to None".into())
    }
}

#[derive(Debug, PartialEq)]
pub enum Message {
    Connect {
        session: u32,
    },
    Ack {
        session: u32,
        len: u32,
    },
    Data {
        session: u32,
        pos: u32,
        data: String,
    },
    Close {
        session: u32,
    },
}

impl Message {
    fn to_bytes(&self) -> Vec<u8> {
        let mut result = "/".to_string();
        match self {
            Message::Connect { session } => write!(&mut result, "connect/{}/", session).unwrap(),
            Message::Ack { session, len } => {
                write!(&mut result, "ack/{}/{}/", session, len).unwrap()
            }
            Message::Data { session, pos, data } => {
                write!(&mut result, "data/{}/{}/{}/", session, pos, data).unwrap()
            }
            Message::Close { session } => write!(&mut result, "close/{}", session).unwrap(),
        };
        result.as_bytes().to_vec()
    }
}

mod parser {
    use super::{Message, MAX_MESSAGE_LEN};
    use nom::{
        branch::alt,
        bytes::complete::{escaped, is_not, tag, take_while, take_while1},
        character::complete::{self, one_of},
        combinator::{eof, map, map_res},
        error::{ErrorKind, ParseError},
        sequence::{delimited, preceded, separated_pair, tuple},
        Err, Finish, IResult,
    };

    #[derive(Debug, PartialEq, thiserror::Error)]
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

    type ParseResult<'input, T> = IResult<&'input str, T, InvalidMessage<&'input str>>;

    pub fn parse_message(input: &str) -> Result<Message, InvalidMessage<&str>> {
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

    fn inner_message(input: &str) -> ParseResult<Message> {
        alt((connect, ack, close, data))(input)
    }

    fn connect(input: &str) -> ParseResult<Message> {
        map(preceded(tag("connect/"), number), |session| {
            Message::Connect { session }
        })(input)
    }

    fn ack(input: &str) -> ParseResult<Message> {
        map(
            preceded(tag("ack/"), separated_pair(number, tag("/"), number)),
            |(session, len)| Message::Ack { session, len },
        )(input)
    }
    fn data(input: &str) -> ParseResult<Message> {
        map(
            preceded(
                tag("data/"),
                tuple((number, tag("/"), number, tag("/"), escaped_string)),
            ),
            |(session, _, pos, _, data)| Message::Data { session, pos, data },
        )(input)
    }

    fn close(input: &str) -> ParseResult<Message> {
        map(preceded(tag("close/"), number), |session| Message::Close {
            session,
        })(input)
    }

    fn number(input: &str) -> ParseResult<u32> {
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
    fn escaped_string(input: &str) -> ParseResult<String> {
        let normal = take_while1(|c: char| c.is_ascii() && c != '/' && c != '\\');
        map(escaped(normal, '\\', one_of(r#"/\"#)), |s: &str| {
            println!("coucou! {s}");
            s.replace(r#"\/"#, "/").replace(r#"\\"#, "\\")
        })(input)
    }

    #[cfg(test)]
    mod test {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn test_connect() {
            assert_eq!(
                parse_message("/connect/123456/"),
                Ok(Message::Connect { session: 123456 })
            );
        }

        #[test]
        fn test_overflow() {
            assert_eq!(
                parse_message("/connect/2147483649/"),
                Err(InvalidMessage::InvalidNumber(2147483649)),
                "valid u32 but too big for this protocol"
            );

            let too_big = "4294967296/"; // u32::MAX + 1
            println!("max u32: {}", u32::MAX);
            assert_eq!(
                parse_message(&format!("/connect/{}", too_big)),
                Err(InvalidMessage::Nom(too_big, ErrorKind::Digit)),
                "invalid u32, too big"
            );
        }

        #[test]
        fn test_close() {
            assert_eq!(
                parse_message("/close/123456/"),
                Ok(Message::Close { session: 123456 })
            )
        }

        #[test]
        fn test_data() {
            assert_eq!(
                parse_message("/data/123456/12/coucou/"),
                Ok(Message::Data {
                    session: 123456,
                    pos: 12,
                    data: "coucou".to_string()
                })
            )
        }

        #[test]
        fn test_data_escaped() {
            assert_eq!(
                parse_message(r#"/data/123456/12/cou\/cou/"#),
                Ok(Message::Data {
                    session: 123456,
                    pos: 12,
                    data: "cou/cou".to_string()
                }),
                "escaped slash"
            );

            assert_eq!(
                parse_message(r#"/data/123456/12/cou\\cou/"#),
                Ok(Message::Data {
                    session: 123456,
                    pos: 12,
                    data: r#"cou\cou"#.to_string()
                }),
                "escaped backslash"
            );
        }

        #[test]
        fn test_message_too_big() {
            let data: String = std::iter::repeat('a').take(MAX_MESSAGE_LEN).collect();
            let msg = format!("/data/123456/0/{}/", data);
            assert_eq!(
                parse_message(&msg),
                Err(InvalidMessage::MessageTooBig {
                    max_len: MAX_MESSAGE_LEN,
                    len: msg.len()
                })
            );
        }
    }
}
