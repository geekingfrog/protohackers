use std::{
    io::ErrorKind,
    net::{SocketAddr, ToSocketAddrs},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpSocket, TcpStream,
    },
};

type BoxResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub async fn main(addr: SocketAddr) -> BoxResult<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("listening on {addr}");

    loop {
        let (stream, client_addr) = listener.accept().await?;
        let upstream = connect_to_upstream().await?;

        let (down_read, down_write) = stream.into_split();
        let (up_read, up_write) = upstream.into_split();
        tracing::debug!("setting up mitm for {client_addr}");

        tokio::spawn(async move {
            let to_upstream = Mitm::new(
                down_read,
                up_write,
                format!("from {client_addr} to upstream"),
            );
            let to_client = Mitm::new(
                up_read,
                down_write,
                format!("from upstream to {client_addr}"),
            );
            let res = tokio::try_join!(to_upstream.intercept(), to_client.intercept());
            match res {
                Ok(_) => (),
                Err(err) => {
                    tracing::error!("Error while mitm the address {}: {err:?}", client_addr);
                }
            }
        });
    }
}

struct Mitm {
    rdr: BufReader<OwnedReadHalf>,
    wrt: OwnedWriteHalf,
    tag: String,
}

impl Mitm {
    fn new(from_stream: OwnedReadHalf, to_stream: OwnedWriteHalf, tag: String) -> Self {
        Self {
            rdr: BufReader::new(from_stream),
            wrt: to_stream,
            tag,
        }
    }

    async fn intercept(mut self) -> BoxResult<()> {
        let mut line: Vec<u8> = Vec::with_capacity(1000);

        loop {
            line.clear();
            let read = self.rdr.read_until(0x0A, &mut line).await?;
            if read == 0 {
                break;
            }
            // reached EOF without an end line, meaning the peer disconnected
            // without sending a full message. Ignore the incomplete message then.
            if line[0..read].last() != Some(&0x0A) {
                tracing::debug!(
                    "message not ending with a newline and got EOF: {:?}",
                    std::str::from_utf8(&line[0..read])
                );
                break;
            }

            // strip the ending newline
            let message = std::str::from_utf8(&line[0..read - 1])?;
            tracing::trace!("got a line: {}", &message[0..message.len()]);
            let mut new_message = transform_line(message);
            new_message.push('\n');
            match self.wrt.write_all(&new_message.as_bytes()).await {
                Err(err) if err.kind() == ErrorKind::BrokenPipe => break,
                _ => (),
            }
        }

        tracing::info!("Shutting down mitm for {}", self.tag);
        match self.wrt.shutdown().await {
            // ignore a not connected error
            Err(err) if err.kind() == ErrorKind::NotConnected => Ok(()),
            x => x,
        }?;
        Ok(())
    }
}

async fn connect_to_upstream() -> BoxResult<TcpStream> {
    let raw = "chat.protohackers.com:16963";
    match raw.to_socket_addrs()?.next() {
        Some(addr) => {
            if addr.is_ipv4() {
                let socket = TcpSocket::new_v4()?;
                Ok(socket.connect(addr).await?)
            } else if addr.is_ipv6() {
                let socket = TcpSocket::new_v6()?;
                Ok(socket.connect(addr).await?)
            } else {
                return Err("WTF the IEEE is up to no good !".into());
            }
        }
        None => return Err("No upstream address found for {raw}".into()),
    }
}

fn transform_line(input: &str) -> String {
    let coucou = input
        .split(" ")
        .map(|word| {
            if is_boguscoin_address(word) {
                "7YWHMfk9JZe0LM0g1ZauHuiSxhI"
            } else {
                word
            }
        })
        .collect::<Vec<_>>();
    coucou.join(" ")
}

fn is_boguscoin_address(w: &str) -> bool {
    // it consists of at least 26, and at most 35
    w.len() >= 26 && w.len() <= 35
    // , alphanumeric characters
    && w.chars().all(|c| c.is_alphanumeric())
    // it starts with a "7"
    && w.chars().next() == Some('7')
}

#[cfg(test)]
mod test {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_no_modification() {
        assert_eq!(
            "* The room contains: alice",
            transform_line("* The room contains: alice")
        );

        assert_eq!("bob", transform_line("bob"));
    }

    #[test]
    fn test_modification() {
        assert_eq!(
            "Hi alice, please send payment to 7YWHMfk9JZe0LM0g1ZauHuiSxhI",
            &transform_line(
                "Hi alice, please send payment to 7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX"
            ),
        )
    }
}
