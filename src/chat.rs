use std::collections::BTreeMap;
use std::sync::Arc;

use futures::{stream, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

type BoxResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
pub async fn main() -> BoxResult<()> {
    let addr = "0.0.0.0:6789";
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("listening on {addr}");

    let server = Arc::new(Server::new());
    loop {
        let (stream, socket_addr) = listener.accept().await?;
        tokio::spawn({
            let server = Arc::clone(&server);
            async move {
                tracing::info!("incoming connection from {}", socket_addr);
                server.handle_client(stream).await.unwrap();
                ()
            }
        });
    }
}

struct Server {
    clients: Arc<Mutex<BTreeMap<String, WriteClient>>>,
}

impl Server {
    fn new() -> Self {
        Self {
            clients: Default::default(),
        }
    }

    async fn handle_client(&self, stream: TcpStream) -> BoxResult<()> {
        let (rdr, wrt) = stream.into_split();
        let mut rdr = ReadClient {
            rdr: BufReader::new(rdr),
        };
        let mut wrt = WriteClient { wrt };
        wrt.send_msg("Gimme a name!").await?;

        let username = match rdr.read_msg().await? {
            Some(username) if valid_username(&username) => username,
            x => {
                tracing::warn!("Invalid username: {:?}", x);
                wrt.shutdown().await?;
                return Ok(());
            }
        };

        let presence_message = {
            let mut msg = "* the room contains ".to_string();
            for (i, (usr, _)) in self.clients.lock().await.iter().enumerate() {
                if i == 0 {
                    msg.push_str(" ");
                } else {
                    msg.push_str(", ");
                }
                msg.push_str(usr)
            }
            msg
        };
        wrt.send_msg(&presence_message).await?;

        self.broadcast(&username, &format!("* {} is in da place", username))
            .await?;
        self.clients.lock().await.insert(username.clone(), wrt);
        tracing::info!("{username} connected");

        loop {
            match rdr.read_msg().await? {
                Some(msg) => {
                    let message = format!("[{username}] {msg}");
                    self.broadcast(&username, &message).await?;
                }
                None => {
                    self.disconnect_client(&username).await?;
                    break Ok(());
                }
            }
        }
    }

    async fn broadcast(&self, source: &str, msg: &str) -> BoxResult<()> {
        let mut clients = self.clients.lock().await;
        stream::iter(
            clients
                .iter_mut()
                .filter(|(username, _)| *username != source),
        )
        .for_each_concurrent(10, |(username, client)| async {
            match client.send_msg(&msg).await {
                Ok(_) => (),
                Err(err) => {
                    tracing::warn!("Couldn't send message to {}: {err:?}", username.clone())
                }
            }
        })
        .await;

        Ok(())
    }

    async fn disconnect_client(&self, username: &str) -> BoxResult<()> {
        tracing::info!("disconnecting {username}");
        let msg = format!("* {username} is no more");
        self.broadcast(username, &msg).await?;
        self.clients.lock().await.remove(username);
        Ok(())
    }
}

struct ReadClient {
    rdr: BufReader<OwnedReadHalf>,
}

impl ReadClient {
    /// read one line from that client. Returns None if the client disconnected
    async fn read_msg(&mut self) -> BoxResult<Option<String>> {
        let mut buf = String::new();
        let n = self.rdr.read_line(&mut buf).await?;
        if n == 0 {
            Ok(None)
        } else {
            let result = buf.trim_end().to_string();
            Ok(Some(result))
        }
    }
}

struct WriteClient {
    wrt: OwnedWriteHalf,
}

impl WriteClient {
    async fn send_msg(&mut self, msg: &str) -> BoxResult<()> {
        self.wrt.write_all(msg.as_bytes()).await?;
        self.wrt.write_u8(0x0A).await?;
        Ok(())
    }

    async fn shutdown(mut self) -> BoxResult<()> {
        self.wrt.shutdown().await?;
        Ok(())
    }
}

fn valid_username(name: &str) -> bool {
    name.len() >= 1 && name.chars().all(|c| c.is_alphanumeric())
}
