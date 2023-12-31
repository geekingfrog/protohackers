use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::utils::BoxResult;

pub async fn main(addr: SocketAddr) -> BoxResult<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("listening on {addr}");

    loop {
        let (stream, socket_addr) = listener.accept().await?;
        tokio::spawn(async move {
            tracing::info!("incoming connection from {}", socket_addr);
            match echo(stream).await {
                Ok(_) => tracing::info!("done with {}", socket_addr),
                Err(err) => tracing::info!("Error while processing {}: {err:?}", socket_addr),
            }
        });
    }
}

async fn echo(mut stream: TcpStream) -> BoxResult<()> {
    let mut buf = [0; 4096];
    loop {
        let read = stream.read(&mut buf).await?;
        if read == 0 {
            break;
        }
        tracing::debug!("echoing data: {:?}", std::str::from_utf8(&buf[0..read]));
        stream.write_all(&buf[0..read]).await?;
    }
    stream.shutdown().await?;
    Ok(())
}
