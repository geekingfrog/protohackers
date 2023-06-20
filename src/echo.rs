use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::{TcpListener, TcpStream};

type BoxResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
pub async fn main() -> BoxResult<()> {
    let addr = "0.0.0.0:6789";
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
        stream.write_all(&buf[0..read]).await?;
    }
    stream.shutdown().await?;
    Ok(())
}
