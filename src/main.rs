use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::{TcpListener, TcpStream};

type BoxResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> BoxResult<()> {
    let listener = TcpListener::bind("0.0.0.0:6789").await?;
    loop {
        let (stream, socket_addr) = listener.accept().await?;
        tokio::spawn(async move {
            eprintln!("incoming connection from {}", socket_addr);
            match echo(stream).await {
                Ok(_) => eprintln!("done with {}", socket_addr),
                Err(err) => eprintln!("Error while processing {}: {err:?}", socket_addr),
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
