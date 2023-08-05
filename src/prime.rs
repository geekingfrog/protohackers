use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::utils::BoxResult;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(tag = "method", content = "number")]
enum Request {
    #[serde(rename = "isPrime")]
    IsPrime(f64),
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "method", content = "prime")]
enum Response {
    #[serde(rename = "isPrime")]
    IsPrime(bool),
}

pub async fn main(addr: SocketAddr) -> BoxResult<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("listening on {addr}");

    loop {
        let (stream, socket_addr) = listener.accept().await?;
        tokio::spawn(async move {
            tracing::info!("incoming connection from {}", socket_addr);
            match prime(stream).await {
                Ok(_) => tracing::info!("done with {}", socket_addr),
                Err(err) => tracing::info!("Error while processing {}: {err:?}", socket_addr),
            }
        });
    }
}

async fn prime(stream: TcpStream) -> BoxResult<()> {
    let (rdr, mut wrt) = stream.into_split();
    let rdr = tokio::io::BufReader::new(rdr);
    let mut lines = rdr.split(0x0A);

    while let Some(slice) = lines.next_segment().await? {
        if slice.is_empty() {
            continue;
        }

        match process_request(&slice) {
            Ok(res) => {
                let resp = serde_json::to_vec(&Response::IsPrime(res)).unwrap();
                wrt.write_all(&resp[..]).await?;
                wrt.write(&[0x0A]).await?;
            }
            Err(err) => {
                tracing::info!("Invalid request received for {err:?} - {:?}", std::str::from_utf8(&slice));
                wrt.write_all(b"error").await?;
                wrt.shutdown().await?;
                return Ok(());
            }
        };
    }

    wrt.shutdown().await?;
    Ok(())
}

fn process_request(slice: &[u8]) -> BoxResult<bool> {
    let request: Request = serde_json::from_slice(slice)?;
    tracing::info!("processing request {request:?}");
    let n = match request {
        Request::IsPrime(n) => n,
    };
    if n.fract() != 0.0 || n < 0.0 {
        return Ok(false);
    }
    let n = n as u64;
    Ok(is_prime::is_prime(&n.to_string()))
}
