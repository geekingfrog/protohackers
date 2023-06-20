use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

type BoxResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

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

#[tokio::main]
pub async fn main() -> BoxResult<()> {
    let addr = "0.0.0.0:6789";
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
    let mut lines = rdr.lines();

    while let Some(line) = lines.next_line().await? {
        if line.is_empty() {
            continue;
        }

        match process_request(&line) {
            Ok(res) => {
                let resp = serde_json::to_vec(&Response::IsPrime(res)).unwrap();
                wrt.write_all(&resp[..]).await?;
                wrt.write(&[0x0A]).await?;
            }
            Err(err) => {
                tracing::info!("Invalid request received for {err:?} - {line}");
                wrt.write_all(b"error").await?;
                wrt.shutdown().await?;
                return Ok(());
            }
        };
    }

    wrt.shutdown().await?;
    Ok(())
}

fn process_request(slice: &str) -> BoxResult<bool> {
    let request: Request = serde_json::from_str(slice)?;
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
