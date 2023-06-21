use std::collections::BTreeMap;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
            match Worker::new().handle_client(stream).await {
                Ok(_) => tracing::info!("done with {}", socket_addr),
                Err(err) => tracing::info!("Error while processing {}: {err:?}", socket_addr),
            }
        });
    }
}

struct Worker {
    store: BTreeMap<i32, i32>,
}

impl Worker {
    fn new() -> Self {
        Self {
            store: BTreeMap::new(),
        }
    }

    async fn handle_client(&mut self, stream: TcpStream) -> BoxResult<()> {
        let (rdr, mut wrt) = stream.into_split();
        let mut rdr = tokio::io::BufReader::new(rdr);

        loop {
            let c = match rdr.read_u8().await {
                Ok(n) if n == 0 => break,
                Ok(c) => c,
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(err.into());
                    }
                }
            };

            match c.into() {
                // insert
                'I' => self.process_insert(&mut rdr).await?,
                // query
                'Q' => {
                    let result = self.process_query(&mut rdr).await?;
                    wrt.write_i32(result).await?;
                }
                _ => {
                    tracing::warn!("Unknown instruction: {c}");
                    break;
                }
            }
        }

        wrt.shutdown().await?;
        Ok(())
    }

    async fn process_insert<R: AsyncReadExt + Unpin>(&mut self, rdr: &mut R) -> BoxResult<()> {
        let ts = rdr.read_i32().await?;
        let price = rdr.read_i32().await?;
        self.store.insert(ts, price);
        Ok(())
    }

    async fn process_query<R: AsyncReadExt + Unpin>(&self, rdr: &mut R) -> BoxResult<i32> {
        let from_ts = rdr.read_i32().await?;
        let to_ts = rdr.read_i32().await?;

        if to_ts < from_ts {
            return Ok(0);
        }

        use std::ops::Bound::Included;
        let mut i = 0usize;
        let mut sum = 0f64;
        for (_, val) in self.store.range((Included(from_ts), Included(to_ts))) {
            i += 1;
            sum += *val as f64;
        }
        if i == 0 {
            Ok(0)
        } else {
            let mean = sum / (i as f64);
            Ok(mean.round() as i32)
        }
    }
}
