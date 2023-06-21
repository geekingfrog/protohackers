use std::collections::HashMap;

use tokio::net::UdpSocket;

type BoxResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
pub async fn main() -> BoxResult<()> {
    let addr = "0.0.0.0:6789";
    let socket = UdpSocket::bind(addr).await?;
    tracing::info!("listening on {addr}");

    // maximum size of udp packet for this problem
    let mut buf = [0; 1000];
    let mut db: HashMap<String, String> = HashMap::new();

    loop {
        let (n, peer_addr) = socket.recv_from(&mut buf).await?;
        tracing::trace!(
            "received {n} bytes from {peer_addr} - {:?}",
            std::str::from_utf8(&buf[0..n])
        );

        let request = Request::parse(&buf[0..n]);

        match request? {
            Request::Insert { key, value } => {
                db.insert(key, value);
            }
            Request::Retrieve { key } => {
                if let Some(val) = db.get(&key) {
                    let message = format!("{key}={val}");
                    socket.send_to(&message.as_bytes(), peer_addr).await?;
                }
            }
            Request::Version => {
                let message = "version=udpdb -1.2";
                socket.send_to(&message.as_bytes(), peer_addr).await?;
            }
            Request::Ignored => {}
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Request {
    Insert { key: String, value: String },
    Retrieve { key: String },
    Version,
    Ignored,
}

impl Request {
    fn parse(data: &[u8]) -> BoxResult<Self> {
        let data = std::str::from_utf8(data)?.to_string();
        match &data.find('=') {
            Some(i) => {
                let (key, value) = data.split_at(*i);
                let value = &value[1..]; // strip the leading =
                if key == "version" {
                    Ok(Request::Ignored)
                } else {
                    Ok(Request::Insert {
                        key: key.to_string(),
                        value: value.to_string(),
                    })
                }
            }
            None => {
                if data == "version" {
                    Ok(Request::Version)
                } else {
                    Ok(Request::Retrieve { key: data })
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse() {
        assert_eq!(
            Request::parse(b"version").expect("correct parse"),
            Request::Version
        );

        assert_eq!(
            Request::parse(b"version=").expect("correct parse"),
            Request::Ignored,
            "ignore attempt to modify the version"
        );

        assert_eq!(
            Request::parse(b"foo=").expect("correct parse"),
            Request::Insert {
                key: "foo".to_string(),
                value: "".to_string()
            }
        );

        assert_eq!(
            Request::parse(b"foo=bar").expect("correct parse"),
            Request::Insert {
                key: "foo".to_string(),
                value: "bar".to_string()
            }
        );

        assert_eq!(
            Request::parse(b"foo==bar").expect("correct parse"),
            Request::Insert {
                key: "foo".to_string(),
                value: "=bar".to_string()
            }
        );

        assert_eq!(
            Request::parse(b"=").expect("correct parse"),
            Request::Insert {
                key: "".to_string(),
                value: "".to_string()
            }
        );

        assert_eq!(
            Request::parse(b"").expect("correct parse"),
            Request::Retrieve {
                key: "".to_string(),
            }
        );
    }
}
