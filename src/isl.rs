use bit_reverse;
use bytes::{BufMut, BytesMut};
use std::{net::SocketAddr, task::Poll};
use tokio::{
    io::{
        AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
    },
    net::{TcpListener, TcpStream},
};

#[cfg(test)]
use tokio::net::ToSocketAddrs;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxResult<T> = Result<T, BoxError>;

pub async fn main(addr: SocketAddr) -> BoxResult<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("tcp listening on {}", addr);
    run_server(listener).await
}

async fn run_server(listener: TcpListener) -> BoxResult<()> {
    loop {
        let (stream, addr) = listener.accept().await?;
        tracing::info!("new client at {}", addr);
        tokio::spawn(async move {
            process(stream)
                .await
                .expect(&format!("error while processing stream for {addr}"))
        });
    }
}

async fn process(stream: TcpStream) -> BoxResult<()> {
    let mut stream = BufReader::new(stream);
    let mut raw_cipherspec = Vec::with_capacity(50);
    stream.read_until(0, &mut raw_cipherspec).await?;
    // strip the delimiter null byte
    let n = raw_cipherspec.len();
    let cipherspec = CipherSpec::from_bytes(&raw_cipherspec[..n - 1])?;

    tracing::trace!("got cipherspec {:?}", cipherspec);

    // TODO make that a BufMut, and then loop over this bufmut only to grab lines.
    // this ensure we're processing everything before trying to read more
    let mut line_buf = BytesMut::with_capacity(stream.buffer().len());

    for (i, encrypted_byte) in stream.buffer().iter().enumerate() {
        line_buf.put_u8(cipherspec.decrypt_byte(*encrypted_byte, i));
    }

    let mut stream = IslStream::new(stream.into_inner(), cipherspec);
    stream.read_counter += line_buf.len();
    let mut stream = BufWriter::new(BufReader::new(stream));

    loop {
        match line_buf.iter().position(|b| *b == b'\n') {
            None => {
                stream.flush().await?;
                if line_buf.capacity() < 5000 {
                    line_buf.reserve(5000 - line_buf.capacity());
                }
                let read_amount = stream.read_buf(&mut line_buf).await?;
                if read_amount == 0 {
                    tracing::trace!("EOF reached");
                    break;
                }
                tracing::debug!("got a read of {read_amount} bytes");
                tracing::trace!(
                    "got a request: {:?}",
                    std::str::from_utf8(&line_buf[..read_amount])
                );
                continue;
            }
            Some(idx) => {
                if idx == 0 {
                    return Err("Empty request is invalid".into());
                }
                let line = line_buf.split_to(idx + 1).freeze();
                let line = std::str::from_utf8(&line[..idx])?;
                let result = process_request(line).ok_or(format!("Invalid request: {line}"))?;
                stream.write_all(result.as_bytes()).await?;
                stream.write_u8(b'\n').await?;
            }
        }
    }
    stream.flush().await?;
    Ok(())
}

#[pin_project::pin_project]
struct IslStream {
    #[pin]
    inner: TcpStream,
    cipherspec: CipherSpec,
    read_counter: usize,
    write_counter: usize,
}

impl IslStream {
    fn new(inner: TcpStream, cipherspec: CipherSpec) -> Self {
        Self {
            inner,
            cipherspec,
            read_counter: 0,
            write_counter: 0,
        }
    }

    #[cfg(test)]
    async fn connect<A>(addr: A, cipherspec: CipherSpec) -> std::io::Result<Self>
    where
        A: ToSocketAddrs,
    {
        let mut stream = TcpStream::connect(addr).await?;
        let mut bs = cipherspec.to_bytes();
        bs.push(0);
        stream.write_all(&bs).await?;
        let stream = IslStream::new(stream, cipherspec);
        Ok(stream)
    }
}

impl AsyncRead for IslStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let start = buf.filled().len();
        futures::ready!(self.as_mut().project().inner.poll_read(cx, buf))?;
        let end = buf.filled().len();
        if start != end {
            for b in buf.filled_mut()[start..].iter_mut() {
                *b = self.cipherspec.decrypt_byte(*b, start + self.read_counter);
                self.read_counter += 1;
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for IslStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        // here, I encrypt possibly more bytes than the underlying reader can
        // take, and that would result in duplicated work. But :shrug:
        let bs = buf
            .iter()
            .enumerate()
            .map(|(i, b)| self.cipherspec.encrypt_byte(*b, i + self.write_counter))
            .collect::<Vec<_>>();
        let n = futures::ready!(self.as_mut().project().inner.poll_write(cx, &bs))?;
        self.write_counter += n;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct CipherSpec {
    ciphers: Vec<CipherByte>,
    inversed: Vec<CipherByte>,
}

impl CipherSpec {
    fn new(ciphers: Vec<CipherByte>) -> BoxResult<Self> {
        let inversed = ciphers.iter().rev().map(|x| x.inverse()).collect();
        let this = Self { ciphers, inversed };
        this.check_spec_valid()?;
        Ok(this)
    }

    fn from_bytes(raw: &[u8]) -> BoxResult<CipherSpec> {
        let ciphers = parse_ciphers(raw)?;
        CipherSpec::new(ciphers)
    }

    #[cfg(test)]
    fn to_bytes(&self) -> Vec<u8> {
        let mut res = Vec::new();
        for c in &self.ciphers {
            match c {
                CipherByte::ReverseBits => res.push(1),
                CipherByte::Xor(x) => {
                    res.push(2);
                    res.push(*x);
                }
                CipherByte::XorPos => res.push(3),
                CipherByte::Add(x) => {
                    res.push(4);
                    res.push(*x);
                }
                CipherByte::AddPos => res.push(5),
                CipherByte::SubPos => panic!("subpos should never be encoded"),
            }
        }
        res
    }

    fn check_spec_valid(&self) -> BoxResult<()> {
        let all_no_op = (u8::MIN..u8::MAX)
            .into_iter()
            .enumerate()
            .all(|(i, b)| self.encrypt_byte(b, i) == b);
        if all_no_op {
            Err(format!("Invalid cipherspec. NO-OP {:?}", self).into())
        } else {
            Ok(())
        }
    }

    fn encrypt_byte(&self, b: u8, pos: usize) -> u8 {
        let p = (pos % 256) as u8;
        self.ciphers.iter().fold(b, |x, cipher| cipher.apply(x, p))
    }

    fn decrypt_byte(&self, b: u8, pos: usize) -> u8 {
        let p = (pos % 256) as u8;
        self.inversed.iter().fold(b, |x, cipher| cipher.apply(x, p))
    }
}

fn parse_ciphers(raw: &[u8]) -> Result<Vec<CipherByte>, String> {
    let mut ciphers = Vec::new();
    let mut i = 0;

    loop {
        if let Some(b) = raw.get(i) {
            match b {
                1 => ciphers.push(CipherByte::ReverseBits),
                2 => {
                    i += 1;
                    match raw.get(i) {
                        Some(x) => ciphers.push(CipherByte::Xor(*x)),
                        None => return Err("Unexpected EOF".into()),
                    };
                }
                3 => ciphers.push(CipherByte::XorPos),
                4 => {
                    i += 1;
                    match raw.get(i) {
                        Some(x) => ciphers.push(CipherByte::Add(*x)),
                        None => return Err("Unexpected EOF".into()),
                    };
                }
                5 => ciphers.push(CipherByte::AddPos),
                _ => return Err(format!("Unexpected cipher: {:x}", b).into()),
            }
            i += 1;
        } else {
            break;
        }
    }

    Ok(ciphers)
}

#[derive(Debug, PartialEq, Eq)]
enum CipherByte {
    ReverseBits,
    Xor(u8),
    XorPos,
    Add(u8),
    AddPos,
    SubPos,
}

impl CipherByte {
    fn inverse(&self) -> CipherByte {
        match self {
            CipherByte::ReverseBits => CipherByte::ReverseBits,
            CipherByte::Xor(x) => CipherByte::Xor(*x),
            CipherByte::XorPos => CipherByte::XorPos,
            CipherByte::Add(x) => CipherByte::Add((u8::MAX - x).wrapping_add(1)),
            CipherByte::AddPos => CipherByte::SubPos,
            CipherByte::SubPos => CipherByte::AddPos,
        }
    }

    fn apply(&self, x: u8, pos: u8) -> u8 {
        match self {
            CipherByte::ReverseBits => bit_reverse::LookupReverse::swap_bits(x),
            CipherByte::Xor(y) => x ^ y,
            CipherByte::XorPos => x ^ pos,
            CipherByte::Add(y) => x.wrapping_add(*y),
            CipherByte::AddPos => x.wrapping_add(pos),
            CipherByte::SubPos => x.wrapping_sub(pos),
        }
    }
}

fn process_request(input: &str) -> Option<&str> {
    input
        .split(",")
        .fold(None, |mb_max, fragment| match mb_max {
            None => Some((parse_fragment(fragment), fragment)),
            Some((len, _)) => {
                let n = parse_fragment(fragment);
                if n > len {
                    Some((n, fragment))
                } else {
                    mb_max
                }
            }
        })
        .map(|x| x.1)
}

fn parse_fragment(frag: &str) -> u32 {
    let (n_str, _rest) = frag.split_once('x').expect("valid request");
    let n = n_str.parse::<u32>().expect("valid number");
    n
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;
    use pretty_assertions::assert_eq;
    use tokio::{task::JoinHandle, time::timeout};

    #[test]
    fn test_no_op_ciphers() {
        use CipherByte::*;
        assert!(CipherSpec::new(vec![]).is_err(), "empty");
        assert!(CipherSpec::new(vec![Xor(0)]).is_err(), "xor(0)");
        assert!(CipherSpec::new(vec![Xor(3), Xor(3)]).is_err(), "double xor");
    }

    #[test]
    fn test_parse_cipher() {
        use CipherByte::*;
        assert_eq!(Ok(vec![Xor(1)]), parse_ciphers(&[0x02, 0x01]));
        assert_eq!(
            Ok(vec![ReverseBits, Xor(5), XorPos, Add(255), AddPos]),
            parse_ciphers(&[0x01, 0x02, 0x05, 0x03, 0x04, 0xFF, 0x05])
        );
    }

    #[test]
    fn test_roundtrip() {
        use CipherByte::*;
        let cipher = CipherSpec::new(vec![Add(12)]).unwrap();
        assert_eq!(cipher.decrypt_byte(cipher.encrypt_byte(b'c', 0), 0), b'c')
    }

    #[test]
    fn test_encrypt_decrypt() {
        use CipherByte::*;
        let cipher = CipherSpec::new(vec![Xor(1), ReverseBits]).unwrap();
        let base = b"hello";
        let encrypted = base
            .iter()
            .copied()
            .enumerate()
            .map(|(i, b)| cipher.encrypt_byte(b, i))
            .collect::<Vec<_>>();
        assert_eq!(encrypted, vec![0x96, 0x26, 0xb6, 0xb6, 0x76]);

        let decrypted = encrypted
            .iter()
            .copied()
            .enumerate()
            .map(|(i, b)| cipher.decrypt_byte(b, i))
            .collect::<Vec<_>>();
        assert_eq!(decrypted, base);
    }

    #[test]
    fn test_encrypt_decrypt2() {
        use CipherByte::*;

        let cipher = CipherSpec::new(vec![AddPos, AddPos]).unwrap();
        let base = b"hello";
        let encrypted = base
            .iter()
            .copied()
            .enumerate()
            .map(|(i, b)| cipher.encrypt_byte(b, i))
            .collect::<Vec<_>>();
        assert_eq!(encrypted, vec![0x68, 0x67, 0x70, 0x72, 0x77]);

        let decrypted = encrypted
            .iter()
            .copied()
            .enumerate()
            .map(|(i, b)| cipher.decrypt_byte(b, i))
            .collect::<Vec<_>>();
        assert_eq!(decrypted, base);
    }

    #[test]
    fn test_process_request() {
        assert_eq!(
            process_request("10x toy car,15x dog on a string,4x inflatable motorcycle"),
            Some("15x dog on a string")
        );
        assert_eq!(
            process_request("40x toy car,15x dog on a string,4x inflatable motorcycle"),
            Some("40x toy car")
        );
    }

    /// make sure to abort a task on drop, so that it's easier to
    /// do assertion in tests with less worry about cleaning up
    struct AbortHdl<T>(JoinHandle<T>);
    impl<T> Drop for AbortHdl<T> {
        fn drop(&mut self) {
            self.0.abort()
        }
    }

    #[tokio::test]
    async fn test_e2e_longer_cipher() -> BoxResult<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let _server = tokio::spawn(async move { run_server(listener).await.unwrap() });

        use CipherByte::*;
        let cipher = CipherSpec::new(vec![Xor(123), AddPos, ReverseBits]).unwrap();
        let stream = IslStream::connect(addr, cipher).await?;
        let mut stream = BufReader::new(stream);

        stream.write_all("4x dog,5x car\n".as_bytes()).await?;
        stream.flush().await?;
        let mut lines = String::new();
        timeout(Duration::from_millis(10), stream.read_line(&mut lines)).await??;
        assert_eq!(lines, "5x car\n".to_string());
        Ok(())
    }

    #[tokio::test]
    async fn test_e2e_multiple_requests() -> BoxResult<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let _server = tokio::spawn(async move { run_server(listener).await.unwrap() });

        use CipherByte::*;
        let cipher = CipherSpec::new(vec![Xor(123), AddPos, ReverseBits]).unwrap();
        let stream = IslStream::connect(addr, cipher).await?;
        let mut stream = BufReader::new(stream);

        stream.write_all("4x dog,5x car\n".as_bytes()).await?;
        stream
            .write_all("10x dogs,15x cats,12x crabs\n".as_bytes())
            .await?;
        stream.flush().await?;
        stream.shutdown().await?;
        let mut lines = String::new();
        timeout(Duration::from_millis(10), stream.read_to_string(&mut lines)).await??;
        assert_eq!(lines, "5x car\n15x cats\n".to_string());
        Ok(())
    }
}
