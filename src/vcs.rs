use std::{
    collections::BTreeMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use crate::utils::{yolo_send, BoxResult};
use similar::algorithms::Algorithm;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::{mpsc, oneshot},
};

pub async fn main(addr: SocketAddr) -> BoxResult<()> {
    Server::bind(addr).await?.run().await?;
    // let buf = b"PUTN";
    // println!("{:?} -- {:?}", &buf[..3], &buf[..3] == b"PUT");
    tracing::info!("exiting");
    Ok(())
}

#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone)]
pub(crate) enum CommandError {
    #[error("ERR illegal method: {0}")]
    IllegalMethod(String),
    #[error("ERR illegal file name")]
    IllegalFilename,
    #[error("ERR usage: PUT file length newline data")]
    PutUsage,
    #[error("ERR usage: GET [file] revision")]
    GetUsage,
    #[error("ERR usage: LIST dir")]
    ListUsage,
    // #[error("ERR no such file")]
    // NoFile,
    // #[error("ERR no such revision")]
    // NoRevision,
    // #[error("ERR incorrect utf-8 command")]
    // IncorrectUtf8,
    #[error("ERR invalid revision {0}")]
    InvalidRevision(String),
    #[error("ERR text files only")]
    NonTextContent,
}

#[derive(Debug)]
enum VcsCommand {
    Put {
        path: Vec<String>,
        data: Vec<u8>,
        resp: oneshot::Sender<usize>,
    },
    Get {
        path: Vec<String>,
        revision: Option<usize>,
        resp: oneshot::Sender<Option<Vec<u8>>>,
    },
    List {
        path: Vec<String>,
        resp: oneshot::Sender<Vec<(String, Option<usize>)>>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ClientCommand {
    Put {
        path: Vec<String>,
        len: usize,
    },
    // Put { path: String, len: usize },
    Get {
        path: Vec<String>,
        rev: Option<usize>,
    },
    List {
        path: Vec<String>,
    },
    Help,
}

mod parser {
    // TODO see if using nom to parse is better
    #![allow(dead_code, unused_imports, unused_variables)]
    use super::{ClientCommand, CommandError};
    use nom::{
        branch::alt,
        // complete::tag,
        bytes::complete::{tag, tag_no_case, take_until},
        character::complete::{self as character, space0},
        combinator::{all_consuming, cut, map, map_res, opt, rest},
        error::{context, ContextError, ErrorKind, ParseError, VerboseError},
        multi::{many1, separated_list0},
        sequence::{preceded, separated_pair, tuple},
        Finish,
        IResult,
        Parser,
    };

    #[derive(Debug)]
    enum VcsParseError<I> {
        Nom { input: I, kind: ErrorKind },
        Cmd(CommandError),
    }

    impl<I> nom::error::ParseError<I> for VcsParseError<I> {
        fn from_error_kind(input: I, kind: ErrorKind) -> Self {
            VcsParseError::Nom { input, kind }
        }

        fn append(input: I, kind: ErrorKind, other: Self) -> Self {
            other
        }
    }

    impl<I> nom::error::FromExternalError<I, CommandError> for VcsParseError<I> {
        fn from_external_error(input: I, kind: ErrorKind, e: CommandError) -> Self {
            VcsParseError::Cmd(e)
        }
    }

    type Res<T, E> = IResult<T, E, VcsParseError<T>>;
    // type Res<T, E> = IResult<T, E>;

    pub(crate) fn parse_command(buf: &[u8]) -> Result<ClientCommand, CommandError> {
        let n = buf.len();
        if is_put_request(&buf) {
            let (path, len) = parse_put_request(&buf[3..])?;
            Ok(ClientCommand::Put { path, len })
        } else if is_get_request(&buf) {
            let (path, rev) = parse_get_request(&buf[3..])?;
            Ok(ClientCommand::Get { path, rev })
        } else if is_help_request(&buf) {
            Ok(ClientCommand::Help)
        } else if is_list_request(&buf) {
            let path = parse_list_request(&buf[4..])?;
            Ok(ClientCommand::List { path })
        } else {
            let cmd = buf
                .split(|b| *b == b' ')
                .next()
                .and_then(|cmd| std::str::from_utf8(cmd).ok())
                .unwrap_or("");
            Err(CommandError::IllegalMethod(cmd.to_string()))
        }
    }

    fn is_put_request(buf: &[u8]) -> bool {
        let r: IResult<&[u8], &[u8]> = tag_no_case("PUT")(buf);
        r.is_ok()
    }

    fn is_get_request(buf: &[u8]) -> bool {
        let r: IResult<&[u8], &[u8]> = tag_no_case("GET")(buf);
        r.is_ok()
    }

    fn is_help_request(buf: &[u8]) -> bool {
        let r: IResult<&[u8], &[u8]> = tag_no_case("HELP")(buf);
        r.is_ok()
    }

    fn is_list_request(buf: &[u8]) -> bool {
        let r: IResult<&[u8], &[u8]> = tag_no_case("LIST")(buf);
        r.is_ok()
    }

    fn parse_put_request(buf: &[u8]) -> Result<(Vec<String>, usize), CommandError> {
        let words = split_words(buf)?;

        if words.len() != 2 {
            return Err(CommandError::PutUsage);
        }

        let path = parse_path(words[0])?;
        let len = usize::from_str_radix(words[1], 10).map_err(|_| CommandError::PutUsage)?;

        Ok((path, len))
    }

    fn parse_get_request(buf: &[u8]) -> Result<(Vec<String>, Option<usize>), CommandError> {
        // GET /path/to/file revision
        let words = split_words(buf)?;
        if words.is_empty() || words.len() > 2 {
            return Err(CommandError::GetUsage);
        }
        let path = parse_path(words[0])?;
        let rev = if words.len() == 2 {
            parse_revision(words[1])?
        } else {
            None
        };

        Ok((path, rev))
    }

    fn parse_list_request(buf: &[u8]) -> Result<Vec<String>, CommandError> {
        let words = split_words(buf)?;
        match words.first() {
            Some(w) => parse_path(w),
            None => Err(CommandError::ListUsage),
        }
    }

    fn split_words(buf: &[u8]) -> Result<Vec<&str>, CommandError> {
        // only supports roman a very limited subset of characters
        if !buf.iter().all(|b| {
            *b == b'/'
                || *b == b'.'
                || *b == b' '
                || *b == b'-'
                || *b == b'_'
                || (*b >= b'a' && *b <= b'z')
                || (*b >= b'A' && *b <= b'Z')
                || (*b >= b'0' && *b <= b'9')
        }) {
            return Err(CommandError::IllegalFilename);
        }

        let words = std::str::from_utf8(buf)
            .map_err(|_| CommandError::IllegalFilename)?
            .split_whitespace()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        Ok(words)
    }

    /// a valid path starts with a `/` and doesn't end with a `/`
    fn parse_path(buf: &str) -> Result<Vec<String>, CommandError> {
        if buf.chars().next() != Some('/') {
            return Err(CommandError::IllegalFilename);
        }

        if buf.len() == 1 {
            // root path: /
            return Ok(Vec::new());
        }

        let frags = buf
            .split('/')
            .map(|f| f.to_string())
            .skip(1)
            .collect::<Vec<_>>();
        if frags.iter().any(|f| f.is_empty()) {
            return Err(CommandError::IllegalFilename);
        }
        Ok(frags)
    }

    fn parse_revision(buf: &str) -> Result<Option<usize>, CommandError> {
        if buf.is_empty() {
            return Ok(None);
        }

        let word = if buf.chars().next() == Some('r') {
            &buf[1..]
        } else {
            buf
        };

        usize::from_str_radix(word, 10)
            .map(Some)
            .map_err(|_| CommandError::InvalidRevision(buf.to_string()))
    }

    // fn put(input: &str) -> Res<&str, ClientCommand> {
    //     let res = preceded(
    //         tag("PUT"),
    //         alt((
    //             map(
    //                 tuple((character::char(' '), path, character::char(' '), number)),
    //                 |(_, path, _, len)| ClientCommand::Put {
    //                     path: path.into_iter().map(|s| s.to_string()).collect(),
    //                     len,
    //                 },
    //             ),
    //             map_res(alt((take_until(" "), rest)), |_| {
    //                 Err(CommandError::PutUsage)
    //             }),
    //         )),
    //     )(input);

    //     match res {
    //         Err(nom::Err::Error(foo @ VcsParseError::Nom { .. })) => {
    //             println!("failure put conversion: {foo:?}");
    //             Err(nom::Err::Failure(VcsParseError::Cmd(
    //                 CommandError::PutUsage,
    //             )))
    //         }
    //         x => x,
    //     }

    //     // preceded(
    //     //     tag("PUT"),
    //     //     alt((
    //     //         map(
    //     //             tuple((character::char(' '), path, character::char(' '), number)),
    //     //             |(_, path, _, len)| ClientCommand::Put {
    //     //                 path: path.into_iter().map(|s| s.to_string()).collect(),
    //     //                 len,
    //     //             },
    //     //         ),
    //     //         map_res(nom::combinator::fail::<_, &str, _>, |_| {
    //     //             Err(CommandError::PutUsage)
    //     //         }),
    //     //     )),
    //     // )(input)
    // }

    // fn get(input: &str) -> Res<&str, ClientCommand> {
    //     let res = preceded(
    //         tag("GET"),
    //         map(
    //             tuple((character::char(' '), path, character::char(' '), revision)),
    //             |(_, path, _, rev)| ClientCommand::Get {
    //                 path: path.into_iter().map(|s| s.to_string()).collect(),
    //                 rev,
    //             },
    //         ),
    //     )(input);

    //     match res {
    //         Err(nom::Err::Error(foo @ VcsParseError::Nom { .. })) => {
    //             println!("failure get conversion: {foo:?}");
    //             Err(nom::Err::Failure(VcsParseError::Cmd(
    //                 CommandError::GetUsage,
    //             )))
    //         }
    //         x => x,
    //     }
    // }

    // fn illegal_method(_input: &str) -> Res<&str, ClientCommand> {
    //     Err(nom::Err::Failure(VcsParseError::Cmd(
    //         CommandError::IllegalFilename,
    //     )))
    // }

    // fn path(input: &str) -> Res<&str, Vec<&str>> {
    //     preceded(
    //         character::char('/'),
    //         separated_list0(character::char('/'), character::alphanumeric1),
    //     )(input)
    //     .map_err(|_: nom::Err<VcsParseError<&str>>| {
    //         nom::Err::Failure(VcsParseError::Cmd(CommandError::IllegalFilename))
    //     })
    // }

    // fn revision(input: &str) -> Res<&str, Option<usize>> {
    //     alt((
    //         map(number, Some),
    //         map(preceded(character::char('r'), number), Some),
    //         map(space0, |_| None),
    //     ))(input)
    // }

    // fn number(input: &str) -> Res<&str, usize> {
    //     map(character::u64, |x| x.try_into().unwrap())(input)
    // }

    #[cfg(test)]
    mod test {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn test_illegal_method() {
            let _ = tracing_subscriber::fmt::try_init();
            assert_eq!(
                parse_command(b"BLAH lol"),
                Err(CommandError::IllegalMethod("BLAH".to_string()))
            );
        }

        #[test]
        fn test_put_ok() {
            let _ = tracing_subscriber::fmt::try_init();
            assert_eq!(
                parse_command(b"PUT /aO-3kc/DftMiGvQKs2Y4d0IMEwr8Ipxh 10"),
                Ok(ClientCommand::Put {
                    path: vec![
                        "aO-3kc".to_string(),
                        "DftMiGvQKs2Y4d0IMEwr8Ipxh".to_string()
                    ],
                    len: 10
                })
            )
        }

        #[test]
        fn test_put_errs() {
            let _ = tracing_subscriber::fmt::try_init();
            assert_eq!(parse_command(b"PUT"), Err(CommandError::PutUsage));
            assert_eq!(parse_command(b"PUT blah"), Err(CommandError::PutUsage));
            assert_eq!(
                parse_command(b"PUT blah 10"),
                Err(CommandError::IllegalFilename),
                "path must start with a /"
            );
            assert_eq!(
                parse_command(b"PUT ///-icUt84AtB 10"),
                Err(CommandError::IllegalFilename),
                "no empty fragments in the path"
            );
        }

        #[test]
        fn test_get() {
            let _ = tracing_subscriber::fmt::try_init();
            assert_eq!(
                parse_command(b"GET /foo/bar r1"),
                Ok(ClientCommand::Get {
                    path: vec!["foo".to_string(), "bar".to_string()],
                    rev: Some(1)
                }),
                "revision as r1"
            );

            assert_eq!(
                parse_command(b"GET /foo/bar 1"),
                Ok(ClientCommand::Get {
                    path: vec!["foo".to_string(), "bar".to_string()],
                    rev: Some(1)
                }),
                "revision as single number"
            );

            assert_eq!(
                parse_command(b"GET /foo/bar"),
                Ok(ClientCommand::Get {
                    path: vec!["foo".to_string(), "bar".to_string()],
                    rev: None
                }),
                "missing revision"
            );
        }

        #[test]
        fn test_get_errs() {
            let _ = tracing_subscriber::fmt::try_init();
            assert_eq!(parse_command(b"GET"), Err(CommandError::GetUsage));
            assert_eq!(
                parse_command(b"GET lol"),
                Err(CommandError::IllegalFilename)
            );
        }

        #[test]
        fn test_list() {
            let _ = tracing_subscriber::fmt::try_init();
            assert_eq!(
                parse_command(b"LIST /foo/bar"),
                Ok(ClientCommand::List {
                    path: vec!["foo".to_string(), "bar".to_string()],
                }),
                "list for dir"
            );
        }

        #[test]
        fn test_list_root() {
            let _ = tracing_subscriber::fmt::try_init();
            assert_eq!(
                parse_command(b"LIST /"),
                Ok(ClientCommand::List { path: Vec::new() }),
                "list for root fs"
            );
        }
    }
}

struct Server {
    fs: InMemFs,
    listener: TcpListener,
    server_tx: mpsc::Sender<VcsCommand>,
    server_rx: mpsc::Receiver<VcsCommand>,
}

impl Server {
    async fn bind<A: ToSocketAddrs + std::fmt::Debug + Copy>(addr: A) -> BoxResult<Self> {
        let listener = TcpListener::bind(addr).await?;
        let (server_tx, server_rx) = mpsc::channel(1);
        tracing::info!("Listening on address {:?}", addr);
        Ok(Server {
            fs: Default::default(),
            listener,
            server_tx,
            server_rx,
        })
    }

    async fn run(&mut self) -> BoxResult<()> {
        loop {
            tokio::select! {
                x = self.listener.accept() => {
                    let (stream, addr) = x?;
                    tracing::info!("new client connecting from {addr}");
                    Client::new(stream, addr, self.server_tx.clone()).spawn_run();
                }
                Some(cmd) = self.server_rx.recv() => {
                    self.handle_command(cmd)?;
                }
            }
        }
    }

    fn handle_command(&mut self, cmd: VcsCommand) -> BoxResult<()> {
        match cmd {
            VcsCommand::Put { path, data, resp } => {
                let rev = self.fs.put_file(path, data);
                yolo_send(resp, rev);
            }
            VcsCommand::Get {
                path,
                revision,
                resp,
            } => {
                let data = self.fs.get_file(path, revision);
                yolo_send(resp, data);
            }
            VcsCommand::List { path, resp } => {
                let results = self.fs.list_files(path);
                yolo_send(resp, results);
            }
        }
        Ok(())
    }
}

struct Client {
    stream: TcpStream,
    peer_addr: SocketAddr,
    server_tx: mpsc::Sender<VcsCommand>,
}

impl Client {
    fn new(stream: TcpStream, peer_addr: SocketAddr, server_tx: mpsc::Sender<VcsCommand>) -> Self {
        Client {
            stream,
            peer_addr,
            server_tx,
        }
    }

    fn spawn_run(self) {
        tokio::spawn(async move {
            let addr = self.peer_addr.clone();
            match self.run().await {
                Ok(_) => tracing::info!("disconnecting peer {:?}", addr),
                Err(err) => tracing::info!("peer at {addr} terminated with an error: {err:?}"),
            }
        });
    }

    async fn run(mut self) -> BoxResult<()> {
        let (rdr, wrt) = self.stream.split();
        let mut rdr = BufReader::new(rdr);
        let mut wrt = BufWriter::new(wrt);
        let mut buf = Vec::with_capacity(100);
        loop {
            buf.clear();
            wrt.write_all(b"READY\n").await?;
            wrt.flush().await?;
            let mut n = rdr.read_until(b'\n', &mut buf).await?;
            if n == 0 {
                break;
            }
            if buf[buf.len() - 1] == b'\n' {
                buf.pop();
                n -= 1;
            }

            match parser::parse_command(&buf[..n]) {
                Ok(ClientCommand::Put { path, len }) => {
                    // a path can be just `/`, but that is forbidden for PUT
                    if path.is_empty() {
                        let err = CommandError::IllegalFilename;
                        wrt.write_all(format!("{err}\n").as_bytes()).await?;
                        wrt.flush().await?;
                        continue;
                    }

                    tracing::debug!("Put {len} bytes at {path:?}");
                    let data = if len > 0 {
                        let mut data = vec![0; len];
                        rdr.read_exact(&mut data).await?;
                        // only accept limited text data
                        if !data
                            .iter()
                            .all(|b| *b == b'\n' || *b == b'\t' || (*b >= 32 && *b < 127))
                        {
                            // if let Err(_) = std::str::from_utf8(&data) {
                            tracing::debug!("rejecting because data isn't text");
                            let err = CommandError::NonTextContent;
                            wrt.write_all(format!("{err}\n").as_bytes()).await?;
                            wrt.flush().await?;
                            continue;
                        }
                        data
                    } else {
                        Vec::new()
                    };
                    let (tx, resp) = oneshot::channel();
                    self.server_tx
                        .send(VcsCommand::Put {
                            path,
                            data,
                            resp: tx,
                        })
                        .await?;
                    let revision = resp.await?;
                    wrt.write_all(format!("OK r{revision}\n").as_bytes())
                        .await?;
                    wrt.flush().await?;
                }
                Ok(ClientCommand::Get { path, rev }) => {
                    tracing::info!("GET {path:?} at rev {rev:?}");
                    let (tx, resp) = oneshot::channel();
                    self.server_tx
                        .send(VcsCommand::Get {
                            path,
                            revision: rev,
                            resp: tx,
                        })
                        .await?;
                    // technically, we could stream stuff, but here we don't care since
                    // everything is in memory anyway
                    let content = resp.await?;
                    match content {
                        Some(bs) => {
                            wrt.write_all(format!("OK {}\n", bs.len()).as_bytes())
                                .await?;
                            wrt.write_all(&bs).await?;
                            wrt.flush().await?;
                        }
                        None => {
                            wrt.write_all(b"ERR no such file\n").await?;
                            wrt.flush().await?;
                        }
                    }
                }
                Ok(ClientCommand::List { path }) => {
                    tracing::info!("LIST {path:?}");
                    let (tx, resp) = oneshot::channel();
                    self.server_tx
                        .send(VcsCommand::List { path, resp: tx })
                        .await?;
                    let results = resp.await?;
                    wrt.write_all(format!("OK {}\n", results.len()).as_bytes())
                        .await?;
                    for (filename, metadata) in results {
                        let metadata = metadata
                            .map(|rev| format!("r{rev}"))
                            .unwrap_or("DIR".to_string());
                        wrt.write_all(format!("{filename} {metadata}\n").as_bytes())
                            .await?;
                    }
                    wrt.flush().await?;
                }
                Ok(ClientCommand::Help) => {
                    wrt.write_all(b"OK usage: HELP|GET|PUT|LIST\n").await?;
                    wrt.flush().await?;
                }
                Err(CommandError::IllegalMethod(method)) => {
                    wrt.write_all(b"ERR illegal method: ").await?;
                    wrt.write_all(method.as_bytes()).await?;
                    wrt.flush().await?;
                    break;
                }
                Err(err) => {
                    wrt.write_all(format!("{err}\n").as_bytes()).await?;
                    wrt.flush().await?;
                    continue;
                }
            }
        }
        wrt.into_inner().shutdown().await?;
        Ok(())
    }
}

#[derive(Default)]
struct InMemFs {
    root: BTreeMap<String, FsTree>,
}

impl InMemFs {
    fn put_file(&mut self, path: Vec<String>, data: Vec<u8>) -> usize {
        let mut frags = path.iter();
        let mut entry = self
            .root
            .entry(frags.next().unwrap().to_string())
            .or_default();
        for frag in frags {
            entry = entry.nodes.entry(frag.to_string()).or_default();
        }

        match entry.file.as_mut() {
            Some(f) => f.update(data),
            None => entry.file = Some(VcsFile::new(data)),
        };

        entry.file.as_ref().unwrap().current_revision()
    }

    fn get_file(&self, path: Vec<String>, revision: Option<usize>) -> Option<Vec<u8>> {
        let mut file = None;
        let mut nodes = &self.root;
        for frag in path {
            match nodes.get(&frag) {
                Some(t) => {
                    file = t.file.as_ref();
                    nodes = &t.nodes;
                }
                None => break,
            }
        }

        file.and_then(|f| f.get_at_rev(revision))
    }

    fn list_files(&self, path: Vec<String>) -> Vec<(String, Option<usize>)> {
        let mut nodes = &self.root;
        for frag in path {
            match nodes.get(&frag) {
                Some(t) => {
                    nodes = &t.nodes;
                }
                None => break,
            }
        }
        let mut results = Vec::new();
        for (k, t) in nodes.iter() {
            match t.file.as_ref() {
                Some(f) => results.push((k.to_string(), Some(f.current_revision()))),
                None => results.push((format!("{k}/"), None)),
            }
        }
        results
    }
}

#[derive(Default)]
struct FsTree {
    // it is possible to have a file at /foo, and /foo also being a directory
    file: Option<VcsFile>,
    nodes: BTreeMap<String, FsTree>,
}

// impl FsTree {
//     fn new() -> Self {
//         FsTree {
//             file: todo!(),
//             nodes: todo!(),
//         }
//     }
// }

/// A file stored in the vcs. Only the latest version is fully stored, previous
/// revisions can be retrieved by reconstructing from the stored operations
struct VcsFile {
    current: Vec<u8>,
    revisions: Vec<Vec<ReconstructOp>>,
}

impl VcsFile {
    fn new(data: Vec<u8>) -> Self {
        Self {
            current: data,
            revisions: Vec::new(),
        }
    }

    fn update(&mut self, new_version: Vec<u8>) {
        if self.current != new_version {
            let ops = diff_update(&new_version, &self.current);
            self.revisions.push(ops);
            self.current = new_version;
        }
    }

    fn current_revision(&self) -> usize {
        self.revisions.len() + 1
    }

    fn get_at_rev(&self, rev: Option<usize>) -> Option<Vec<u8>> {
        if rev == Some(0) {
            return None;
        }
        let rev = rev.unwrap_or_else(|| self.current_revision());
        match rev.cmp(&self.current_revision()) {
            std::cmp::Ordering::Greater => None,
            std::cmp::Ordering::Equal => Some(self.current.clone()),
            std::cmp::Ordering::Less => {
                let diffs_to_apply = self.current_revision() - rev;
                let mut file = self.current.clone();
                for i in 0..diffs_to_apply {
                    let idx = self.revisions.len() - i - 1;
                    let ops = self.revisions.get(idx).unwrap();
                    file = reconstruct(&file, &ops);
                }
                Some(file)
            }
        }
    }
}

#[derive(Debug)]
enum ReconstructOp {
    Equal { old_index: usize, len: usize },
    Insert { new_str: Vec<u8> },
}

/// returns the newest String and the diffs to get back the old string
fn diff_update(old: &[u8], new: &[u8]) -> Vec<ReconstructOp> {
    let deadline = Instant::now() + Duration::from_millis(100);
    // let (old_idx, _new_idx, mut ops) = TextDiff::configure()
    //     .algorithm(Algorithm::Patience)
    //     .deadline(deadline)
    //     .newline_terminated(true)
    //     .diff_chars(old, new)
    //     .grouped_ops(20)
    //     .into_iter()
    //     .flatten()

    let (old_idx, _new_idx, mut ops) =
        similar::capture_diff_slices_deadline(Algorithm::Myers, old, new, Some(deadline))
            .into_iter()
            // .map(|d| {
            //     println!("diff: {:?}", d);
            //     d
            // })
            .fold(
                (0, 0, Vec::new()),
                |(prev_old_idx, _new_idx, mut ops), diff| {
                    let (old_idx, _) = get_idx(&diff);
                    if prev_old_idx < old_idx {
                        ops.push(ReconstructOp::Equal {
                            old_index: prev_old_idx,
                            len: old_idx - prev_old_idx,
                        });
                    };

                    match diff {
                        similar::DiffOp::Equal {
                            old_index,
                            new_index,
                            len,
                        } => {
                            ops.push(ReconstructOp::Equal { old_index, len });
                            (old_index + len, new_index + len, ops)
                        }
                        similar::DiffOp::Delete {
                            old_index,
                            old_len,
                            new_index,
                        } => (old_index + old_len, new_index, ops),
                        similar::DiffOp::Insert {
                            old_index,
                            new_index,
                            new_len,
                        } => {
                            ops.push(ReconstructOp::Insert {
                                new_str: new[new_index..new_index + new_len].to_vec(),
                            });
                            (old_index, new_index + new_len, ops)
                        }
                        similar::DiffOp::Replace {
                            old_index,
                            old_len,
                            new_index,
                            new_len,
                        } => {
                            ops.push(ReconstructOp::Insert {
                                // old_index: new_index,
                                // len: new_len,
                                new_str: new[new_index..new_index + new_len].to_vec(),
                            });
                            (old_index + old_len, new_index + new_len, ops)
                        }
                    }
                },
            );

    // if the ends of the two strings after the diffs are the same,
    // there will be no diff at all, so we need to add one in order
    // to reconstruct the tail.
    if old_idx < old.len() {
        ops.push(ReconstructOp::Equal {
            old_index: old_idx,
            len: old.len() - old_idx,
        })
    }

    ops
}

fn get_idx(op: &similar::DiffOp) -> (usize, usize) {
    match op {
        similar::DiffOp::Equal {
            old_index,
            new_index,
            len: _,
        } => (*old_index, *new_index),
        similar::DiffOp::Delete {
            old_index,
            old_len: _,
            new_index,
        } => (*old_index, *new_index),
        similar::DiffOp::Insert {
            old_index,
            new_index,
            new_len: _,
        } => (*old_index, *new_index),
        similar::DiffOp::Replace {
            old_index,
            old_len: _,
            new_index,
            new_len: _,
        } => (*old_index, *new_index),
    }
}

fn reconstruct(base: &[u8], ops: &[ReconstructOp]) -> Vec<u8> {
    let mut result = Vec::new();
    for op in ops {
        match op {
            ReconstructOp::Equal { old_index, len } => {
                result.extend_from_slice(&base[*old_index..*old_index + len])
            }
            ReconstructOp::Insert { new_str } => result.extend_from_slice(new_str),
        }
    }
    result
}

trait TotalMem {
    /// Returns the total number of bytes used for this object.
    /// That may not correspond exactly to the actual memory consumed
    /// because of alignment
    fn total_mem(&self) -> usize;
}

impl TotalMem for String {
    fn total_mem(&self) -> usize {
        std::mem::size_of_val(self) + self.as_bytes().len()
    }
}

impl TotalMem for ReconstructOp {
    fn total_mem(&self) -> usize {
        let base = std::mem::size_of_val(self);
        match self {
            ReconstructOp::Equal { .. } => base + 2 * std::mem::size_of::<usize>(),
            ReconstructOp::Insert { new_str } => {
                base + std::mem::size_of::<String>() + new_str.len()
            }
        }
    }
}

impl TotalMem for VcsFile {
    fn total_mem(&self) -> usize {
        let s_size = std::mem::size_of_val(&self.current) + self.current.len();
        let ops_size = std::mem::size_of_val(&self.revisions)
            + self
                .revisions
                .iter()
                .map(|ops| {
                    std::mem::size_of_val(ops) + ops.iter().map(|op| op.total_mem()).sum::<usize>()
                })
                .sum::<usize>();

        s_size + ops_size
    }
}

#[cfg(test)]
mod test {
    use crate::utils::AbortHdl;

    use super::*;
    use pretty_assertions::assert_eq;
    use tokio::{io::AsyncBufRead, time::timeout};

    #[async_trait::async_trait]
    trait TestClient: AsyncBufRead {
        async fn read_n_lines(&mut self, buf: &mut String, n: usize) -> std::io::Result<usize>;
    }

    #[async_trait::async_trait]
    impl<T> TestClient for T
    where
        T: AsyncBufRead + Send + Unpin,
    {
        async fn read_n_lines(&mut self, mut buf: &mut String, n: usize) -> std::io::Result<usize> {
            let mut read = 0;
            for _ in 0..n {
                read += self.read_line(&mut buf).await?;
            }
            Ok(read)
        }
    }

    #[test]
    fn test_reconstruct() {
        let s1 = b"coucou";
        let s2 = b"coublah";
        let ops = diff_update(s2, s1);
        let reconstructed = reconstruct(s2, &ops);
        assert_eq!(s1, reconstructed.as_slice());
    }

    #[test]
    fn test_reconstruct_flipped() {
        let s1 = b"coucouh";
        let s2 = b"coubla";
        let ops = diff_update(s2, s1);
        let reconstructed = reconstruct(s2, &ops);
        assert_eq!(s1, reconstructed.as_slice());
    }

    #[test]
    fn test_reconstruct_delete() {
        let s1 = b"";
        let s2 = b"coucou blah";
        let ops = diff_update(s2, s1);
        let reconstructed = reconstruct(s2, &ops);
        assert_eq!(s1, reconstructed.as_slice());
    }

    #[test]
    fn test_reconstruct_equals() {
        let s1 = b"the quick brown fox jumps over the lazy dog";
        let s2 = b"the quick brown fox jumps over the lazy dog";
        let ops = diff_update(s2, s1);
        let reconstructed = reconstruct(s2, &ops);
        assert_eq!(s1, reconstructed.as_slice());
    }

    #[test]
    fn test_reconstruct_random() {
        let s1 = b"hello etausrnetausnretaurnsetuadljetublahualdtdaeluateldjauteldau coucou";
        let s2 =
            b"blah etausrnetausnretaurnsetuadljetublahualdtdaeluateldjauteldau hello coumoo world!";
        let ops = diff_update(s2, s1);
        let reconstructed = reconstruct(s2, &ops);
        assert_eq!(s1, reconstructed.as_slice());
    }

    #[test]
    fn test_file_ops() {
        let mut f = VcsFile::new(b"coucou".to_vec());
        assert_eq!(f.current_revision(), 1, "first revision is 1");
        f.update(b"scratch that.".to_vec());
        f.update(b"scratch that itch.".to_vec());
        assert_eq!(f.current_revision(), 3);
        assert_eq!(None, f.get_at_rev(Some(4)));
        assert_eq!(
            Some(Ok("scratch that itch.")),
            f.get_at_rev(Some(3))
                .as_ref()
                .map(|b| std::str::from_utf8(b))
        );
        assert_eq!(
            Some(Ok("scratch that.")),
            f.get_at_rev(Some(2))
                .as_ref()
                .map(|b| std::str::from_utf8(b))
        );
        assert_eq!(
            Some(Ok("coucou")),
            f.get_at_rev(Some(1))
                .as_ref()
                .map(|b| std::str::from_utf8(b))
        );
        assert_eq!(None, f.get_at_rev(Some(0)));
    }

    #[test]
    fn test_inmem_fs() {
        let mut fs = InMemFs::default();
        let path = vec!["foo".to_string(), "bar".to_string()];
        let rev = fs.put_file(path.clone(), b"foobar".to_vec());
        assert_eq!(rev, 1);
        let rev2 = fs.put_file(path.clone(), b"new version of foobar".to_vec());
        assert_eq!(rev2, 2);
        assert_eq!(
            1,
            fs.put_file(
                vec!["other".to_string(), "path".to_string()],
                b"coucou".to_vec()
            )
        );

        let last_content = fs.get_file(path, None);
        assert_eq!(last_content, Some(b"new version of foobar".to_vec()));
    }

    #[test]
    fn test_no_new_rev_if_identical_content() {
        let mut fs = InMemFs::default();
        let path = vec!["foo".to_string(), "bar".to_string()];
        let rev = fs.put_file(path.clone(), b"foobar".to_vec());
        assert_eq!(rev, 1);
        let rev2 = fs.put_file(path.clone(), b"foobar".to_vec());
        assert_eq!(rev2, 1, "no new revision given if content is identical");

        let last_content = fs.get_file(path, None);
        assert_eq!(last_content, Some(b"foobar".to_vec()));
    }

    async fn start_server() -> BoxResult<(SocketAddr, AbortHdl<()>)> {
        let _ = tracing_subscriber::fmt::try_init();
        let mut server = Server::bind("127.0.0.1:0").await?;
        let addr = server.listener.local_addr().unwrap();
        let hdl = AbortHdl(tokio::spawn(async move { server.run().await.unwrap() }));
        Ok((addr, hdl))
    }

    async fn swallow_ready<R: AsyncBufRead + Unpin>(rdr: &mut R) -> BoxResult<bool> {
        let mut buf = String::new();
        timeout(Duration::from_millis(50), rdr.read_line(&mut buf))
            .await
            .map_err(|_| "expect ready")??;
        if buf != "READY\n" {
            Ok(false)
        } else {
            Ok(true)
        }
    }

    #[tokio::test]
    async fn test_unknown_command() -> BoxResult<()> {
        let (addr, _server) = start_server().await?;
        let mut stream = TcpStream::connect(addr).await?;
        stream.write_all(b"coucou\n").await?;
        stream.flush().await?;
        let mut buf = Vec::new();
        timeout(Duration::from_millis(5), stream.read_to_end(&mut buf))
            .await
            .expect("peer disconnects")?;
        assert_eq!(
            std::str::from_utf8(&buf),
            Ok("READY\nERR illegal method: coucou")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_put_invalid() -> BoxResult<()> {
        let (addr, _server) = start_server().await?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = BufReader::new(stream);
        let mut buf = String::new();

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all("PUT\n".as_bytes()).await?;
        stream.flush().await?;

        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to an empty PUT")?;
        assert_eq!(buf, "ERR usage: PUT file length newline data\n");

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all("PUT /écoucou 3\n".as_bytes()).await?;
        stream.flush().await?;
        buf.clear();
        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to invalid put filename")?;
        assert_eq!(buf, "ERR illegal file name\n");

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all("PUT /coucou\n".as_bytes()).await?;
        stream.flush().await?;
        buf.clear();
        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to missing length")?;
        assert_eq!(buf, "ERR usage: PUT file length newline data\n");

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all("PUT / 4\n".as_bytes()).await?;
        stream.flush().await?;
        buf.clear();
        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to PUT /")?;
        assert_eq!(buf, "ERR illegal file name\n");
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_puts() -> BoxResult<()> {
        let (addr, _server) = start_server().await?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = BufReader::new(stream);
        let mut buf = String::new();

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all(b"PUT /foo/bar/coucou 4\nfoo\n").await?;
        stream.flush().await?;
        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to first put")?;
        assert_eq!(buf, "OK r1\n", "first revision stored");
        buf.clear();

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all(b"PUT /foo/bar/coucou 6\nfoo 2\n").await?;
        stream.flush().await?;
        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to second put")?;
        assert_eq!(buf, "OK r2\n", "new revision");
        buf.clear();

        Ok(())
    }

    #[tokio::test]
    async fn test_put_get() -> BoxResult<()> {
        let (addr, _server) = start_server().await?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = BufReader::new(stream);
        let mut buf = String::new();

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all(b"PUT /foo/bar/coucou 4\nfoo\n").await?;
        stream.flush().await?;
        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to first put")?;
        assert_eq!(buf, "OK r1\n", "first revision stored");
        buf.clear();

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all(b"PUT /foo/bar/coucou 6\nfoo 2\n").await?;
        stream.flush().await?;
        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to second put")?;
        assert_eq!(buf, "OK r2\n", "new revision");
        buf.clear();

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all(b"GET /foo/bar/coucou 1\n").await?;
        stream.flush().await?;
        timeout(Duration::from_millis(50), stream.read_n_lines(&mut buf, 2))
            .await
            .expect("get a response to first get")?;
        assert_eq!(buf, "OK 4\nfoo\n", "get initial file");
        buf.clear();

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all(b"GET /foo/bar/coucou r2\n").await?;
        stream.flush().await?;
        timeout(Duration::from_millis(50), stream.read_n_lines(&mut buf, 2))
            .await
            .expect("get a response to second get")?;
        assert_eq!(buf, "OK 6\nfoo 2\n", "get second revision");

        Ok(())
    }

    #[tokio::test]
    async fn test_list() -> BoxResult<()> {
        let (addr, _server) = start_server().await?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = BufReader::new(stream);
        let mut buf = String::new();

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all(b"PUT /foo/bar/coucou 4\nfoo\n").await?;
        stream.flush().await?;
        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to first put")?;
        assert_eq!(buf, "OK r1\n", "first file stored");
        buf.clear();

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all(b"PUT /foo/bar/blah 4\nbar\n").await?;
        stream.flush().await?;
        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to second put")?;
        assert_eq!(buf, "OK r1\n", "second file stored");
        buf.clear();

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all(b"LIST /foo/bar\n").await?;
        stream.flush().await?;
        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to LIST put")?;
        assert_eq!(buf, "OK 2\n", "2 files stored");
        buf.clear();

        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to LIST put second line")?;
        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to LIST put third line")?;
        assert_eq!(buf, "blah r1\ncoucou r1\n", "2 files stored, both at rev 1");
        buf.clear();

        Ok(())
    }

    #[tokio::test]
    async fn test_list_dir() -> BoxResult<()> {
        let (addr, _server) = start_server().await?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = BufReader::new(stream);
        let mut buf = String::new();

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all(b"PUT /foo/bar/coucou 4\nfoo\n").await?;
        stream.flush().await?;
        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to first put")?;
        assert_eq!(buf, "OK r1\n", "first file stored");
        buf.clear();

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all(b"PUT /bar 4\nbar\n").await?;
        stream.flush().await?;
        timeout(Duration::from_millis(50), stream.read_line(&mut buf))
            .await
            .expect("get a response to second put")?;
        assert_eq!(buf, "OK r1\n", "first file stored");
        buf.clear();

        assert!(swallow_ready(&mut stream).await?);
        stream.write_all(b"LIST /\n").await?;
        stream.flush().await?;
        timeout(Duration::from_millis(50), stream.read_n_lines(&mut buf, 3))
            .await
            .expect("get a response to LIST root dir")?;
        assert_eq!(buf, "OK 2\nbar r1\nfoo/ DIR\n", "2 files stored");
        buf.clear();

        Ok(())
    }
}
