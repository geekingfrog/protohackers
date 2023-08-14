use serde::{Deserialize, Serialize};
use serde_json as json;
use std::{
    collections::{binary_heap::PeekMut, BTreeMap, BTreeSet, BinaryHeap, HashMap},
    future::Future,
    net::SocketAddr,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream, ToSocketAddrs,
    },
    sync::{broadcast, mpsc, oneshot},
};

use crate::utils::{BoxResult, BoxError, yolo_send};


pub async fn main(addr: SocketAddr) -> BoxResult<()> {
    Server::bind(addr).await?.run().await?;
    Ok(())
}

struct Server {
    listener: TcpListener,
    queues: HashMap<String, Queue>,
    jobs: BTreeMap<JobId, Job>,
    job_id_counter: u64,
    client_id_counter: u64,
    // keep track of jobs that have been deleted since removing
    // an element from a binaryheap is quite expensive, do that when retrieving
    // element, and poping deleted job at that point in time.
    tombstones: BTreeSet<JobId>,

    waiters: Waiters,

    // to receive messages from the client
    server_rx: mpsc::Receiver<ClientRequest>,
    server_tx: mpsc::Sender<ClientRequest>,

    // to broadcast to all client that a job has been deleted
    broadcast_tx: broadcast::Sender<JobId>,
}

impl Server {
    async fn bind<A: ToSocketAddrs + std::fmt::Display + Copy>(addr: A) -> BoxResult<Self> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("server listening on {}", addr);
        Ok(Server::new(listener))
    }

    fn new(listener: TcpListener) -> Self {
        let (server_tx, server_rx) = mpsc::channel(1);
        let (broadcast_tx, _) = broadcast::channel(500);
        let server = Self {
            listener,
            queues: Default::default(),
            job_id_counter: 0,
            client_id_counter: 0,
            jobs: Default::default(),
            tombstones: Default::default(),
            waiters: Default::default(),
            server_tx,
            server_rx,
            broadcast_tx,
        };
        server
    }

    async fn run(mut self) -> BoxResult<()> {
        loop {
            tokio::select! {
                x = self.listener.accept() => {
                    let (stream, addr) = x?;
                    self.spawn_client(stream, addr, ClientId(self.client_id_counter));
                    self.client_id_counter += 1;
                    let r: BoxResult<()> = Ok(());
                    r
                },
                client_req = self.server_rx.recv() => {
                    // the channel cannot ever be closed since the server also owns
                    // a Sender. As long as the receiver exists, so will at least one sender
                    let client_req = client_req.unwrap();
                    self.process_client_request(client_req).await?;
                    Ok(())
                }
            }?
        }
    }

    fn spawn_client(&mut self, stream: TcpStream, addr: SocketAddr, id: ClientId) {
        tracing::info!("spawning a new client from {addr} with id {id:?}");
        Client::new(
            stream,
            id,
            self.server_tx.clone(),
            self.broadcast_tx.subscribe(),
        )
        .spawn_run();
    }

    async fn process_client_request(&mut self, req: ClientRequest) -> BoxResult<()> {
        match req {
            ClientRequest::PutJob {
                queue,
                job,
                pri,
                resp,
            } => {
                let job_id = self.put_job(queue, pri, job);
                yolo_send(resp, job_id);
                Ok(())
            }
            ClientRequest::GetJob { queues, resp } => self.get_job(queues, resp).await,
            ClientRequest::DelJob { id, resp } => self.delete_job(id, resp).await,
            ClientRequest::AbortJobs { jobs } => {
                self.abort_jobs(jobs);
                Ok(())
            }
        }
    }

    fn put_job(&mut self, queue: String, pri: Priority, job: json::Value) -> JobId {
        let job_id = JobId(self.job_id_counter);
        self.job_id_counter += 1;
        let job = Job {
            id: job_id,
            job,
            queue,
            pri,
        };
        self.enqueue_job(job);
        job_id
    }

    fn enqueue_job(&mut self, job: Job) {
        tracing::info!(
            "saving new job {:?} on queue {} with priority {:?}",
            job.id,
            job.queue,
            job.pri
        );

        self.jobs.insert(job.id, job.clone());

        if let Some(tx) = self.waiters.pop_for_queue(&job.queue) {
            tracing::debug!("unparking job {:?} for queue {}", job.id, job.queue);
            let response = Job {
                id: job.id,
                job: job.job,
                queue: job.queue,
                pri: job.pri,
            };
            yolo_send(tx, Ok(response));
        } else {
            self.queues
                .entry(job.queue.clone())
                .or_default()
                .put_job(job.pri, job.id);
        }
    }

    async fn get_job(&mut self, queues: Vec<String>, resp: GetJobResp) -> BoxResult<()> {
        let candidates = queues.iter().filter_map(|q| {
            self.queues
                .get_mut(q)
                .map(|ref mut queue| (q, queue.peek_and_clean(&self.tombstones)))
        });

        let mb_job = candidates
            .into_iter()
            .filter_map(|(q, mb_job)| match mb_job {
                None => None,
                Some((pri, job_id)) => Some((q, pri, job_id)),
            })
            .max_by_key(|(_, pri, _)| *pri);

        match (mb_job, resp) {
            (Some((q_name, _pri, job_id)), r) => {
                self.queues.get_mut(q_name).unwrap().pop();
                match self.jobs.get(&job_id) {
                    None => {
                        tracing::error!("Got a job from the queues but not present in the set of jobs! {job_id:?}");
                        r.send_err("server crashed".into());
                    }
                    Some(job) => r.send_job(job.clone()),
                };
            }
            (None, GetJobResp::Now(tx)) => yolo_send(tx, Ok(None)),
            (None, GetJobResp::Wait(tx)) => {
                tracing::debug!("request parked until got a job on queues {:?}", queues);
                self.waiters.add(queues, tx);
            }
        };

        Ok(())
    }

    async fn delete_job(&mut self, id: JobId, resp: oneshot::Sender<bool>) -> BoxResult<()> {
        self.tombstones.insert(id);
        let existing_job = self.jobs.remove(&id).is_some();
        tracing::info!("deleting job {id:?}, existing job? {existing_job}");
        if let Err(err) = self.broadcast_tx.send(id) {
            tracing::warn!("error broadcasting deleted job {id:?}: {err:?}");
        }
        yolo_send(resp, existing_job);
        Ok(())
    }

    fn abort_jobs(&mut self, jobs: Vec<Job>) {
        for job in jobs {
            self.enqueue_job(job)
        }
    }
}

#[derive(Debug, Clone)]
struct Job {
    id: JobId,
    job: json::Value,
    queue: String,
    pri: Priority,
}

#[derive(Default)]
struct Queue {
    q: BinaryHeap<(Priority, JobId)>,
}

impl Queue {
    fn put_job(&mut self, priority: Priority, job_id: JobId) {
        self.q.push((priority, job_id))
    }

    /// Returns the highest priority job that hasn't been deleted yet.
    /// This doesn't pop the job out of the queue
    fn peek_and_clean(&mut self, tombstones: &BTreeSet<JobId>) -> Option<(Priority, JobId)> {
        loop {
            match self.q.peek_mut() {
                None => break None,
                Some(entry) => {
                    if tombstones.contains(&entry.1) {
                        // PeekM
                        // self.q.pop();
                        PeekMut::pop(entry);
                        continue;
                    } else {
                        break Some((entry.0, entry.1));
                    }
                }
            }
        }
    }

    /// may return a deleted job, should be paired with peek_and_clean before
    fn pop(&mut self) -> Option<(Priority, JobId)> {
        self.q.pop()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq, Clone, Copy)]
struct JobId(u64);

impl std::convert::From<u64> for JobId {
    fn from(value: u64) -> Self {
        JobId(value)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct Priority(u64);

impl std::convert::From<u64> for Priority {
    fn from(value: u64) -> Self {
        Priority(value)
    }
}

#[derive(Debug, Clone, Copy)]
struct ClientId(u64);

struct Client {
    lines: Lines<BufReader<OwnedReadHalf>>,
    wrt: OwnedWriteHalf,
    id: ClientId,
    assigned_jobs: BTreeMap<JobId, Job>,
    server_chan: mpsc::Sender<ClientRequest>,
    deleted_job_chan: broadcast::Receiver<JobId>,
    waiter: Option<oneshot::Receiver<BoxResult<Job>>>,
    // we can't only rely on waiter.is_some() to check if we're waiting for a job
    // because in order to await the future in the option, we call `take`, thus
    // making the waiter None, but we're still waiting
    waiting_for_job: bool,
}

impl Client {
    fn new(
        stream: TcpStream,
        id: ClientId,
        server_chan: mpsc::Sender<ClientRequest>,
        deleted_job_chan: broadcast::Receiver<JobId>,
    ) -> Self {
        let (rdr, wrt) = stream.into_split();
        let lines = BufReader::new(rdr).lines();
        Self {
            lines,
            wrt,
            id,
            assigned_jobs: Default::default(),
            server_chan,
            deleted_job_chan,
            waiter: None,
            waiting_for_job: false,
        }
    }

    fn spawn_run(mut self) {
        tokio::spawn(async move {
            let id = self.id;
            match self.run().await {
                Ok(_) => tracing::info!("client {:?} disconnected", id),
                Err(err) => tracing::error!("client {:?} errored: {err:?}", id),
            };
            if let Err(err) = self.abort_jobs().await {
                tracing::error!("error while aborting all jobs: {err:?}");
            }
        });
    }

    async fn run(&mut self) -> BoxResult<()> {
        loop {
            tokio::select! {
                // we do want to check first for deleted job in order to avoid clogging
                // the broadcast channel and missing events
                biased;
                job_id = self.deleted_job_chan.recv() => {
                    self.assigned_jobs.remove(&job_id?);
                },
                Some(job_result) = mb_await(&mut self.waiter) => {
                    self.get_job_response(job_result?).await?;
                },

                // don't process request if we're waiting for a job
                mb_line = self.lines.next_line(), if !self.waiting_for_job => {
                    match mb_line? {
                        None => break, // EOF, peer went away
                        Some(line) => self.process_request(line).await?,
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_request(&mut self, line: String) -> BoxResult<()> {
        let req: Request = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(err) => {
                tracing::debug!("received invalid request: {err:?}");
                let resp = Response::Error {
                    error: format!("{err:?}"),
                };
                self.send_resp(&resp).await?;
                return Ok(());
            }
        };
        tracing::trace!("processing request: {req:?}");
        match req {
            Request::Put { queue, job, pri } => self.put_job(queue, job, pri).await?,
            Request::Get { queues, wait } => self.get_job(queues, wait).await?,
            Request::Delete { id } => self.delete_job(id).await?,
            Request::Abort { id } => self.abort_job(id).await?,
        };
        Ok(())
    }

    async fn put_job(&mut self, queue: String, job: json::Value, pri: Priority) -> BoxResult<()> {
        let (tx, rx) = oneshot::channel();
        let req = ClientRequest::PutJob {
            queue,
            job,
            pri,
            resp: tx,
        };
        self.server_chan.send(req).await?;
        let job_id = rx.await?;
        let resp = Response::PutResp { id: job_id };
        self.send_resp(&resp).await?;
        Ok(())
    }

    async fn get_job(&mut self, queues: Vec<String>, wait: bool) -> BoxResult<()> {
        let response = if wait {
            let (tx, rx) = oneshot::channel();
            let req = ClientRequest::GetJob {
                queues,
                resp: GetJobResp::Wait(tx),
            };
            self.server_chan.send(req).await?;

            // directly awaiting on the `rx` mean that the client may block forever
            // and the tcp stream could be left hanging.
            // Also, we would miss delete job requests from the server.
            // So store this oneshot channel and await it in the select loop
            self.waiter = Some(rx);
            self.waiting_for_job = true;
            return Ok(());
        } else {
            let (tx, rx) = oneshot::channel();
            let req = ClientRequest::GetJob {
                queues,
                resp: GetJobResp::Now(tx),
            };
            self.server_chan.send(req).await?;
            match rx.await? {
                Ok(Some(job)) => {
                    self.assigned_jobs.insert(job.id, job.clone());
                    Response::GetResp {
                        queue: job.queue,
                        job: job.job,
                        pri: job.pri,
                        id: job.id,
                    }
                }
                Ok(None) => Response::NoJob,
                Err(err) => {
                    tracing::debug!("get job errored with {err:?}");
                    let resp = Response::Error {
                        error: format!("{err:?}"),
                    };
                    resp
                }
            }
        };
        self.send_resp(&response).await?;
        Ok(())
    }

    async fn get_job_response(&mut self, job_result: BoxResult<Job>) -> BoxResult<()> {
        self.waiting_for_job = false;
        let resp = match job_result {
            Ok(job) => {
                self.assigned_jobs.insert(job.id, job.clone());
                Response::GetResp {
                    queue: job.queue,
                    job: job.job,
                    pri: job.pri,
                    id: job.id,
                }
            }
            Err(err) => {
                tracing::debug!("get job errored with {err:?}");
                let resp = Response::Error {
                    error: format!("{err:?}"),
                };
                resp
            }
        };
        self.send_resp(&resp).await
    }

    async fn delete_job(&mut self, id: JobId) -> BoxResult<()> {
        let (resp, rx) = oneshot::channel();
        self.server_chan
            .send(ClientRequest::DelJob { id, resp })
            .await?;
        let response = if rx.await? {
            Response::Ok
        } else {
            Response::NoJob
        };
        self.send_resp(&response).await?;
        Ok(())
    }

    async fn abort_job(&mut self, id: JobId) -> BoxResult<()> {
        let resp = match self.assigned_jobs.remove(&id) {
            Some(job) => {
                tracing::debug!("aborting job {:?}", job.id);
                self.server_chan
                    .send(ClientRequest::AbortJobs { jobs: vec![job] })
                    .await?;
                Response::Ok
            }
            None => Response::Error {
                error: "cannot cancel a job not assigned to client".to_string(),
            },
        };
        self.send_resp(&resp).await?;
        Ok(())
    }

    // abort all jobs upon client termination
    async fn abort_jobs(self) -> BoxResult<()> {
        let jobs: Vec<_> = self.assigned_jobs.into_values().collect();

        if !jobs.is_empty() {
            self.server_chan
                .send(ClientRequest::AbortJobs { jobs })
                .await?;
        }
        Ok(())
    }

    async fn send_resp(&mut self, resp: &Response) -> BoxResult<()> {
        let mut bs = serde_json::to_vec(resp).unwrap();
        bs.push(b'\n');
        self.wrt.write_all(&bs).await?;
        self.wrt.flush().await?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "request", rename_all = "lowercase")]
enum Request {
    Put {
        queue: String,
        job: json::Value,
        pri: Priority,
    },
    Get {
        queues: Vec<String>,
        #[serde(default)]
        wait: bool,
    },
    Delete {
        id: JobId,
    },
    Abort {
        id: JobId,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase", try_from = "Response_")]
enum Response {
    Error {
        error: String,
    },
    #[serde(rename = "ok")]
    GetResp {
        queue: String,
        job: json::Value,
        pri: Priority,
        id: JobId,
    },
    #[serde(rename = "ok")]
    PutResp {
        id: JobId,
    },
    #[serde(rename = "no-job")]
    NoJob,
    Ok,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum Response_ {
    Error {
        error: String,
    },
    #[serde(rename = "no-job")]
    NoJob,
    Ok {
        queue: Option<String>,
        job: Option<json::Value>,
        pri: Option<Priority>,
        id: Option<JobId>,
    },
}

impl std::convert::TryFrom<Response_> for Response {
    type Error = String;

    fn try_from(value: Response_) -> Result<Self, String> {
        match value {
            Response_::Error { error } => Ok(Response::Error { error }),
            Response_::NoJob => Ok(Response::NoJob),
            Response_::Ok {
                queue,
                job,
                pri,
                id,
            } => match (queue, job, pri, id) {
                (None, None, None, None) => Ok(Response::Ok),
                (None, None, None, Some(id)) => Ok(Response::PutResp { id }),
                (Some(queue), Some(job), Some(pri), Some(id)) => Ok(Response::GetResp {
                    queue,
                    job,
                    pri,
                    id,
                }),
                x => Err(format!("Invalid combination of attributes: {x:?}")),
            },
        }
    }
}

/// used for spawned clients to communicate with the server
#[derive(Debug)]
enum ClientRequest {
    PutJob {
        queue: String,
        job: json::Value,
        pri: Priority,
        resp: oneshot::Sender<JobId>,
    },
    GetJob {
        queues: Vec<String>,
        resp: GetJobResp,
    },
    DelJob {
        id: JobId,
        /// false if the job doesn't exist
        resp: oneshot::Sender<bool>,
    },
    AbortJobs {
        jobs: Vec<Job>,
    },
}

#[derive(Debug)]
enum GetJobResp {
    Now(oneshot::Sender<BoxResult<Option<Job>>>),
    Wait(oneshot::Sender<BoxResult<Job>>),
}

impl GetJobResp {
    fn send_err(self, error: BoxError) {
        match self {
            // if the client went away (shouldn't happen), simply ignore that
            GetJobResp::Now(tx) => yolo_send(tx, Err(error)),
            GetJobResp::Wait(tx) => yolo_send(tx, Err(error)),
        };
    }

    fn send_job(self, job: Job) {
        match self {
            GetJobResp::Now(tx) => yolo_send(tx, Ok(Some(job))),
            GetJobResp::Wait(tx) => yolo_send(tx, Ok(job)),
        }
    }
}

/// to manage clients waiting for job on a given queue.
#[derive(Default)]
struct Waiters {
    // waiters: BTreeMap<u64, (Vec<String>, oneshot::Sender<BoxResult<Job>>)>,
    waiters: Vec<(Vec<String>, oneshot::Sender<BoxResult<Job>>)>,
}

impl Waiters {
    fn add(&mut self, queues: Vec<String>, tx: oneshot::Sender<BoxResult<Job>>) {
        // self.waiters.insert(self.counter, (queues, tx));
        // self.counter += 1;
        self.waiters.push((queues, tx));
    }

    /// returns the first not closed sender registered for the given queue
    fn pop_for_queue(&mut self, queue: &str) -> Option<oneshot::Sender<BoxResult<Job>>> {
        let mut i = 0;
        loop {
            match self.waiters.get(i) {
                None => {
                    break None;
                }
                Some((qs, _)) => {
                    if qs.iter().find(|q| (**q).as_str() == queue).is_some() {
                        let (_, tx) = self.waiters.swap_remove(i);
                        if tx.is_closed() {
                            tracing::debug!("dropping closed waiter for queue {}", queue);
                            continue;
                        }
                        break Some(tx);
                    }
                    i += 1;
                }
            }
        }
    }
}

async fn mb_await<F>(fut: &mut Option<F>) -> Option<F::Output>
where
    F: Future,
    <F as Future>::Output: std::fmt::Debug,
{
    match fut.take() {
        None => None,
        Some(f) => Some(f.await),
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;
    use crate::utils::AbortHdl;
    use json::json;
    use pretty_assertions::assert_eq;
    use tokio::time::timeout;

    async fn setup() -> BoxResult<(AbortHdl<()>, SocketAddr)> {
        let _ = tracing_subscriber::fmt::try_init();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = Server::new(listener);
        let hdl = AbortHdl(tokio::spawn(async move {
            match server.run().await {
                Ok(_) => tracing::error!("server has exited"),
                Err(err) => tracing::error!("error running server? {err:?}"),
            }
        }));
        Ok((hdl, addr))
    }

    struct TestClient {
        stream: BufReader<TcpStream>,
    }

    impl TestClient {
        async fn connect(addr: SocketAddr) -> BoxResult<Self> {
            let stream = TcpStream::connect(addr).await?;
            Ok(TestClient {
                stream: BufReader::new(stream),
            })
        }

        async fn send(&mut self, req: &Request) -> BoxResult<()> {
            let mut bs = serde_json::to_vec(req).unwrap();
            bs.push(b'\n');
            self.stream.write_all(&bs).await?;
            self.stream.flush().await?;
            Ok(())
        }

        async fn recv(&mut self) -> BoxResult<Response> {
            let mut buf = String::new();
            timeout(Duration::from_millis(10), self.stream.read_line(&mut buf))
                .await
                .expect("no timeout on receive")?;
            match serde_json::from_str(&buf) {
                Ok(r) => Ok(r),
                Err(err) => Err(format!("Could not parse response: {err:?} - {buf}").into()),
            }
        }

        async fn request(&mut self, req: &Request) -> BoxResult<Response> {
            self.send(req).await?;
            self.recv().await
        }

        /// useful when you only want a random job in a queue
        async fn submit_job(&mut self, queue: String) -> BoxResult<JobId> {
            let resp = self
                .request(&Request::Put {
                    queue,
                    job: json!(42),
                    pri: 10.into(),
                })
                .await?;
            match resp {
                Response::PutResp { id } => Ok(id),
                err => Err(format!("Expected a GetResp but got {err:?}").into()),
            }
        }
    }

    #[tokio::test]
    async fn test_get_no_queue() -> BoxResult<()> {
        let (_server, addr) = setup().await?;
        let mut client = TestClient::connect(addr).await?;
        let req = Request::Get {
            queues: vec!["q1".to_string()],
            wait: false,
        };
        match client.request(&req).await? {
            Response::NoJob => (),
            r => panic!("querying a non-existing queue is fine but got {r:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_simple_put_get() -> BoxResult<()> {
        let (_server, addr) = setup().await?;
        let mut client = TestClient::connect(addr).await?;
        let queue = "q1".to_string();
        let req = Request::Put {
            queue: queue.clone(),
            job: json!("job"),
            pri: 10.into(),
        };
        let resp = client.request(&req).await?;
        let job_id = match resp {
            Response::PutResp { id } => id,
            r => panic!("expected a PutResponse but got {r:?}"),
        };

        let get_req = Request::Get {
            queues: vec![queue],
            wait: false,
        };
        let get_resp = client.request(&get_req).await?;
        match get_resp {
            Response::GetResp {
                queue: _,
                job,
                pri,
                id,
            } => {
                assert_eq!(id, job_id, "got the correct job");
                assert_eq!(job, json!("job"), "correct payload");
                assert_eq!(pri, 10.into(), "correct priority");
            }
            r => panic!("expected a PutResponse but got {r:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_wait_job() -> BoxResult<()> {
        let (_server, addr) = setup().await?;
        let mut client = TestClient::connect(addr).await?;
        let mut client2 = TestClient::connect(addr).await?;
        let queue = "q1".to_string();

        client2
            .send(&Request::Get {
                queues: vec![queue.clone()],
                wait: true,
            })
            .await?;
        let mut buf = String::new();
        let timeout_resp =
            timeout(Duration::from_millis(1), client2.stream.read_line(&mut buf)).await;
        assert!(timeout_resp.is_err(), "waiting for job on queue");

        client
            .request(&Request::Put {
                queue: queue.clone(),
                job: json!("coucou"),
                pri: 10.into(),
            })
            .await?;

        client2.recv().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_abort() -> BoxResult<()> {
        let (_server, addr) = setup().await?;
        let mut client = TestClient::connect(addr).await?;
        let q = "q1".to_string();
        let job_id = client.submit_job(q.clone()).await?;

        let get_req = Request::Get {
            queues: vec![q.clone()],
            wait: false,
        };
        client.request(&get_req).await?;

        let mut client2 = TestClient::connect(addr).await?;
        let resp = client2.request(&get_req).await?;
        assert!(
            matches!(resp, Response::NoJob),
            "cannot get the same job twice"
        );

        let abort = Request::Abort { id: job_id };
        match client2.request(&abort).await? {
            Response::Error { .. } => (),
            r => panic!("Cannot abort non-assigned job: {r:?}"),
        };

        client.request(&abort).await?;
        match client2.request(&get_req).await? {
            Response::GetResp { .. } => (),
            _ => panic!("an aborted job should be retrievable again"),
        };

        client2.stream.into_inner().shutdown().await?;

        match client.request(&get_req).await? {
            Response::GetResp { .. } => (),
            _ => panic!("a client shutting down should automatically abort its requests"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> BoxResult<()> {
        let (_server, addr) = setup().await?;
        let mut client = TestClient::connect(addr).await?;
        let q = "q1".to_string();
        let job_id = client.submit_job(q.clone()).await?;
        let resp = client.request(&Request::Delete { id: job_id }).await?;
        assert!(matches!(resp, Response::Ok), "can delete a job");

        let resp = client.request(&Request::Delete { id: job_id }).await?;
        assert!(matches!(resp, Response::NoJob), "cannot delete a job twice");

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_with_waiter() -> BoxResult<()> {
        let (_server, addr) = setup().await?;
        let mut client = TestClient::connect(addr).await?;
        let mut client2 = TestClient::connect(addr).await?;
        let q = "q1".to_string();
        let job_id = client.submit_job(q.clone()).await?;

        client
            .request(&Request::Get {
                queues: vec![q.clone()],
                wait: true,
            })
            .await?;

        let resp = client2.request(&Request::Delete { id: job_id }).await?;
        assert!(
            matches!(resp, Response::Ok),
            "can delete a job while being processed"
        );

        let resp = client.request(&Request::Abort { id: job_id }).await?;
        assert!(
            matches!(resp, Response::Error { .. }),
            "cannot abort a deleted job"
        );

        Ok(())
    }
}
