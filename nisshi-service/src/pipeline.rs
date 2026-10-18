// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! kr: P13 (ADR-007 D3). The Produce pipeline of one connection.
//!
//! A reader and a writer on the connection task (`try_join!`, no spawn: P11 holds). The
//! reader reads request n, and for a Produce polls it until the handler admits it (every
//! batch ordered or refused by the engine, P12) before it reads n + 1; the writer writes the
//! responses in the order of the requests (`FuturesOrdered`). At most `depth` requests are
//! read and not answered: a permit is taken before the first byte of a request is read.
//! Any request other than a Produce is a barrier: it is first polled once every earlier
//! response is written, and nothing is read after it before its own response is written.

use std::{future::Future, io, ops::RangeInclusive, pin::Pin, sync::Arc};

use bytes::Bytes;
use futures_util::stream::{FuturesOrdered, StreamExt as _};
use nisshi_sans_io::{BytesInput, ProduceAdmission, RootMessageMeta};
use opentelemetry::KeyValue;
use rama::{
    Service,
    extensions::{Extensions, ExtensionsRef},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite},
    sync::{Notify, Semaphore, SemaphorePermit, mpsc},
    time::{Instant, timeout},
};
use tracing::debug;

use crate::{
    Error, frame_size,
    probe::{self, Stage},
    stream::{ConnectionLimits, TcpBytesService, read_exact_with_idle_timeout},
};

/// The largest `max.in.flight.requests.per.connection` of an idempotent producer (Java,
/// librdkafka): the deduplication window is 5 batches (ADR-002 D1).
pub(crate) const MAX_PRODUCE_PIPELINE_DEPTH: usize = 5;

const PRODUCE: i16 = 0;
/// KIP-896: the lowest Produce version a Kafka 4 broker serves.
const PRODUCE_BASELINE: i16 = 3;

type Response<'a, E> = Pin<Box<dyn Future<Output = Result<Bytes, E>> + Send + 'a>>;

enum Item<'a, E> {
    /// A request read, with the permits it holds until its response is written.
    Request {
        response: Response<'a, E>,
        permits: SemaphorePermit<'a>,
        api_key: i16,
    },
    /// The reader stopped: every response already queued is written, then the connection
    /// closes with this error.
    Close(E),
}

fn header(frame: &[u8], at: usize) -> Option<i16> {
    frame
        .get(at..at + 2)
        .and_then(|b| b.try_into().ok())
        .map(i16::from_be_bytes)
}

fn produce_versions() -> Option<RangeInclusive<i16>> {
    RootMessageMeta::messages()
        .requests()
        .get(&PRODUCE)
        .map(|meta| meta.version.valid.start.max(PRODUCE_BASELINE)..=meta.version.valid.end)
}

/// A Produce of an announced version: pipelined. Anything else is a barrier (ADR-007 D3). A
/// SASL v0 token is opaque and may look like one: it is never admitted by the Produce
/// handler, so it is served in series anyway (its admission is its completion).
fn pipelined(frame: &[u8]) -> bool {
    header(frame, 4) == Some(PRODUCE)
        && header(frame, 6).is_some_and(|v| produce_versions().is_some_and(|r| r.contains(&v)))
}

/// Requests read whose response is not written yet, the one being read included.
fn in_flight(slots: &Semaphore, depth: usize) -> usize {
    depth.saturating_sub(slots.available_permits())
}

pub(crate) async fn serve<S, Stream>(
    svc: &TcpBytesService<S>,
    stream: Stream,
    limits: ConnectionLimits,
    attributes: &[KeyValue],
) -> Result<(), S::Error>
where
    S: Service<BytesInput, Output = Bytes>,
    S::Error: From<Error> + From<io::Error> + std::fmt::Debug + Send,
    Stream: AsyncRead + AsyncWrite + ExtensionsRef + Unpin + Send,
{
    let depth = limits
        .produce_pipeline_depth
        .clamp(1, MAX_PRODUCE_PIPELINE_DEPTH);
    // Shared with every barrier, like the upstream loop (`clone`, not `fork`): what a
    // handler records on the connection (authentication) stays on it.
    let base = stream.extensions().clone();
    let (mut rd, mut wr) = tokio::io::split(stream);
    let slots = Semaphore::new(depth);
    let written = Notify::new();
    let (tx, rx) = mpsc::channel(depth + 1);

    tokio::try_join!(
        reader(
            svc, &mut rd, limits, attributes, &base, &slots, depth, &written, tx
        ),
        writer(svc, &mut wr, limits, rx, &written),
    )
    .map(|_| ())
}

/// The first byte of the next request. The connection idle timeout runs only while nothing
/// is in flight: it is re-armed as long as a response remains to be written (ADR-007 D3).
async fn first_byte<R>(
    rd: &mut R,
    limits: ConnectionLimits,
    slots: &Semaphore,
    depth: usize,
    written: &Notify,
) -> io::Result<u8>
where
    R: AsyncRead + Unpin,
{
    let mut byte = [0u8; 1];
    loop {
        let busy = in_flight(slots, depth) > 1;
        // Reading one byte is cancel safe: a dropped read consumed nothing.
        let n = match limits.connection_idle_timeout {
            Some(idle) if !busy => match timeout(idle, rd.read(&mut byte)).await {
                Ok(n) => n?,
                Err(_) if in_flight(slots, depth) > 1 => continue,
                Err(_) => return Err(io::Error::from(io::ErrorKind::TimedOut)),
            },
            Some(_) => tokio::select! {
                n = rd.read(&mut byte) => n?,
                () = written.notified() => continue,
            },
            None => rd.read(&mut byte).await?,
        };
        return if n == 0 {
            Err(io::Error::from(io::ErrorKind::UnexpectedEof))
        } else {
            Ok(byte[0])
        };
    }
}

/// The size prefix once its first byte is in: the rest of `TcpBytesService::wait`.
async fn rest_of_size<R, E>(rd: &mut R, first: u8, limits: ConnectionLimits) -> Result<[u8; 4], E>
where
    R: AsyncRead + Unpin,
    E: From<Error> + From<io::Error>,
{
    let mut size = [first, 0, 0, 0];
    read_exact_with_idle_timeout(rd, &mut size[1..], limits.io_idle_timeout).await?;
    let frame_size = frame_size(size)?;
    if limits
        .maximum_frame_size
        .is_some_and(|maximum_frame_size| frame_size > maximum_frame_size)
    {
        return Err(Error::FrameTooBig(frame_size).into());
    }
    Ok(size)
}

#[allow(clippy::too_many_arguments)]
async fn reader<'a, S, R>(
    svc: &'a TcpBytesService<S>,
    rd: &mut R,
    limits: ConnectionLimits,
    attributes: &'a [KeyValue],
    base: &Extensions,
    slots: &'a Semaphore,
    depth: usize,
    written: &Notify,
    tx: mpsc::Sender<Item<'a, S::Error>>,
) -> Result<(), S::Error>
where
    S: Service<BytesInput, Output = Bytes>,
    S::Error: From<Error> + From<io::Error> + std::fmt::Debug + Send,
    R: AsyncRead + Unpin,
{
    // `Err` only reaches the writer through `Close`: the writer must still write what is
    // queued. The writer gone (write failure) stops the reader too.
    loop {
        let Ok(permit) = slots.acquire().await else {
            return Ok(());
        };
        let started = probe::now();
        let first = match first_byte(rd, limits, slots, depth, written).await {
            Ok(first) => first,
            Err(err) => {
                debug!(?err);
                _ = tx.send(Item::Close(err.into())).await;
                return Ok(());
            }
        };
        let waited = probe::now();
        let request = match rest_of_size::<_, S::Error>(rd, first, limits).await {
            Ok(size) => svc.read(rd, size, limits).await,
            Err(err) => Err(err),
        };
        let request = match request {
            Ok(request) => request,
            Err(err) => {
                _ = tx.send(Item::Close(err)).await;
                return Ok(());
            }
        };
        let api_key = probe::api_key_of(&request);
        probe::record(api_key, Stage::Idle, waited.duration_since(started));
        probe::record(api_key, Stage::Read, waited.elapsed());
        probe::record_depth(in_flight(slots, depth));

        if !pipelined(&request) {
            // Barrier: polled once every earlier response is written (their permits back).
            let Ok(others) = slots.acquire_many((depth - 1) as u32).await else {
                return Ok(());
            };
            let mut permits = permit;
            permits.merge(others);
            let response = Box::pin(svc.process(attributes, request, base.clone()));
            if tx
                .send(Item::Request {
                    response,
                    permits,
                    api_key,
                })
                .await
                .is_err()
            {
                return Ok(());
            }
            continue;
        }

        let admitted = Arc::new(Notify::new());
        let extensions = base.fork();
        _ = extensions.insert(ProduceAdmission::new({
            let admitted = Arc::clone(&admitted);
            move || admitted.notify_one()
        }));
        let read = Instant::now();
        let mut response: Response<'a, S::Error> =
            Box::pin(svc.process(attributes, request, extensions));
        // Stage 1 (decoding, P11, included) is polled here, by the reader.
        let failed = tokio::select! {
            biased;
            done = &mut response => {
                let failed = done.is_err();
                response = Box::pin(std::future::ready(done));
                failed
            }
            () = admitted.notified() => false,
        };
        probe::record(api_key, Stage::Admit, read.elapsed());
        if tx
            .send(Item::Request {
                response,
                permits: permit,
                api_key,
            })
            .await
            .is_err()
            || failed
        {
            // Nothing is read after a request whose processing failed (ADR-007 I-P2).
            return Ok(());
        }
    }
}

async fn writer<'a, S, W>(
    svc: &TcpBytesService<S>,
    wr: &mut W,
    limits: ConnectionLimits,
    mut rx: mpsc::Receiver<Item<'a, S::Error>>,
    written: &Notify,
) -> Result<(), S::Error>
where
    S: Service<BytesInput, Output = Bytes>,
    S::Error: From<Error> + From<io::Error> + std::fmt::Debug + Send,
    W: AsyncWrite + Unpin,
{
    let mut queue = FuturesOrdered::new();
    let mut closing = None;
    let mut open = true;

    loop {
        if !open && queue.is_empty() {
            return closing.map_or(Ok(()), Err);
        }

        tokio::select! {
            item = rx.recv(), if open => match item {
                Some(Item::Request { response, permits, api_key }) => {
                    queue.push_back(async move {
                        let response = response.await;
                        (response, permits, api_key, Instant::now())
                    });
                }
                Some(Item::Close(err)) => closing = Some(err),
                None => open = false,
            },

            Some((response, permits, api_key, done)) = queue.next(), if !queue.is_empty() => {
                // An error closes the connection: nothing is written for this request nor
                // after it (ADR-007 I-P2), what is queued behind it is dropped.
                let response = response?;
                let writing = Instant::now();
                probe::record(api_key, Stage::Queue, writing.duration_since(done));
                svc.write(wr, response, limits).await?;
                probe::record(api_key, Stage::Write, writing.elapsed());
                drop(permits);
                written.notify_one();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        pin::Pin,
        sync::Mutex,
        task::{Context, Poll},
        time::Duration,
    };

    use rama::Layer as _;
    use tokio::io::{AsyncWriteExt as _, DuplexStream, ReadBuf, duplex};

    use super::*;
    use crate::stream::{TcpBytesLayer, TcpContext, TcpContextLayer};

    /// Deterministic generator for the scripts (no dependency on a `rand` version).
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
    }

    /// What one scripted request does in the inner service.
    #[derive(Clone, Copy, Debug)]
    struct Script {
        /// Produce (pipelined) or another api (a barrier).
        produce: bool,
        /// Delay before the admission of a Produce (`None`: never admitted).
        admit_after: Option<u64>,
        /// Delay of the whole request.
        done_after: u64,
        /// The service fails: before or after admission (by `done_after` vs `admit_after`).
        fails: bool,
    }

    #[derive(Default, Debug)]
    struct Log {
        /// Frames whose first byte the server consumed, and response frames fully written.
        started: usize,
        written: usize,
        read_bytes: usize,
        written_bytes: usize,
        /// Request index -> admitted (or completed) by the service.
        admitted: BTreeMap<usize, bool>,
        /// Request index -> `written` when the service first polled it.
        polled_at_written: BTreeMap<usize, usize>,
        violations: Vec<String>,
        fail_write_at: Option<usize>,
    }

    #[derive(Clone)]
    struct Shared {
        log: Arc<Mutex<Log>>,
        /// Byte offset where each request frame starts, then the total length.
        starts: Arc<Vec<usize>>,
        scripts: Arc<Vec<Script>>,
        depth: usize,
        response_len: usize,
    }

    impl Shared {
        fn with<T>(&self, f: impl FnOnce(&mut Log) -> T) -> T {
            f(&mut self.log.lock().unwrap())
        }
    }

    /// Server end of the connection: records reads and writes, checks I-P3 to I-P5 at the
    /// moment bytes cross it.
    struct Recorder {
        stream: DuplexStream,
        extensions: Extensions,
        shared: Shared,
    }

    impl AsyncRead for Recorder {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let before = buf.filled().len();
            let poll = Pin::new(&mut self.stream).poll_read(cx, buf);
            let n = buf.filled().len() - before;
            if n > 0 {
                let shared = self.shared.clone();
                shared.with(|log| {
                    log.read_bytes += n;
                    let starts = &shared.starts[..shared.starts.len() - 1];
                    let started = starts.iter().filter(|s| **s < log.read_bytes).count();
                    for j in log.started..started {
                        // I-P3
                        if j + 1 - log.written > shared.depth {
                            log.violations.push(format!(
                                "I-P3: request {j} read with {} unanswered",
                                j - log.written
                            ));
                        }
                        // I-P5: n + 1 read only once n is admitted (or completed).
                        if j > 0 && !log.admitted.get(&(j - 1)).copied().unwrap_or_default() {
                            log.violations.push(format!(
                                "I-P5: request {j} read before {} was admitted",
                                j - 1
                            ));
                        }
                        // I-P4: nothing read after a barrier before its response is written.
                        if let Some(b) = (0..j).rev().find(|b| !shared.scripts[*b].produce)
                            && log.written <= b
                        {
                            log.violations.push(format!(
                                "I-P4: request {j} read before barrier {b} was answered"
                            ));
                        }
                    }
                    log.started = log.started.max(started);
                });
            }
            poll
        }
    }

    impl AsyncWrite for Recorder {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let shared = self.shared.clone();
            if shared.with(|log| log.fail_write_at == Some(log.written)) {
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
            }
            let poll = Pin::new(&mut self.stream).poll_write(cx, buf);
            if let Poll::Ready(Ok(n)) = poll {
                shared.with(|log| {
                    log.written_bytes += n;
                    log.written = log.written_bytes / shared.response_len;
                });
            }
            poll
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_shutdown(cx)
        }
    }

    impl ExtensionsRef for Recorder {
        fn extensions(&self) -> &Extensions {
            &self.extensions
        }
    }

    /// The inner service: plays the script of each request (by its correlation id).
    #[derive(Clone)]
    struct Scripted(Shared);

    impl Service<BytesInput> for Scripted {
        type Output = Bytes;
        type Error = Error;

        async fn serve(&self, req: BytesInput) -> Result<Bytes, Error> {
            let shared = &self.0;
            let id = req
                .bytes
                .get(8..12)
                .and_then(|b| b.try_into().ok())
                .map(i32::from_be_bytes)
                .and_then(|id| usize::try_from(id).ok())
                .unwrap_or_default();
            let script = shared.scripts[id];
            shared.with(|log| {
                let written = log.written;
                _ = log.polled_at_written.insert(id, written);
                // I-P4: a barrier is first polled once every earlier response is written.
                if !script.produce && written < id {
                    log.violations
                        .push(format!("I-P4: barrier {id} polled with {written} written"));
                }
            });
            let admission = req.extensions.get_ref::<ProduceAdmission>().cloned();
            if (script.produce && shared.depth > 1) != admission.is_some() {
                shared.with(|log| {
                    log.violations
                        .push(format!("request {id}: admission {}", admission.is_some()))
                });
            }
            let admit = || {
                if let Some(a) = admission.as_ref() {
                    a.admit();
                    a.admit(); // idempotent
                }
                shared.with(|log| _ = log.admitted.insert(id, true));
            };
            match script.admit_after {
                Some(at) if at <= script.done_after && script.produce => {
                    tokio::time::sleep(Duration::from_millis(at)).await;
                    if script.fails && at == script.done_after {
                        return Err(Error::Message(format!("failed before admission {id}")));
                    }
                    admit();
                    tokio::time::sleep(Duration::from_millis(script.done_after - at)).await;
                }
                _ => tokio::time::sleep(Duration::from_millis(script.done_after)).await,
            }
            if script.fails {
                return Err(Error::Message(format!("failed {id}")));
            }
            shared.with(|log| _ = log.admitted.insert(id, true));
            Ok(response(id, shared.response_len))
        }
    }

    fn response(id: usize, len: usize) -> Bytes {
        let mut b = ((len - 4) as i32).to_be_bytes().to_vec();
        b.extend_from_slice(&(id as i32).to_be_bytes());
        b.resize(len, 0xAB);
        Bytes::from(b)
    }

    /// size, api key, version, correlation id, padding.
    fn request(id: usize, produce: bool, len: usize) -> Vec<u8> {
        let mut b = ((len - 4) as i32).to_be_bytes().to_vec();
        b.extend_from_slice(&(if produce { 0i16 } else { 3i16 }).to_be_bytes());
        b.extend_from_slice(&(if produce { 9i16 } else { 12i16 }).to_be_bytes());
        b.extend_from_slice(&(id as i32).to_be_bytes());
        b.resize(len, id as u8);
        b
    }

    fn scripts(rng: &mut Rng, n: usize, fail: Option<(usize, bool)>) -> Vec<Script> {
        (0..n)
            .map(|i| {
                let produce = rng.below(5) != 0;
                let done_after = 1 + rng.below(40);
                let admit_after = Some(rng.below(done_after + 1));
                let mut s = Script {
                    produce,
                    admit_after,
                    done_after,
                    fails: false,
                };
                if let Some((at, after_admission)) = fail
                    && at == i
                {
                    s.produce = true;
                    s.fails = true;
                    s.admit_after = Some(if after_admission { 0 } else { s.done_after });
                }
                s
            })
            .collect()
    }

    struct Run {
        /// Correlation ids of the responses, in the order the client read them.
        responses: Vec<usize>,
        /// Every byte the client read.
        bytes: Vec<u8>,
        log: Log,
        result: Result<(), Error>,
    }

    async fn run(
        depth: usize,
        scripts: Vec<Script>,
        idle: Option<Duration>,
        close_after_send: bool,
        fail_write_at: Option<usize>,
        client_pause: Option<Duration>,
    ) -> Run {
        let request_len = 40;
        let response_len = 16;
        let mut wire = Vec::new();
        let mut starts = Vec::new();
        for (i, s) in scripts.iter().enumerate() {
            starts.push(wire.len());
            wire.extend(request(i, s.produce, request_len));
        }
        starts.push(wire.len());
        let shared = Shared {
            log: Arc::new(Mutex::new(Log {
                fail_write_at,
                ..Log::default()
            })),
            starts: Arc::new(starts),
            scripts: Arc::new(scripts),
            depth,
            response_len,
        };
        let (client, server) = duplex(1 << 16);
        let server = Recorder {
            stream: server,
            extensions: Extensions::default(),
            shared: shared.clone(),
        };
        let service = (
            TcpContextLayer::new(
                TcpContext::default()
                    .produce_pipeline_depth(Some(depth))
                    .connection_idle_timeout(idle)
                    .io_idle_timeout(None),
            ),
            TcpBytesLayer,
        )
            .into_layer(Scripted(shared.clone()));
        let (mut crd, mut cwr) = tokio::io::split(client);
        let client = async move {
            if let Some(pause) = client_pause {
                // Two requests, a pause longer than the idle timeout, then the rest.
                let cut = shared.starts.get(2).copied().unwrap_or(wire.len());
                cwr.write_all(&wire[..cut]).await.unwrap();
                tokio::time::sleep(pause).await;
                cwr.write_all(&wire[cut..]).await.unwrap();
            } else {
                cwr.write_all(&wire).await.unwrap();
            }
            if close_after_send {
                cwr.shutdown().await.unwrap();
            }
            let mut bytes = Vec::new();
            _ = crd.read_to_end(&mut bytes).await;
            drop(cwr);
            bytes
        };
        let (result, bytes) = tokio::join!(service.serve(server), client);
        let responses = bytes
            .chunks(response_len)
            .filter(|c| c.len() == response_len)
            .map(|c| i32::from_be_bytes([c[4], c[5], c[6], c[7]]) as usize)
            .collect();
        let log = std::mem::take(&mut *shared.log.lock().unwrap());
        Run {
            responses,
            bytes,
            log,
            result,
        }
    }

    fn check(seed: u64, depth: usize, r: &Run, expected: usize) {
        assert!(
            r.log.violations.is_empty(),
            "seed {seed} depth {depth}: {:?}",
            r.log.violations
        );
        // I-P1: a prefix of the requests, in order, without repetition.
        assert_eq!(
            (0..expected).collect::<Vec<_>>(),
            r.responses,
            "seed {seed} depth {depth}"
        );
    }

    /// I-P1 to I-P5, EOF with requests in flight (every response written), and the
    /// differential I-P10: the bytes are those of the upstream loop (depth 1).
    #[tokio::test(start_paused = true)]
    async fn properties_and_differential_against_depth_1() {
        for seed in 1..=60u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
            let n = 1 + rng.below(24) as usize;
            let s = scripts(&mut rng, n, None);
            let upstream = run(1, s.clone(), None, true, None, None).await;
            check(seed, 1, &upstream, n);
            for depth in 2..=MAX_PRODUCE_PIPELINE_DEPTH {
                let r = run(depth, s.clone(), None, true, None, None).await;
                check(seed, depth, &r, n);
                assert_eq!(upstream.bytes, r.bytes, "seed {seed} depth {depth}: I-P10");
                assert!(r.result.is_err(), "EOF closes with an error, like upstream");
            }
        }
    }

    /// The pipeline is used: with slow commits, Produce requests overlap up to `depth`.
    #[tokio::test(start_paused = true)]
    async fn produce_requests_overlap_up_to_depth() {
        let s = (0..20)
            .map(|_| Script {
                produce: true,
                admit_after: Some(0),
                done_after: 10,
                fails: false,
            })
            .collect::<Vec<_>>();
        let started = Instant::now();
        let r = run(5, s.clone(), None, true, None, None).await;
        check(0, 5, &r, 20);
        assert!(
            started.elapsed() <= Duration::from_millis(60),
            "{:?}",
            started.elapsed()
        );
        let started = Instant::now();
        let r = run(1, s, None, true, None, None).await;
        check(0, 1, &r, 20);
        assert!(started.elapsed() >= Duration::from_millis(200));
    }

    /// I-P2: `process(n)` fails before or after its admission: responses 1..n-1 written,
    /// nothing for n nor after, the connection closes.
    #[tokio::test(start_paused = true)]
    async fn process_error_before_and_after_admission() {
        for seed in 1..=40u64 {
            for after_admission in [false, true] {
                let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1);
                let n = 4 + rng.below(12) as usize;
                let at = rng.below(n as u64) as usize;
                let s = scripts(&mut rng, n, Some((at, after_admission)));
                for depth in [1, 3, 5] {
                    let r = run(depth, s.clone(), None, false, None, None).await;
                    assert!(r.result.is_err());
                    assert!(r.log.violations.is_empty(), "{:?}", r.log.violations);
                    assert_eq!(
                        (0..at).collect::<Vec<_>>(),
                        r.responses,
                        "seed {seed} depth {depth} at {at}"
                    );
                    assert_eq!(at * 16, r.bytes.len(), "no byte after the failed request");
                }
            }
        }
    }

    /// A failed write closes at once.
    #[tokio::test(start_paused = true)]
    async fn write_failure_closes() {
        let mut rng = Rng(7);
        let s = scripts(&mut rng, 12, None);
        for depth in [1, 5] {
            let r = run(depth, s.clone(), None, false, Some(3), None).await;
            assert!(r.result.is_err());
            assert_eq!(vec![0, 1, 2], r.responses);
        }
    }

    /// The connection idle timeout does not fire while responses are pending, and does once
    /// the connection is idle.
    #[tokio::test(start_paused = true)]
    async fn idle_timeout_waits_for_the_requests_in_flight() {
        let s = (0..2)
            .map(|_| Script {
                produce: true,
                admit_after: Some(0),
                done_after: 500,
                fails: false,
            })
            .collect::<Vec<_>>();
        let started = Instant::now();
        let r = run(5, s, Some(Duration::from_millis(50)), false, None, None).await;
        assert_eq!(vec![0, 1], r.responses);
        assert!(r.result.is_err(), "closed by the idle timeout");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(550) && elapsed < Duration::from_millis(700),
            "{elapsed:?}"
        );
    }

    /// A barrier waits for the pending Produce responses even when it completes first, and
    /// what follows a never admitted Produce is read only at its completion.
    #[tokio::test(start_paused = true)]
    async fn barrier_and_completion_without_admission() {
        let s = vec![
            Script {
                produce: true,
                admit_after: Some(0),
                done_after: 100,
                fails: false,
            },
            Script {
                produce: false,
                admit_after: None,
                done_after: 1,
                fails: false,
            },
            Script {
                produce: true,
                admit_after: None,
                done_after: 30,
                fails: false,
            },
            Script {
                produce: true,
                admit_after: Some(0),
                done_after: 1,
                fails: false,
            },
        ];
        let r = run(5, s, None, true, None, None).await;
        check(0, 5, &r, 4);
        assert_eq!(
            Some(&1),
            r.log.polled_at_written.get(&1),
            "barrier polled after 0 written"
        );
    }
}
