// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
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

use std::time::{SystemTime, UNIX_EPOCH};

use futures::future::join_all;
use nisshi_sans_io::{
    ApiKey, BatchAttribute, ErrorCode, ProduceAdmission, ProduceRequest, ProduceResponse,
    RequestInput, TimestampType,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::deflated,
};
use rama::Service;
use tracing::{debug, error, instrument, warn};

use crate::{Error, ProduceCompletion, Result, Storage, Topition};

/// Why a client batch must be rejected with `INVALID_RECORD` before anything
/// is written, or `None` if it may be stored. Kafka's `LogValidator` rejects
/// the same batches.
///
/// - Only the broker writes control batches (transaction commit/abort
///   markers), and those go directly to storage rather than through
///   [`ProduceService`]. Every backend skips schema validation and lake
///   writes for a control batch, so a client must not be able to set the bit.
/// - A consistent header has at least one record and
///   `last_offset_delta + 1 == record_count`. Every backend uses
///   `last_offset_delta` to advance the high watermark, so a mismatch corrupts
///   or wedges the partition. `record_count` is an int32 on the wire, so a
///   value above `i32::MAX` is rejected too.
fn rejection(batch: &deflated::Batch) -> Option<&'static str> {
    if batch.is_control() {
        return Some("clients may not write control batches");
    }

    let Ok(record_count) = i32::try_from(batch.record_count) else {
        return Some("record_count exceeds i32::MAX");
    };

    if record_count < 1 {
        Some("batch has no records")
    } else if batch.last_offset_delta.checked_add(1) != Some(record_count) {
        Some("last_offset_delta + 1 does not equal record_count")
    } else {
        None
    }
}

/// A [`Service`] using [`Storage`] as [`Context`] taking [`ProduceRequest`] returning [`ProduceResponse`].
/// ```no_run
/// use bytes::Bytes;
/// use rama::Service as _;
/// use nisshi_sans_io::{
///     CreateTopicsRequest, ErrorCode, ProduceRequest,
///     create_topics_request::CreatableTopic,
///     produce_request::{PartitionProduceData, TopicProduceData},
///     record::{Record, deflated::Frame, inflated},
/// };
/// use nisshi_storage::{CreateTopicsService, Error, ProduceService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const CLUSTER_ID: &str = "nisshi";
/// const NODE_ID: i32 = 111;
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
///
/// let storage = StorageContainer::builder()
///     .cluster_id(CLUSTER_ID)
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let create_topic = CreateTopicsService {
///     storage: storage.clone(),
/// };
///
/// let name = "abcba";
///
/// let response = create_topic
///     .serve(
///         CreateTopicsRequest::default()
///             .topics(Some(vec![
///                 CreatableTopic::default()
///                     .name(name.into())
///                     .num_partitions(5)
///                     .replication_factor(3)
///                     .assignments(Some([].into()))
///                     .configs(Some([].into())),
///             ]))
///             .validate_only(Some(false)),
///     )
///     .await?;
///
/// let topics = response.topics.unwrap_or_default();
/// assert_eq!(1, topics.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
///
/// let produce = ProduceService {
///     storage: storage.clone(),
/// };
///
/// let partition = 0;
///
/// let response = produce
///     .serve(
///         ProduceRequest::default().topic_data(Some(
///             [TopicProduceData::default()
///                 .name(name.into())
///                 .partition_data(Some(
///                     [PartitionProduceData::default()
///                         .index(partition)
///                         .records(Some(Frame {
///                             batches: vec![
///                                 inflated::Batch::builder()
///                                     .record(
///                                         Record::builder().value(
///                                             Bytes::from_static(
///                                                 b"Lorem ipsum dolor sit amet",
///                                             )
///                                             .into(),
///                                         ),
///                                     )
///                                     .build()
///                                     .and_then(TryInto::try_into)?,
///                             ],
///                         }))]
///                     .into(),
///                 ))]
///             .into(),
///         )),
///     )
///     .await?;
///
/// let topics = response.responses.as_deref().unwrap_or_default();
/// assert_eq!(1, topics.len());
/// let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
/// assert_eq!(1, partitions.len());
/// assert_eq!(
///     ErrorCode::None,
///     ErrorCode::try_from(partitions[0].error_code)?
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ProduceService<G> {
    pub storage: G,
}

impl<G> ApiKey for ProduceService<G> {
    const KEY: i16 = ProduceRequest::KEY;
}

impl<G> ProduceService<G>
where
    G: Storage,
{
    fn error(&self, index: i32, error_code: ErrorCode) -> PartitionProduceResponse {
        PartitionProduceResponse::default()
            .index(index)
            .error_code(error_code.into())
            .base_offset(-1)
            .log_append_time_ms(Some(-1))
            .log_start_offset(Some(0))
            .record_errors(Some([].into()))
            .error_message(None)
            .current_leader(None)
    }

    #[instrument(skip_all)]
    async fn partition(
        &self,
        transaction_id: Option<&str>,
        name: &str,
        partition: PartitionProduceData,
    ) -> PartitionProduceResponse {
        if let Some(records) = partition.records {
            if let Some((rejected, reason)) = records
                .batches
                .iter()
                .find_map(|batch| rejection(batch).map(|reason| (batch, reason)))
            {
                warn!(
                    topic = name,
                    partition = partition.index,
                    record_count = rejected.record_count,
                    last_offset_delta = rejected.last_offset_delta,
                    reason,
                    "rejecting produce batch",
                );

                return self
                    .error(partition.index, ErrorCode::InvalidRecord)
                    .error_message(Some(reason.into()));
            }

            let mut base_offset = None;

            for mut batch in records.batches {
                let tp = Topition::new(name, partition.index);

                if BatchAttribute::try_from(batch.attributes)
                    .map(|attributes| attributes.timestamp == TimestampType::LogAppendTime)
                    .unwrap_or_default()
                {
                    let base_timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|duration| duration.as_millis() as i64)
                        .unwrap_or_default();

                    batch.base_timestamp = base_timestamp;
                    batch.max_timestamp = base_timestamp;
                }

                match self
                    .storage
                    .produce(transaction_id, &tp, batch)
                    .await
                    .inspect_err(|err| match err {
                        storage_api @ Error::Api(_) => {
                            warn!(?storage_api)
                        }
                        otherwise => error!(?otherwise),
                    }) {
                    Ok(offset) => _ = base_offset.get_or_insert(offset),

                    Err(Error::Api(error_code)) => {
                        debug!(?self, ?error_code);
                        return self.error(partition.index, error_code);
                    }

                    Err(otherwise) => {
                        warn!(?otherwise);
                        let error = self.error(partition.index, ErrorCode::UnknownServerError);
                        return error;
                    }
                }
            }

            if let Some(base_offset) = base_offset {
                PartitionProduceResponse::default()
                    .index(partition.index)
                    .error_code(ErrorCode::None.into())
                    .base_offset(base_offset)
                    .log_append_time_ms(Some(-1))
                    .log_start_offset(Some(0))
                    .record_errors(Some([].into()))
                    .error_message(None)
                    .current_leader(None)
            } else {
                self.error(partition.index, ErrorCode::UnknownServerError)
            }
        } else {
            self.error(partition.index, ErrorCode::UnknownServerError)
        }
    }

    #[instrument(skip_all)]
    async fn topic(
        &self,
        transaction_id: Option<&str>,
        topic: TopicProduceData,
    ) -> TopicProduceResponse {
        // kr: P3 (ADR-002 D9). The partitions of a request are produced
        // concurrently, as Kafka appends them all before waiting in
        // purgatory: with an engine that acknowledges after a replicated
        // commit, the commits overlap instead of adding up. A partition
        // appears once per request, and its batches stay sequential, so the
        // order within a partition is unchanged; responses keep the order of
        // the request.
        let partitions = join_all(
            topic
                .partition_data
                .unwrap_or_default()
                .into_iter()
                .map(|partition| self.partition(transaction_id, &topic.name, partition)),
        )
        .await;

        TopicProduceResponse::default()
            .name(topic.name)
            .partition_responses(Some(partitions))
    }

    /// kr: P12 (ADR-007 D2). Stage 1 of one partition of the request: the same checks as
    /// [`Self::partition`], then its batches handed to [`Storage::produce_staged`] one after
    /// the other. The first refusal stops the partition: later batches are not staged.
    async fn partition_staged(
        &self,
        transaction_id: Option<&str>,
        name: &str,
        partition: PartitionProduceData,
    ) -> StagedPartition {
        let index = partition.index;
        let staged = |early| StagedPartition {
            index,
            completions: Vec::new(),
            refused: None,
            early: Some(early),
        };

        let Some(records) = partition.records else {
            return staged(self.error(index, ErrorCode::UnknownServerError));
        };

        if let Some((rejected, reason)) = records
            .batches
            .iter()
            .find_map(|batch| rejection(batch).map(|reason| (batch, reason)))
        {
            warn!(
                topic = name,
                partition = index,
                record_count = rejected.record_count,
                last_offset_delta = rejected.last_offset_delta,
                reason,
                "rejecting produce batch",
            );

            return staged(
                self.error(index, ErrorCode::InvalidRecord)
                    .error_message(Some(reason.into())),
            );
        }

        let mut out = StagedPartition {
            index,
            completions: Vec::with_capacity(records.batches.len()),
            refused: None,
            early: None,
        };

        let tp = Topition::new(name, index);

        for mut batch in records.batches {
            // Set before stage 1: the batch is ordered with its final timestamps.
            if BatchAttribute::try_from(batch.attributes)
                .map(|attributes| attributes.timestamp == TimestampType::LogAppendTime)
                .unwrap_or_default()
            {
                let base_timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|duration| duration.as_millis() as i64)
                    .unwrap_or_default();

                batch.base_timestamp = base_timestamp;
                batch.max_timestamp = base_timestamp;
            }

            match self
                .storage
                .produce_staged(transaction_id, &tp, batch)
                .await
            {
                Ok(completion) => out.completions.push(completion),

                Err(Error::Api(error_code)) => {
                    debug!(?self, ?error_code);
                    out.refused = Some(error_code);
                    break;
                }

                Err(otherwise) => {
                    warn!(?otherwise);
                    out.refused = Some(ErrorCode::UnknownServerError);
                    break;
                }
            }
        }

        out
    }

    /// kr: P12 (ADR-007 D2). Stage 2 of one partition: the response [`Self::partition`]
    /// would have built (base offset of the first batch, code of the first error).
    async fn partition_completed(&self, staged: StagedPartition) -> PartitionProduceResponse {
        if let Some(early) = staged.early {
            return early;
        }

        let mut base_offset = None;

        for completion in staged.completions {
            match completion.await.inspect_err(|err| match err {
                storage_api @ Error::Api(_) => {
                    warn!(?storage_api)
                }
                otherwise => error!(?otherwise),
            }) {
                Ok(offset) => _ = base_offset.get_or_insert(offset),
                Err(Error::Api(error_code)) => return self.error(staged.index, error_code),
                Err(_) => return self.error(staged.index, ErrorCode::UnknownServerError),
            }
        }

        match (staged.refused, base_offset) {
            (Some(error_code), _) => self.error(staged.index, error_code),
            (None, Some(base_offset)) => PartitionProduceResponse::default()
                .index(staged.index)
                .error_code(ErrorCode::None.into())
                .base_offset(base_offset)
                .log_append_time_ms(Some(-1))
                .log_start_offset(Some(0))
                .record_errors(Some([].into()))
                .error_message(None)
                .current_leader(None),
            (None, None) => self.error(staged.index, ErrorCode::UnknownServerError),
        }
    }

    /// kr: P12 (ADR-007 D2). The request in two stages, for a pipelining connection loop:
    /// stage 1 of every partition (in parallel, P3; a partition present twice in the request
    /// is staged in request order), then `admission`, then stage 2 of every partition.
    async fn serve_staged(
        &self,
        transaction_id: Option<&str>,
        topics: Vec<TopicProduceData>,
        admission: &ProduceAdmission,
    ) -> Vec<TopicProduceResponse> {
        let mut names = Vec::with_capacity(topics.len());
        let mut entries = Vec::new();

        for (t, topic) in topics.into_iter().enumerate() {
            for partition in topic.partition_data.unwrap_or_default() {
                entries.push((t, partition));
            }
            names.push(topic.name);
        }

        let total = entries.len();

        // One chain per distinct topic partition, in order of first appearance.
        type Chain = Vec<(usize, usize, PartitionProduceData)>;
        let mut chains: Vec<(String, i32, Chain)> = Vec::new();
        for (i, (t, partition)) in entries.into_iter().enumerate() {
            let name = names.get(t).cloned().unwrap_or_default();
            let index = partition.index;
            match chains
                .iter_mut()
                .find(|(n, p, _)| *n == name && *p == index)
            {
                Some((_, _, chain)) => chain.push((i, t, partition)),
                None => chains.push((name, index, vec![(i, t, partition)])),
            }
        }

        // The first refusal stops the chain: a later entry of the same partition is not
        // staged either (ADR-007 D2), it gets the same code.
        let stage_one = join_all(chains.into_iter().map(|(name, _, chain)| async move {
            let mut out = Vec::with_capacity(chain.len());
            let mut stopped = None;
            for (i, t, partition) in chain {
                let s = match stopped {
                    Some(code) => StagedPartition {
                        index: partition.index,
                        completions: Vec::new(),
                        refused: None,
                        early: Some(self.error(partition.index, code)),
                    },
                    None => {
                        self.partition_staged(transaction_id, &name, partition)
                            .await
                    }
                };
                stopped = stopped.or_else(|| s.stop());
                out.push((i, t, s));
            }
            out
        }))
        .await;

        admission.admit();

        let mut staged: Vec<Option<(usize, StagedPartition)>> = (0..total).map(|_| None).collect();
        for (i, t, s) in stage_one.into_iter().flatten() {
            if let Some(slot) = staged.get_mut(i) {
                *slot = Some((t, s));
            }
        }

        let completed = join_all(
            staged
                .into_iter()
                .flatten()
                .map(|(t, s)| async move { (t, self.partition_completed(s).await) }),
        )
        .await;

        let mut responses: Vec<Vec<PartitionProduceResponse>> =
            names.iter().map(|_| Vec::new()).collect();
        for (t, response) in completed {
            if let Some(partitions) = responses.get_mut(t) {
                partitions.push(response);
            }
        }

        names
            .iter()
            .zip(responses)
            .map(|(name, partitions)| {
                TopicProduceResponse::default()
                    .name(name.clone())
                    .partition_responses(Some(partitions))
            })
            .collect()
    }
}

/// kr: P12. Stage 1 of one partition entry of a Produce request.
struct StagedPartition {
    index: i32,
    /// Batches ordered by the engine, in request order.
    completions: Vec<ProduceCompletion>,
    /// Refusal of the first batch not staged.
    refused: Option<ErrorCode>,
    /// Response decided before any batch was staged.
    early: Option<PartitionProduceResponse>,
}

impl StagedPartition {
    /// The code that stops the chain of this partition in the request, if any.
    fn stop(&self) -> Option<ErrorCode> {
        self.refused.or_else(|| {
            self.early
                .as_ref()
                .filter(|early| early.error_code != i16::from(ErrorCode::None))
                .map(|early| {
                    ErrorCode::try_from(early.error_code).unwrap_or(ErrorCode::UnknownServerError)
                })
        })
    }
}

impl<G, I> Service<I> for ProduceService<G>
where
    G: Storage,
    I: Into<RequestInput<ProduceRequest>> + Send + 'static,
{
    type Output = ProduceResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let RequestInput {
            request,
            extensions,
        } = input.into();

        let transaction_id = request.transactional_id.as_deref();

        // kr: P12 (ADR-007 D2). Two stages only for a pipelining connection loop; without
        // the extension, the P3 path below, unchanged.
        if let Some(admission) = extensions.get_ref::<ProduceAdmission>() {
            let responses = self
                .serve_staged(
                    transaction_id,
                    request.topic_data.unwrap_or_default(),
                    admission,
                )
                .await;

            return Ok(ProduceResponse::default()
                .responses(Some(responses))
                .throttle_time_ms(Some(0))
                .node_endpoints(None));
        }

        let responses = join_all(
            request
                .topic_data
                .unwrap_or_default()
                .into_iter()
                .map(|topic| self.topic(transaction_id, topic)),
        )
        .await;

        Ok(ProduceResponse::default()
            .responses(Some(responses))
            .throttle_time_ms(Some(0))
            .node_endpoints(None))
    }
}

/// kr: P12 (ADR-007 D2, I-P10). On an engine with the default `produce_staged` (stage 1 =
/// the whole produce), here a channel-backed fake that numbers batches in arrival order.
#[cfg(test)]
mod kr_p12_tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use bytes::Bytes;
    use nisshi_sans_io::record::{Record, deflated::Frame, inflated};
    use rama::extensions::Extensions;

    use super::*;
    use crate::{Request, RequestChannelService, bounded_channel, service::Response};

    /// A storage whose produce gives `next` offsets per topic partition, in arrival order.
    fn storage() -> RequestChannelService {
        storage_seen().0
    }

    /// Records of a batch the storage refuses with `REQUEST_TIMED_OUT` (like `Busy`).
    const REFUSED_RECORDS: usize = 7;

    /// `storage`, refusing batches of `REFUSED_RECORDS` records, with the record counts of
    /// the batches it was given, in arrival order.
    fn storage_seen() -> (RequestChannelService, Arc<std::sync::Mutex<Vec<i32>>>) {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (tx, mut rx) = bounded_channel(16);
        drop(tokio::spawn({
            let seen = Arc::clone(&seen);
            async move {
                let mut next: BTreeMap<Topition, i64> = BTreeMap::new();
                while let Some((request, reply)) = rx.recv().await {
                    if let Request::Produce {
                        topition, batch, ..
                    } = request
                    {
                        let records = batch.last_offset_delta + 1;
                        if let Ok(mut seen) = seen.lock() {
                            seen.push(records);
                        }
                        if records == REFUSED_RECORDS as i32 {
                            _ = reply.send(Response::Produce(Err(Error::Api(
                                ErrorCode::RequestTimedOut,
                            ))));
                            continue;
                        }
                        let at = next.entry(topition).or_default();
                        let offset = *at;
                        *at += i64::from(records);
                        _ = reply.send(Response::Produce(Ok(offset)));
                    }
                }
            }
        }));
        (RequestChannelService::new(tx), seen)
    }

    fn batch(value: &'static [u8], records: usize) -> Result<deflated::Batch> {
        let mut b = inflated::Batch::builder().last_offset_delta(records as i32 - 1);
        for i in 0..records {
            b = b.record(
                Record::builder()
                    .offset_delta(i as i32)
                    .value(Bytes::from_static(value).into()),
            );
        }
        b.build().and_then(TryInto::try_into).map_err(Into::into)
    }

    fn partition(index: i32, batches: Vec<deflated::Batch>) -> PartitionProduceData {
        PartitionProduceData::default()
            .index(index)
            .records(Some(Frame { batches }))
    }

    fn topic(name: &str, partitions: Vec<PartitionProduceData>) -> TopicProduceData {
        TopicProduceData::default()
            .name(name.into())
            .partition_data(Some(partitions))
    }

    fn admission() -> (Extensions, Arc<AtomicUsize>) {
        let admitted = Arc::new(AtomicUsize::new(0));
        let extensions = Extensions::default();
        _ = extensions.insert(ProduceAdmission::new({
            let admitted = Arc::clone(&admitted);
            move || {
                _ = admitted.fetch_add(1, Ordering::SeqCst);
            }
        }));
        (extensions, admitted)
    }

    /// Two topics, several batches per partition, an empty partition and a refused batch.
    fn request() -> Result<ProduceRequest> {
        let mut control = batch(b"c", 1)?;
        control.attributes |= 1 << 5;
        Ok(ProduceRequest::default().topic_data(Some(vec![
            topic(
                "a",
                vec![
                    partition(0, vec![batch(b"x", 2)?, batch(b"y", 3)?]),
                    partition(1, vec![batch(b"z", 1)?]),
                    PartitionProduceData::default().index(2),
                ],
            ),
            topic(
                "b",
                vec![
                    partition(0, vec![control]),
                    partition(1, vec![batch(b"v", 4)?]),
                ],
            ),
        ])))
    }

    #[tokio::test]
    async fn staged_path_answers_like_p3_and_admits_once() -> Result<()> {
        let upstream = ProduceService { storage: storage() }
            .serve(request()?)
            .await?;

        let (extensions, admitted) = admission();
        let staged = ProduceService { storage: storage() }
            .serve(RequestInput {
                request: request()?,
                extensions,
            })
            .await?;

        assert_eq!(1, admitted.load(Ordering::SeqCst));
        assert_eq!(upstream, staged);
        Ok(())
    }

    #[tokio::test]
    async fn a_partition_present_twice_is_staged_in_request_order() -> Result<()> {
        let (extensions, admitted) = admission();
        let request = ProduceRequest::default().topic_data(Some(vec![
            topic(
                "a",
                vec![
                    partition(0, vec![batch(b"x", 2)?, batch(b"y", 3)?]),
                    partition(1, vec![batch(b"z", 1)?]),
                    partition(0, vec![batch(b"w", 4)?]),
                ],
            ),
            topic("a", vec![partition(0, vec![batch(b"v", 1)?])]),
        ]));
        let response = ProduceService { storage: storage() }
            .serve(RequestInput {
                request,
                extensions,
            })
            .await?;
        assert_eq!(1, admitted.load(Ordering::SeqCst));
        let offsets = response
            .responses
            .iter()
            .flatten()
            .map(|t| {
                t.partition_responses
                    .iter()
                    .flatten()
                    .map(|p| (p.index, p.base_offset, p.error_code))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            vec![vec![(0, 0, 0), (1, 0, 0), (0, 5, 0)], vec![(0, 9, 0)]],
            offsets
        );
        Ok(())
    }

    /// A refusal stops the partition for the rest of the request: a later entry of the same
    /// partition is not staged (ADR-007 D2), another partition is.
    #[tokio::test]
    async fn a_refusal_stops_the_later_entries_of_the_partition() -> Result<()> {
        let (extensions, admitted) = admission();
        let request = ProduceRequest::default().topic_data(Some(vec![
            topic(
                "a",
                vec![
                    partition(0, vec![batch(b"x", 2)?, batch(b"r", REFUSED_RECORDS)?]),
                    partition(1, vec![batch(b"z", 1)?]),
                    partition(0, vec![batch(b"w", 4)?]),
                ],
            ),
            topic("a", vec![partition(0, vec![batch(b"v", 3)?])]),
        ]));
        let (storage, seen) = storage_seen();
        let response = ProduceService { storage }
            .serve(RequestInput {
                request,
                extensions,
            })
            .await?;
        assert_eq!(1, admitted.load(Ordering::SeqCst));
        let codes = response
            .responses
            .iter()
            .flatten()
            .map(|t| {
                t.partition_responses
                    .iter()
                    .flatten()
                    .map(|p| (p.index, p.error_code))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let timed_out = i16::from(ErrorCode::RequestTimedOut);
        assert_eq!(
            vec![
                vec![(0, timed_out), (1, 0), (0, timed_out)],
                vec![(0, timed_out)]
            ],
            codes
        );
        let mut seen = seen.lock().map(|s| s.clone()).unwrap_or_default();
        seen.sort_unstable();
        assert_eq!(
            vec![1, 2, REFUSED_RECORDS as i32],
            seen,
            "4 and 3 never staged"
        );
        Ok(())
    }
}

// #[cfg(all(test, feature = "dynostore"))]
// mod tests {
//     use super::*;
//     use crate::{Error, service::init_producer_id::InitProducerIdService};
//     use bytes::Bytes;
//     use nisshi_sans_io::{
//         ErrorCode, InitProducerIdRequest,
//         record::{
//             Record,
//             deflated::{self, Frame},
//             inflated,
//         },
//     };
//     use object_store::memory::InMemory;
//     use rama::Context;
//     use tracing::subscriber::DefaultGuard;

//     fn init_tracing() -> Result<DefaultGuard> {
//         use std::{fs::File, sync::Arc, thread};

//         use tracing::Level;
//         use tracing_subscriber::fmt::format::FmtSpan;

//         Ok(tracing::subscriber::set_default(
//             tracing_subscriber::fmt()
//                 .with_level(true)
//                 .with_line_number(true)
//                 .with_thread_names(false)
//                 .with_max_level(Level::DEBUG)
//                 .with_span_events(FmtSpan::ACTIVE)
//                 .with_writer(
//                     thread::current()
//                         .name()
//                         .ok_or(Error::Message(String::from("unnamed thread")))
//                         .and_then(|name| {
//                             File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
//                                 .map_err(Into::into)
//                         })
//                         .map(Arc::new)?,
//                 )
//                 .finish(),
//         ))
//     }

//     fn topic_data(
//         topic: &str,
//         index: i32,
//         builder: inflated::Builder,
//     ) -> Result<Option<Vec<TopicProduceData>>> {
//         builder
//             .build()
//             .and_then(deflated::Batch::try_from)
//             .map(|deflated| {
//                 let partition_data =
//                     PartitionProduceData::default()
//                         .index(index)
//                         .records(Some(Frame {
//                             batches: vec![deflated],
//                         }));

//                 Some(vec![
//                     TopicProduceData::default()
//                         .name(topic.into())
//                         .partition_data(Some(vec![partition_data])),
//                 ])
//             })
//             .map_err(Into::into)
//     }

//     #[tokio::test]
//     async fn non_txn_idempotent_unknown_producer_id() -> Result<()> {
//         let _guard = init_tracing()?;

//         let cluster = "abc";
//         let node = 12321;

//         let topic = "pqr";
//         let index = 0;

//         let transactional_id = None;
//         let acks = 0;
//         let timeout_ms = 0;

//         let storage = DynoStore::new(cluster, node, InMemory::new());
//         let ctx = Context::with_state(storage);
//         let service = ProduceService;

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::UnknownProducerId.into())
//                                 .base_offset(-1)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             service
//                 .serve(
//                     ctx,
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id)
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(
//                                     Record::builder().value(Bytes::from_static(b"lorem").into())
//                                 )
//                                 .producer_id(54345)
//                         )?)
//                 )
//                 .await?
//         );

//         Ok(())
//     }

//     #[tokio::test]
//     async fn non_txn_idempotent() -> Result<()> {
//         let _guard = init_tracing()?;

//         let cluster = "abc";
//         let node = 12321;
//         let topic = "pqr";
//         let index = 0;

//         let storage = DynoStore::new(cluster, node, InMemory::new());
//         let ctx = Context::with_state(storage);

//         let init_producer_id = InitProducerIdService;

//         let producer = init_producer_id
//             .serve(
//                 ctx.clone(),
//                 InitProducerIdRequest::default()
//                     .transactional_id(None)
//                     .transaction_timeout_ms(0)
//                     .producer_id(Some(-1))
//                     .producer_epoch(Some(-1)),
//             )
//             .await?;

//         let request = ProduceService;

//         let transactional_id = None;
//         let acks = 0;
//         let timeout_ms = 0;

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::None.into())
//                                 .base_offset(0)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx.clone(),
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id.clone())
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(Record::builder().value(
//                                     Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
//                                 ))
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::None.into())
//                                 .base_offset(1)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx.clone(),
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id.clone())
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(Record::builder().value(
//                                     Bytes::from_static(b"consectetur adipiscing elit").into()
//                                 ))
//                                 .record(
//                                     Record::builder()
//                                         .value(Bytes::from_static(b"sed do eiusmod tempor").into())
//                                 )
//                                 .base_sequence(1)
//                                 .last_offset_delta(1)
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::None.into())
//                                 .base_offset(3)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx,
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id.clone())
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(
//                                     Record::builder()
//                                         .value(Bytes::from_static(b"incididunt ut labore").into())
//                                 )
//                                 .base_sequence(3)
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         Ok(())
//     }

//     #[tokio::test]
//     async fn non_txn_idempotent_duplicate_sequence() -> Result<()> {
//         let _guard = init_tracing()?;

//         let cluster = "abc";
//         let node = 12321;
//         let topic = "pqr";
//         let index = 0;

//         let storage = DynoStore::new(cluster, node, InMemory::new());
//         let ctx = Context::with_state(storage);

//         let init_producer_id = InitProducerIdService;

//         let producer = init_producer_id
//             .serve(
//                 ctx.clone(),
//                 InitProducerIdRequest::default()
//                     .transactional_id(None)
//                     .transaction_timeout_ms(0)
//                     .producer_id(Some(-1))
//                     .producer_epoch(Some(-1)),
//             )
//             .await?;

//         let request = ProduceService;

//         let transactional_id = None;
//         let acks = 0;
//         let timeout_ms = 0;

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::None.into())
//                                 .base_offset(0)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx.clone(),
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id.clone())
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(Record::builder().value(
//                                     Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
//                                 ))
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::DuplicateSequenceNumber.into())
//                                 .base_offset(-1)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx,
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id)
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(Record::builder().value(
//                                     Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
//                                 ))
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         Ok(())
//     }

//     #[tokio::test]
//     async fn non_txn_idempotent_sequence_out_of_order() -> Result<()> {
//         let _guard = init_tracing()?;

//         let cluster = "abc";
//         let node = 12321;
//         let topic = "pqr";
//         let index = 0;

//         let storage = DynoStore::new(cluster, node, InMemory::new());
//         let ctx = Context::with_state(storage);

//         let init_producer_id = InitProducerIdService;

//         let producer = init_producer_id
//             .serve(
//                 ctx.clone(),
//                 InitProducerIdRequest::default()
//                     .transactional_id(None)
//                     .transaction_timeout_ms(0)
//                     .producer_id(Some(-1))
//                     .producer_epoch(Some(-1)),
//             )
//             .await?;

//         let request = ProduceService;

//         let transactional_id = None;
//         let acks = 0;
//         let timeout_ms = 0;

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::None.into())
//                                 .base_offset(0)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx.clone(),
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id.clone())
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(Record::builder().value(
//                                     Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
//                                 ))
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::OutOfOrderSequenceNumber.into())
//                                 .base_offset(-1)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx,
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id)
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(Record::builder().value(
//                                     Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
//                                 ))
//                                 .base_sequence(2)
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         Ok(())
//     }
// }
