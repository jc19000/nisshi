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

use nisshi_sans_io::{
    ApiKey, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, NULL_TOPIC_ID, RequestInput,
    create_topics_response::CreatableTopicResult,
};
use rama::Service;
use tracing::{debug, instrument};

use crate::{Error, Result, Storage};

use super::metadata::is_valid_topic_name;

/// Build the [`CreatableTopicResult`] for a topic that was rejected, either
/// by storage or by validation performed before storage is ever consulted.
fn error_result(
    name: String,
    num_partitions: Option<i32>,
    replication_factor: Option<i16>,
    error_code: ErrorCode,
) -> CreatableTopicResult {
    CreatableTopicResult::default()
        .name(name)
        .topic_id(Some(NULL_TOPIC_ID))
        .error_code(error_code.into())
        .error_message(Some(error_code.to_string()))
        .topic_config_error_code(None)
        .num_partitions(num_partitions)
        .replication_factor(replication_factor)
        .configs(Some([].into()))
}

/// A [`Service`] using [`Storage`] as [`Context`] taking [`CreateTopicsRequest`] returning [`CreateTopicsResponse`].
/// ```no_run
/// use rama::Service as _;
/// use nisshi_sans_io::{NULL_TOPIC_ID, CreateTopicsRequest,
///     create_topics_request::CreatableTopic, ErrorCode};
/// use nisshi_storage::{CreateTopicsService, Error, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let storage = StorageContainer::builder()
///     .cluster_id("nisshi")
///     .node_id(111)
///     .advertised_listener(Url::parse("tcp://localhost:9092")?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let service = CreateTopicsService { storage };
///
/// let name = "abcba";
///
/// let response = service
///     .serve(
///         CreateTopicsRequest::default()
///             .topics(Some(vec![
///                 CreatableTopic::default()
///                     .name(name.into())
///                     .num_partitions(1)
///                     .replication_factor(3)
///                     .assignments(Some([].into()))
///                     .configs(Some([].into())),
///             ]))
///             .validate_only(Some(false)),
///     )
///     .await?;
///
/// let topics = response.topics.unwrap_or_default();
///
/// assert_eq!(1, topics.len());
/// assert_eq!(name, topics[0].name.as_str());
/// assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
/// assert_eq!(Some(1), topics[0].num_partitions);
/// assert_eq!(Some(3), topics[0].replication_factor);
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct CreateTopicsService<G> {
    pub storage: G,
}

impl<G> ApiKey for CreateTopicsService<G> {
    const KEY: i16 = CreateTopicsRequest::KEY;
}

impl<G, I> Service<I> for CreateTopicsService<G>
where
    G: Storage,
    I: Into<RequestInput<CreateTopicsRequest>> + Send + 'static,
{
    type Output = CreateTopicsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let mut topics = vec![];

        for mut topic in input.request.topics.unwrap_or_default() {
            let name = topic.name.clone();

            let num_partitions = Some(match topic.num_partitions {
                -1 => {
                    topic.num_partitions = 3;
                    topic.num_partitions
                }
                otherwise => otherwise,
            });

            // kr: P15 - the engine resolves the default; it always receives the effective
            // value, which is also the one answered.
            let replication_factor = Some(match topic.replication_factor {
                -1 => {
                    topic.replication_factor = self.storage.default_replication_factor();
                    topic.replication_factor
                }
                otherwise => otherwise,
            });

            // Validate before storage is ever consulted, so invalid topics
            // are rejected regardless of `validate_only`, and so a bad name
            // (e.g. "", ".", "..", or one containing "/") can never reach a
            // backend whose delete/list operations are prefix based.
            if !is_valid_topic_name(&name) {
                topics.push(error_result(
                    name,
                    num_partitions,
                    replication_factor,
                    ErrorCode::InvalidTopicException,
                ));
                continue;
            }

            // -1 (broker default) was already replaced above, so anything
            // below 1 is invalid.
            if num_partitions.is_some_and(|partitions| partitions < 1) {
                topics.push(error_result(
                    name,
                    num_partitions,
                    replication_factor,
                    ErrorCode::InvalidPartitions,
                ));
                continue;
            }

            match self
                .storage
                .create_topic(topic, input.request.validate_only.unwrap_or_default())
                .await
            {
                Ok(topic_id) => {
                    debug!(?topic_id);

                    topics.push(
                        CreatableTopicResult::default()
                            .name(name)
                            .topic_id(Some(topic_id.into_bytes()))
                            .error_code(ErrorCode::None.into())
                            .error_message(None)
                            .topic_config_error_code(Some(ErrorCode::None.into()))
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .configs(Some([].into())),
                    );
                }

                Err(Error::Api(error_code)) => topics.push(error_result(
                    name,
                    num_partitions,
                    replication_factor,
                    error_code,
                )),

                Err(error) => {
                    debug!(?error);

                    topics.push(
                        CreatableTopicResult::default()
                            .name(name)
                            .topic_id(Some(NULL_TOPIC_ID))
                            .error_code(ErrorCode::UnknownServerError.into())
                            .error_message(None)
                            .topic_config_error_code(None)
                            .num_partitions(None)
                            .replication_factor(None)
                            .configs(Some([].into())),
                    )
                }
            }
        }

        Ok(CreateTopicsResponse::default()
            .topics(Some(topics))
            .throttle_time_ms(Some(0)))
    }
}

// kr: P15 (ADR-006 amendment D3-RF)
#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
        time::{Duration, SystemTime},
    };

    use async_trait::async_trait;
    use nisshi_sans_io::{
        ConfigResource, IsolationLevel, ListOffset, ScramMechanism,
        create_topics_request::CreatableTopic, delete_groups_response::DeletableGroupResult,
        delete_records_request::DeleteRecordsTopic,
        delete_records_response::DeleteRecordsTopicResult,
        describe_cluster_response::DescribeClusterBroker,
        describe_configs_response::DescribeConfigsResult,
        describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
        fetch_response::AbortedTransaction,
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
        list_groups_response::ListedGroup, record::deflated,
        txn_offset_commit_response::TxnOffsetCommitResponseTopic,
    };
    use url::Url;
    use uuid::Uuid;

    use super::*;
    use crate::{
        BrokerRegistrationRequest, GroupDetail, ListOffsetResponse, MetadataResponse,
        NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, ScramCredential,
        TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
        TxnOffsetCommitRequest, UpdateError, Version,
    };

    /// Records the topics it is asked to create; `rf` is its default replication factor.
    #[derive(Debug, Default)]
    struct Recorder {
        rf: i16,
        received: Mutex<Vec<CreatableTopic>>,
    }

    #[async_trait]
    impl Storage for Recorder {
        async fn register_broker(
            &self,
            _broker_registration: BrokerRegistrationRequest,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
            unimplemented!()
        }

        async fn create_topic(&self, topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
            self.received.lock()?.push(topic);
            Ok(Uuid::nil())
        }

        async fn delete_records(
            &self,
            _topics: &[DeleteRecordsTopic],
        ) -> Result<Vec<DeleteRecordsTopicResult>> {
            unimplemented!()
        }

        async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
            unimplemented!()
        }

        async fn incremental_alter_resource(
            &self,
            _resource: AlterConfigsResource,
        ) -> Result<AlterConfigsResourceResponse> {
            unimplemented!()
        }

        async fn produce(
            &self,
            _transaction_id: Option<&str>,
            _topition: &Topition,
            _deflated: deflated::Batch,
        ) -> Result<i64> {
            unimplemented!()
        }

        async fn fetch(
            &self,
            _topition: &Topition,
            _offset: i64,
            _min_bytes: u32,
            _max_bytes: u32,
            _isolation_level: IsolationLevel,
            _max_wait: Duration,
        ) -> Result<Vec<deflated::Batch>> {
            unimplemented!()
        }

        async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
            unimplemented!()
        }

        async fn offset_commit(
            &self,
            _group: &str,
            _retention: Option<Duration>,
            _offsets: &[(Topition, OffsetCommitRequest)],
        ) -> Result<Vec<(Topition, ErrorCode)>> {
            unimplemented!()
        }

        async fn committed_offset_topitions(
            &self,
            _group_id: &str,
        ) -> Result<BTreeMap<Topition, i64>> {
            unimplemented!()
        }

        async fn offset_fetch(
            &self,
            _group_id: Option<&str>,
            _topics: &[Topition],
            _require_stable: Option<bool>,
        ) -> Result<BTreeMap<Topition, i64>> {
            unimplemented!()
        }

        async fn list_offsets(
            &self,
            _isolation_level: IsolationLevel,
            _offsets: &[(Topition, ListOffset)],
        ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
            unimplemented!()
        }

        async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
            unimplemented!()
        }

        async fn describe_config(
            &self,
            _name: &str,
            _resource: ConfigResource,
            _keys: Option<&[String]>,
        ) -> Result<DescribeConfigsResult> {
            unimplemented!()
        }

        async fn describe_topic_partitions(
            &self,
            _topics: Option<&[TopicId]>,
            _partition_limit: i32,
            _cursor: Option<Topition>,
        ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
            unimplemented!()
        }

        async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
            unimplemented!()
        }

        async fn delete_groups(
            &self,
            _group_ids: Option<&[String]>,
        ) -> Result<Vec<DeletableGroupResult>> {
            unimplemented!()
        }

        async fn describe_groups(
            &self,
            _group_ids: Option<&[String]>,
            _include_authorized_operations: bool,
        ) -> Result<Vec<NamedGroupDetail>> {
            unimplemented!()
        }

        async fn update_group(
            &self,
            _group_id: &str,
            _detail: GroupDetail,
            _version: Option<Version>,
        ) -> Result<Version, UpdateError<GroupDetail>> {
            unimplemented!()
        }

        async fn init_producer(
            &self,
            _transaction_id: Option<&str>,
            _transaction_timeout_ms: i32,
            _producer_id: Option<i64>,
            _producer_epoch: Option<i16>,
        ) -> Result<ProducerIdResponse> {
            unimplemented!()
        }

        async fn txn_add_offsets(
            &self,
            _transaction_id: &str,
            _producer_id: i64,
            _producer_epoch: i16,
            _group_id: &str,
        ) -> Result<ErrorCode> {
            unimplemented!()
        }

        async fn txn_add_partitions(
            &self,
            _partitions: TxnAddPartitionsRequest,
        ) -> Result<TxnAddPartitionsResponse> {
            unimplemented!()
        }

        async fn txn_offset_commit(
            &self,
            _offsets: TxnOffsetCommitRequest,
        ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
            unimplemented!()
        }

        async fn txn_end(
            &self,
            _transaction_id: &str,
            _producer_id: i64,
            _producer_epoch: i16,
            _committed: bool,
        ) -> Result<ErrorCode> {
            unimplemented!()
        }

        async fn maintain(&self, _now: SystemTime) -> Result<()> {
            unimplemented!()
        }

        async fn maintain_transactions(&self, _now: SystemTime) -> Result<()> {
            unimplemented!()
        }

        async fn aborted_transactions(
            &self,
            _topition: &Topition,
            _offset: i64,
            _last_stable_offset: i64,
        ) -> Result<Vec<AbortedTransaction>> {
            unimplemented!()
        }

        async fn cluster_id(&self) -> Result<String> {
            unimplemented!()
        }

        async fn node(&self) -> Result<i32> {
            unimplemented!()
        }

        async fn advertised_listener(&self) -> Result<Url> {
            unimplemented!()
        }

        async fn ping(&self) -> Result<()> {
            unimplemented!()
        }

        async fn delete_user_scram_credential(
            &self,
            _user: &str,
            _mechanism: ScramMechanism,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn upsert_user_scram_credential(
            &self,
            _user: &str,
            _mechanism: ScramMechanism,
            _credential: ScramCredential,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn user_scram_credential(
            &self,
            _user: &str,
            _mechanism: ScramMechanism,
        ) -> Result<Option<ScramCredential>> {
            unimplemented!()
        }

        fn default_replication_factor(&self) -> i16 {
            self.rf
        }
    }

    async fn create<G: Storage>(storage: G, rf: i16) -> Result<(i16, Option<i16>)> {
        let service = CreateTopicsService { storage };
        let response = service
            .serve(
                CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name("t".into())
                            .num_partitions(1)
                            .replication_factor(rf)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                    ]))
                    .validate_only(Some(false)),
            )
            .await?;
        let topics = response.topics.unwrap_or_default();
        Ok((topics[0].error_code, topics[0].replication_factor))
    }

    #[tokio::test]
    async fn default_replication_factor_comes_from_the_engine() -> Result<()> {
        let three = Arc::new(Recorder {
            rf: 3,
            ..Default::default()
        });
        assert_eq!(create(Arc::clone(&three), -1).await?, (0, Some(3)));
        assert_eq!(create(Arc::clone(&three), 2).await?, (0, Some(2)));
        let got: Vec<i16> = three
            .received
            .lock()?
            .iter()
            .map(|t| t.replication_factor)
            .collect();
        assert_eq!(got, vec![3, 2], "the engine receives the effective value");
        // The trait default keeps the upstream behaviour, through `Box` too.
        let one: Box<dyn Storage> = Box::new(Recorder {
            rf: 1,
            ..Default::default()
        });
        assert_eq!(one.default_replication_factor(), 1);
        assert_eq!(create(one, -1).await?, (0, Some(1)));
        Ok(())
    }
}
