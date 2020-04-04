from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.utils.tiered_storage import TierSupport, TieredStorageMetricsRegistry, S3_BACKEND, GCS_BACKEND
from kafkatest.version import LATEST_0_9, LATEST_0_10_0, LATEST_0_10_1, \
    LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0, LATEST_1_1, LATEST_2_0, \
    LATEST_2_1, LATEST_2_2, LATEST_2_3, LATEST_2_4, DEV_BRANCH, KafkaVersion

import time

def verify_offset_for_times_data(producer_data, consumer_data):
    """
    Verify that the timestamp to offset information extracted by a consumer matches that from a producer. Producers
    can collect this information while producing a record and consumers collect this information using their API call
    offsetsForTimes()
    Input data is expected as nested dictionaries. {<TopicPartition> : {<timestamp> : <offset>}}
    :param producer_data: timestamp to offset information specific to topic partition(s), provided by a producer
    :param consumer_data: timestamp to offset information specific to topic partition(s), provided by a consumer
    """
    msg = "Mismatch at offsetForTimes verification: "
    for tp in consumer_data.keys():
        assert tp in producer_data, (msg + "Consumer read from partition: %s but producer did not write to it" % tp)
        for ts, off in consumer_data[tp].iteritems():
            assert ts in producer_data[tp], (msg + "Producer did not produce any record at timestamp: %s for partition: %s " % (ts, tp))
            assert producer_data[tp][ts] == off, (msg + "Partition %s :: Producer(ts: %s, offset: %s) Consumer(ts: %s, offset: %s)"
                                                  % (tp, ts, producer_data[tp][ts], ts, off))

class TierRoundtripTest(ProduceConsumeValidateTest, TierSupport):
    """
    This test validates that brokers can archive to S3 and consumers can fetch all records,
    including those which were tiered to S3.

    When running this test via Ducker, the containers must be built such that AWS credentials
    for `TIER_S3_BUCKET` are available to the broker at runtime:
    $ docker_args="\
      --build-arg aws_access_key_id=$(aws configure get aws_access_key_id) \
      --build-arg aws_secret_access_key=$(aws configure get aws_secret_access_key)" \
      ./tests/docker/ducker-ak up
    """

    # The value of log.segment.bytes and number of records to produce should be set such that
    # multiple segments are rolled, tiered to S3 and deleted from the local log.
    LOG_SEGMENT_BYTES = 100 * 1024
    MIN_RECORDS_PRODUCED = 25000

    TOPIC_CONFIG = {
        "partitions": 1,
        "replication-factor": 1,
        "configs": {
            "min.insync.replicas": 1,
            "confluent.tier.enable": True
        }
    }

    def __init__(self, test_context):
        super(TierRoundtripTest, self).__init__(test_context=test_context)

        self.zk = ZookeeperService(test_context, num_nodes=1)

        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk)

        self.num_producers = 1
        self.num_consumers = 1
        self.num_verifiable_consumers = 1

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        return super(TierRoundtripTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    def restart_jmx_tool(self):
        for knode in self.kafka.nodes:
            knode.account.kill_java_processes(self.kafka.jmx_class_name(), clean_shutdown=False, allow_fail=True)
            idx = self.kafka.idx(knode)
            self.kafka.started[idx-1] = False
            self.kafka.start_jmx_tool(idx, knode)

    @matrix(client_version=[str(DEV_BRANCH), str(LATEST_2_4), str(LATEST_2_3), str(LATEST_2_2), str(LATEST_2_1), str(LATEST_2_0),
                            str(LATEST_1_1), str(LATEST_1_0), str(LATEST_0_11_0), str(LATEST_0_10_2), str(LATEST_0_10_1),
                            str(LATEST_0_10_0), str(LATEST_0_9)],
            prefer_tier_fetch=[False],
            backend=[S3_BACKEND, GCS_BACKEND])
    @matrix(client_version=[str(DEV_BRANCH), str(LATEST_0_9)],
            prefer_tier_fetch=[True],
            backend=[S3_BACKEND])
    def test_tier_roundtrip(self, client_version, prefer_tier_fetch, backend):
        if prefer_tier_fetch:
            hotset_ms = -1
            hotset_bytes = -1
            prefer_tier_fetch_ms = 0
        else:
            hotset_ms = 1
            hotset_bytes = 1
            prefer_tier_fetch_ms = -1

        self.configure_tiering(backend, metadata_replication_factor=1, log_segment_bytes=self.LOG_SEGMENT_BYTES,
                hotset_ms=hotset_ms, hotset_bytes=hotset_bytes, prefer_tier_fetch_ms=prefer_tier_fetch_ms)

        self.topic = "test-topic"
        
        self.kafka.topics = {self.topic: self.TOPIC_CONFIG}

        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=1000, message_validator=is_int,
                                           version=KafkaVersion(client_version))

        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=60000, message_validator=is_int,
                                        version=KafkaVersion(client_version))

        self.kafka.start()
        self.producer.start()

        wait_until(lambda: self.producer.each_produced_at_least(self.MIN_RECORDS_PRODUCED),
            timeout_sec=180, backoff_sec=1,
            err_msg="Producer did not produce all messages in reasonable amount of time")
        producer_data = self.producer.offset_for_times_data
        end_offsets = self.producer.last_acked_offsets
        self.producer.stop()

        self.add_log_metrics(self.topic)
        self.restart_jmx_tool()

        if prefer_tier_fetch:
            wait_until(lambda: self.tiering_completed_prefer_fetch(self.topic, self.LOG_SEGMENT_BYTES),
                    timeout_sec=180, backoff_sec=2, err_msg="archive did not complete within timeout")
        else:
            wait_until(lambda: self.tiering_completed(self.topic),
                    timeout_sec=180, backoff_sec=2, err_msg="archive did not complete within timeout")

        self.consumer.start()
        self.consumer.wait()
        self.validate()

        if client_version == DEV_BRANCH:
            self.verifiable_consumer = VerifiableConsumer(self.test_context, self.num_verifiable_consumers, self.kafka, self.topic,
                                                            group_id = "test_consumer_group", static_membership=False,
                                                            max_messages=-1, session_timeout_sec=30, enable_autocommit=False,
                                                            assignment_strategy=None, version=KafkaVersion(client_version),
                                                            stop_timeout_sec=30, log_level="INFO", jaas_override_variables=None,
                                                            on_record_consumed=None, reset_policy="earliest", verify_offsets=False,
                                                            send_offset_for_times_data=True)
            self.verifiable_consumer.start()
            def verifiable_consumer_done():
                for partition in range(self.kafka.topics[self.topic]["partitions"]):
                    if end_offsets[(self.topic, partition)] > self.verifiable_consumer.current_position((self.topic, partition)):
                        return False
                return True

            wait_until(verifiable_consumer_done,
                       timeout_sec=180, backoff_sec=5, err_msg="VerifiableConsumer did not finish in reasonable amount of time")
            verify_offset_for_times_data(producer_data, self.verifiable_consumer.offset_for_times_data)

        self.kafka.read_jmx_output_all_nodes()
        tier_bytes_fetched = self.kafka.maximum_jmx_value[str(TieredStorageMetricsRegistry.FETCHER_BYTES_FETCHED)]
        log_size_key = str(TieredStorageMetricsRegistry.log_local_size(self.topic, 0))
        log_segs_key = str(TieredStorageMetricsRegistry.num_log_segments(self.topic, 0))

        log_size = self.kafka.maximum_jmx_value[log_size_key]
        log_segments = self.kafka.maximum_jmx_value[log_segs_key]
        self.logger.info("{} bytes fetched from S3 of {} total bytes, in {} segments".format(tier_bytes_fetched, log_size, log_segments))
        bytes_fetched_from_local_log = log_size - tier_bytes_fetched

        print("log size", log_size, "bytes fetched", tier_bytes_fetched)
        print("fetched local log", bytes_fetched_from_local_log, "seg bytes", self.LOG_SEGMENT_BYTES)

        assert bytes_fetched_from_local_log <= self.LOG_SEGMENT_BYTES
        assert self.check_cluster_state()
