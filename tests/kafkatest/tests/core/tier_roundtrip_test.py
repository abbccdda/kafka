from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.utils.tiered_storage import TierSupport, TieredStorageMetricsRegistry
from kafkatest.version import LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0, LATEST_1_1, LATEST_2_0, \
    LATEST_2_1, LATEST_2_2, DEV_BRANCH, KafkaVersion


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

    TIER_S3_BUCKET = "confluent-tier-system-test"
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
        self.configure_tiering(self.TIER_S3_BUCKET,
                               metadata_replication_factor=1,
                               log_segment_bytes=self.LOG_SEGMENT_BYTES)

        self.num_producers = 1
        self.num_consumers = 1

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

    @matrix(client_version=[str(DEV_BRANCH), str(LATEST_2_2), str(LATEST_2_1), str(LATEST_2_0), str(LATEST_1_1),
                            str(LATEST_1_0), str(LATEST_0_11_0), str(LATEST_0_10_2), str(LATEST_0_10_1)])
    def test_tier_roundtrip(self, client_version):
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
            timeout_sec=120, backoff_sec=1,
            err_msg="Producer did not produce all messages in reasonable amount of time")

        self.producer.stop()

        self.add_log_metrics(self.topic)
        self.restart_jmx_tool()

        wait_until(lambda: self.tiering_completed(self.topic),
                   timeout_sec=60, backoff_sec=2, err_msg="archive did not complete within timeout")

        self.consumer.start()
        self.consumer.wait()
        self.validate()

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
