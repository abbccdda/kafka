from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.utils.tiered_storage import TierSupport, TieredStorageMetricsRegistry
from kafkatest.version import DEV_BRANCH, KafkaVersion

import time

class TierBrokerBounceTest(ProduceConsumeValidateTest, TierSupport):
    """
    This test validates the recovery functionality of tier storage. During producer session, broker is brought down
    several times, to simulate various recovery flow - metadata recovery, leader transitions etc. At the end of the test
    the consumer makes sure none of the data which is acked gets lost or corrupted.
    Note: Due to various resource constraints of default system test environment, the test does not stress the system
    and uses relatively light workload (still useful for validating good part of recovery flow).
    A multi producer/consumer and topic/partition based test will be next step which can be run in soak like or multi
    physical node based system test environment.
    The test sets aggressive tiering parameters so that all of archiving/metadata and fetch path is tested.

    When running this test via Docker, the containers must be built such that AWS credentials
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
    MIN_RECORDS_PRODUCED = 10000
    DELAY_BETWEEN_RESTART_SEC = 10

    TOPIC_CONFIG = {
        "partitions": 20,
        "replication-factor": 3,
        "configs": {
            "min.insync.replicas": 1,
            "confluent.tier.enable": True
        }
    }

    def __init__(self, test_context):
        super(TierBrokerBounceTest, self).__init__(test_context=test_context)

        self.topic = "test-topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)

        self.kafka = KafkaService(test_context, num_nodes=3, zk=self.zk)
        self.configure_tiering(self.TIER_S3_BUCKET,
                               metadata_replication_factor=3,
                               log_segment_bytes=self.LOG_SEGMENT_BYTES)

        self.num_producers = 1
        self.num_consumers = 1
        self.session_timeout_sec = 60
        self.PRODUCER_REQUEST_TIMEOUT_SEC = 30
        self.consumption_timeout_sec = max(self.PRODUCER_REQUEST_TIMEOUT_SEC + 5, 2 * self.session_timeout_sec)
        self.iterations = 2
        self.produced = 0  # tracks the number of messages produced.

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        return super(TierBrokerBounceTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    def await_consumed_messages(self, consumer, min_messages=1):
        current_total = len(self.consumer.messages_consumed[1])
        wait_until(lambda: len(self.consumer.messages_consumed[1]) >= current_total + min_messages,
                   timeout_sec=self.consumption_timeout_sec,
                   err_msg="Timed out waiting for consumption")

    def rolling_bounce_broker(self, node, clean_shutdown=True):
        self.kafka.stop_node(node, clean_shutdown)
        time.sleep(self.DELAY_BETWEEN_RESTART_SEC)
        self.kafka.start_node(node)
        wait_until(lambda: len(self.kafka.isr_idx_list(self.topic, 0)) == 3,
                    timeout_sec=60,
                    err_msg="Timed out while waiting for broker to join")
        self.restart_jmx_tool()

    def diff_consumer(self):
        """
        Deep validation of data. Expects all the acked data to be returned via fetch. If fetch returns any extra data
        then it must be among unacked data.
        """
        consumer_data = set()
        producer_data = set()
        matches = True

        for ii, v in enumerate(self.consumer.messages_consumed[1]):
            if v in consumer_data:
                # Do not expect duplicate data as retry is disabled from producer.
                self.logger.error("duplicate_consumed_data {}".format(v))
                matches = False
            else:
                consumer_data.add(v)

        for ii, v in enumerate(self.producer.acked):
            if v in producer_data:
                self.logger.error("duplicate_produced_data {}".format(v))
                matches = False
            else:
                producer_data.add(v)

        if len(consumer_data) != len(producer_data):
            for v in consumer_data:
                if v not in producer_data:
                    if v not in self.producer.not_acked_values:
                        self.logger.error("consumer_data_not_produced {}".format(v))
                        matches = False
                    else:
                        self.logger.info("consumer_data_not_acked {}".format(v))
            for v in producer_data:
                if v not in consumer_data:
                    self.logger.error("producer_data_not_consumed {}".format(v))
                    matches = False
        return matches

    def consume(self, client_version):
        """
        Opens a consumer sessions, consumes all data and perform deep validation.
        """
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=60000, message_validator=is_int,
                                        version=KafkaVersion(client_version))
        self.consumer.start()
        self.consumer.wait()
        assert self.diff_consumer()
        self.consumer.stop()

    def stats(self):
        """
        Stats to report amount of data fetched from tier storage against total data read.
        Returns total log bytes and tier bytes fetched.
        """
        self.restart_jmx_tool()
        self.kafka.read_jmx_output_all_nodes()
        tier_bytes_fetched = self.kafka.maximum_jmx_value[str(TieredStorageMetricsRegistry.FETCHER_BYTES_FETCHED)]
        log_size_key = str(TieredStorageMetricsRegistry.log_local_size(self.topic, 0))
        log_segs_key = str(TieredStorageMetricsRegistry.num_log_segments(self.topic, 0))

        log_size = self.kafka.maximum_jmx_value[log_size_key]
        log_segments = self.kafka.maximum_jmx_value[log_segs_key]
        self.logger.info(
            "{} bytes fetched from S3 of {} total bytes, in {} segments".format(tier_bytes_fetched, log_size,
                                                                                log_segments))
        self.logger.info("log size {} Vs bytes fetched {}".format(log_size, tier_bytes_fetched))
        return (log_size, tier_bytes_fetched)

    def produced_message(self):
        now = len(self.producer.acked)
        self.logger.info("Produced {} bytes of data leading to size {}".format((now - self.produced), now))
        if now - self.produced >= self.MIN_RECORDS_PRODUCED:
            self.produced = now
            return True
        return False

    @matrix(client_version=[str(DEV_BRANCH)])
    def test_tier_broker_bounce(self, client_version):
        self.kafka.topics = {self.topic: self.TOPIC_CONFIG}
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=1000, message_validator=is_int,
                                           version=KafkaVersion(client_version))
        self.kafka.start()
        self.add_log_metrics(self.topic)
        self.restart_jmx_tool()
        self.producer.start()

        for ii in range(self.iterations):
            self.logger.info("Starting iteration count: %d producing initial message", ii+1)
            for jj, node in enumerate(self.kafka.nodes):
                wait_until(lambda: self.produced_message(),
                           timeout_sec=120, backoff_sec=1,
                           err_msg="Producer did not produce all messages in reasonable amount of time")
                self.rolling_bounce_broker(node, clean_shutdown=False)
                self.logger.info("*** End of rolling of node at index %d iteration %d *** \n", jj, ii + 1)
            self.logger.info("******* End of iteration %d ********", ii+1)

        self.producer.stop()
        # Will consume and validate data. Will assert if validation fails.
        self.consume(client_version)
        self.logger.info("Total produced: {} and total consumed: {}".format(
            len(self.producer.acked), len(self.consumer.messages_consumed[1])))
        log_size, tier_bytes_fetched = self.stats()

        # Make sure that atleast some of the fetch is from tier storage.
        assert (tier_bytes_fetched > 0)
