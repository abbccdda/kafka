# Copyright 2020 Confluent Inc.

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.utils.tiered_storage import TierSupport, \
    TieredStorageMetricsRegistry, S3_BACKEND, GCS_BACKEND
from kafkatest.version import DEV_BRANCH, KafkaVersion

import time
import random
import os


class TierMetadataCorruptionTest(ProduceConsumeValidateTest, TierSupport):
    """
    This test aims to test the recovery logic of the Tiered storage metadata
    arrangement by injecting various I/O errors. Initially, this test will
    permanently delete some of the tierstate related files on the brokers
    and verify that the recovery happens properly.

    When running this test via Docker, the containers must be built such that
    AWS and GCS credentials are available to the broker at runtime.
    Also, the following command assumes that the GCS credential file is present
    at tests/docker/gcs_arg.json (tests/docker being the docker root):
    $ docker_args="\
      --build-arg aws_access_key_id=$(aws configure get aws_access_key_id) \
      --build-arg aws_secret_access_key=$(aws configure get aws_secret_access_key) \
      --build-arg gcs_credentials_file=gcs_arg.json \
      --build-arg azure_credentials_file=<PATH_TO_AZURE_CREDENTIALS>" \
      ./tests/docker/ducker-ak up
    """
    # The value of log.segment.bytes and number of records to produce
    # should be set such that multiple segments are rolled,
    # tiered to S3 and deleted from the local log.
    LOG_SEGMENT_BYTES = 100 * 1024
    MIN_RECORDS_PRODUCED = 1000
    DELAY_BETWEEN_RESTART_SEC = 10
    BROKER_COUNT = 3
    PARTITION_COUNT = 20

    FILE_EXTS_FOR_DELETION = ["tierstate", "tierstate.mutable"]

    TOPIC_CONFIG = {
        "partitions": PARTITION_COUNT,
        "replication-factor": BROKER_COUNT,
        "configs": {
            "min.insync.replicas": 2,
            "confluent.tier.enable": True
        }
    }

    def __init__(self, test_context):
        super(TierMetadataCorruptionTest, self).__init__(
            test_context=test_context)

        self.topic_name = "user_topic_test"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=self.BROKER_COUNT,
                                  zk=self.zk)

        self.num_producers = 1
        self.num_consumers = 1
        self.session_timeout_sec = 60
        self.PRODUCER_REQUEST_TIMEOUT_SEC = 30
        self.consumption_timeout_sec = max(
            self.PRODUCER_REQUEST_TIMEOUT_SEC + 5,
            2 * self.session_timeout_sec)
        self.iterations = 3
        self.produced = 0  # tracks the number of messages produced.

    def setUp(self):
        self.zk.start()

    def teardown(self):
        self.kafka.stop()
        self.zk.stop()

    def min_cluster_size(self):
        return super(TierMetadataCorruptionTest, self).min_cluster_size() \
            + self.num_producers + self.num_consumers

    def await_consumed_messages(self, consumer, min_messages=1):
        current_total = len(self.consumer.messages_consumed[1])
        wait_until(lambda: len(
            self.consumer.messages_consumed[1]) >= current_total + min_messages,
            timeout_sec=self.consumption_timeout_sec,
            err_msg="Timed out waiting for consumption")

    def rolling_bounce_broker(self, node, partition_id, clean_shutdown=True):
        self.kafka.stop_node(node, clean_shutdown)
        time.sleep(self.DELAY_BETWEEN_RESTART_SEC)
        self.kafka.start_node(node)
        wait_until(lambda: len(self.kafka.isr_idx_list(self.topic_name,
                                                       partition_id)) == self.BROKER_COUNT,
                   timeout_sec=60,
                   err_msg="Timed out while waiting for broker to join")
        self.restart_jmx_tool()

    def diff_consumer(self):
        """
        Deep validation of data. Expects all the acked data to be returned via
        fetch. If fetch returns any extra data then it must be among
        unacked data.
        """
        consumer_data = set()
        producer_data = set()
        matches = True

        for ii, v in enumerate(self.consumer.messages_consumed[1]):
            if v in consumer_data:
                # Do not expect duplicate data as retry is disabled from producer
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
                        self.logger.error(
                            "consumer_data_not_produced {}".format(v))
                        matches = False
                    else:
                        self.logger.info(
                            "consumer_data_not_acked {}".format(v))
            for v in producer_data:
                if v not in consumer_data:
                    self.logger.error(
                        "producer_data_not_consumed {}".format(v))
                    matches = False
        return matches

    def consume(self, client_version):
        """
        Opens a consumer sessions, consumes all data and perform deep validation.
        """
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers,
                                        self.kafka,
                                        self.topic_name, consumer_timeout_ms=60000,
                                        message_validator=is_int,
                                        version=KafkaVersion(client_version))
        self.consumer.start()
        self.consumer.wait()
        if not self.diff_consumer():
            self.logger.error("Consumer didn't consume all data properly!")
        self.consumer.stop()

    def stats(self):
        """
        Stats to report amount of data fetched from tier storage against total data read.
        Returns total log bytes and tier bytes fetched.
        """
        self.restart_jmx_tool()
        self.kafka.read_jmx_output_all_nodes()
        tier_bytes_fetched = self.kafka.maximum_jmx_value[str(
            TieredStorageMetricsRegistry.FETCHER_BYTES_FETCHED)]
        log_size_key = str(
            TieredStorageMetricsRegistry.log_local_size(self.topic_name, 0))
        log_segs_key = str(
            TieredStorageMetricsRegistry.num_log_segments(self.topic_name, 0))

        log_size = self.kafka.maximum_jmx_value[log_size_key]
        log_segments = self.kafka.maximum_jmx_value[log_segs_key]
        self.logger.info(
            "{} bytes fetched from S3 of {} total bytes, in {} segments".format(
                tier_bytes_fetched, log_size,
                log_segments))
        self.logger.info("log size {} Vs bytes fetched {}".format(
            log_size, tier_bytes_fetched))
        return (log_size, tier_bytes_fetched)

    def produced_message(self):
        now = len(self.producer.acked)
        self.logger.info("Produced {} bytes of data leading to size {}".format(
            (now - self.produced), now))
        if now - self.produced >= self.MIN_RECORDS_PRODUCED:
            self.produced = now
            return True
        return False

    def _generate_unavailable_file_path(self, partition_id, file_ext):
        """
        This method will generate the file path for which the File_Not_Available
        error would be injected
        :param file_ext: the ext for which the wild card would be generated
        :param partition_id: the partition id
        :return: fully qualified file path name on the local broker
        """
        folder_name = "%s-%d" % (self.topic_name, partition_id)
        folder_path = os.path.join(self.kafka.DATA_LOG_DIR_1, folder_name)
        return os.path.join(folder_path, "*.%s" % file_ext)

    def _make_file_unavailable(self, node, partition_id):
        """
        Responsible for deleting the necessary files
        :param node: broker node instance
        :param partition_id: partition id
        :return:
        """
        all_file_paths = [self._generate_unavailable_file_path(partition_id,
                                                               ext)
                          for ext in self.FILE_EXTS_FOR_DELETION]
        self.logger.info("Deleting files: %s for node: %s" %
                         (", ".join(all_file_paths), node.name))

        node.account.ssh("rm -f -- %s" % " ".join(all_file_paths),
                         allow_fail=False)

    @matrix(client_version=[str(DEV_BRANCH)], backend=[S3_BACKEND])
    def test_tier_metadata_corruption(self, client_version, backend):
        self.configure_tiering(backend,
                               metadata_replication_factor=self.BROKER_COUNT,
                               log_segment_bytes=self.LOG_SEGMENT_BYTES)
        self.kafka.topics = {self.topic_name: self.TOPIC_CONFIG}
        self.kafka.start()

        self.producer = VerifiableProducer(self.test_context,
                                           self.num_producers,
                                           self.kafka, self.topic_name,
                                           throughput=1000,
                                           message_validator=is_int,
                                           version=KafkaVersion(client_version))
        self.add_log_metrics(self.topic_name)
        self.restart_jmx_tool()
        self.producer.start()

        for _ in xrange(self.iterations):
            # Wait for new data to be produced in every iteration
            wait_until(lambda: self.produced_message(),
                       timeout_sec=120, backoff_sec=1,
                       err_msg="Producer did not produce all messages "
                               "in reasonable amount of time")
            # Choose a random partition
            partition_id = random.randint(0, self.PARTITION_COUNT - 1)
            # Pick the leader of that partition
            leader_node = self.kafka.leader(self.topic_name, partition_id)
            # Making the file unavailable resulting in the crash of the node
            self._make_file_unavailable(leader_node, partition_id)
            # Continuing to restart the broker
            self.rolling_bounce_broker(leader_node, partition_id,
                                       clean_shutdown=False)

        # Test success depends on consumer successfully
        # reading back all the data from all the nodes
        self.producer.stop()
        # Will consume and validate data. Assertion error if validation fails.
        self.consume(client_version)
        self.logger.info("Total produced: {} and total consumed: {}".format(
            len(self.producer.acked), len(self.consumer.messages_consumed[1])))

        # Make sure that at least some of the fetch is from tiered storage.
        _, tier_bytes_fetched = self.stats()
        if not (tier_bytes_fetched > 0):
            self.logger.error("Consumer didn't read any tiered data")
