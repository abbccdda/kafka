import json

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.utils.tiered_storage import tier_set_configs, TierSupport, TieredStorageMetric, TieredStorageMetricsRegistry, S3_BACKEND, GCS_BACKEND
from kafkatest.version import DEV_BRANCH, KafkaVersion
from kafkatest.services.kafka.util import fix_opts_for_new_jvm

import uuid
import time

class TierMetadataPartitionFencingTest(ProduceConsumeValidateTest, TierSupport):
    """
    This test sets up tiered storage archival workload for a test topic, then intentionally triggers fencing on a
    partition of the tested topic. The test asserts that the fencing event is reported via JMX metrics, and also that
    the controller is able to delete the topic whose partition was fenced.

    When running this test via Ducker, the containers must be built such that AWS credentials
    for `TIER_S3_BUCKET` are available to the broker at runtime:
    $ docker_args="\
      --build-arg aws_access_key_id=$(aws configure get aws_access_key_id) \
      --build-arg aws_secret_access_key=$(aws configure get aws_secret_access_key)" \
      --build-arg gcs_credentials_file=gcs_arg.json" \
      ./tests/docker/ducker-ak up
    """

    # The value of log.segment.bytes and number of records to produce should be set such that
    # multiple segments are rolled, tiered to S3 and deleted from the local log.
    LOG_SEGMENT_BYTES = 100 * 1024
    MIN_RECORDS_PRODUCED = 25000
    BROKER_COUNT = 3
    PARTITION_COUNT = 5
    PARTITION_ID_TO_BE_FENCED = 2
    TIER_BUCKET_PREFIX = "system-test-run-" + str(int(round(time.time() * 1000))) + "-" + str(uuid.uuid4()) + "/"

    TOPIC_CONFIG = {
        "partitions": PARTITION_COUNT,
        "replication-factor": 3,
        "configs": {
            "min.insync.replicas": 2,
            "confluent.tier.enable": True
        }
    }

    def __init__(self, test_context):
        super(TierMetadataPartitionFencingTest, self).__init__(test_context=test_context)

        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=self.BROKER_COUNT, zk=self.zk)
        self.num_producers = 1
        self.num_consumers = 1

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        return super(TierMetadataPartitionFencingTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    def topic_id(self, partition):
        self.logger.debug(
            "Querying zookeeper to find assigned topic ID for topic %s and partition %d" % (self.topic, partition))
        zk_path = "/brokers/topics/%s" % self.topic
        topic_info_json = self.kafka.zk.query(zk_path, chroot=self.kafka.zk_chroot)

        if topic_info_json is None:
            raise Exception("Error finding state for topic %s (partition %d)." % (self.topic, partition))

        topic_info = json.loads(topic_info_json)
        self.logger.info(topic_info)
        topic_id = topic_info["confluent_topic_id"]
        self.logger.info("Topic ID assigned for topic %s is %s (partition %d)" % (self.topic, topic_id, partition))
        return topic_id

    def trigger_fencing(self, partition):
        topic_id = self.topic_id(partition)
        node = self.kafka.nodes[0]

        cmd = fix_opts_for_new_jvm(node)
        cmd += self.kafka.path.script("kafka-run-class.sh", node)
        cmd += " kafka.tier.tools.TierPartitionStateFencingTrigger"
        cmd += " --bootstrap-servers %s --tiered-partition-topic-name %s --tiered-partition-name %d" % (
            self.kafka.bootstrap_servers(self.kafka.security_protocol), self.topic, partition)
        cmd += " --tiered-partition-topic-id %s dangerous-fence-via-delete-event" % topic_id
        cmd += " 2>> %s/trigger_fencing.log" % KafkaService.PERSISTENT_ROOT
        cmd += " | tee -a %s/trigger_fencing.log &" % KafkaService.PERSISTENT_ROOT
        output = ""
        self.logger.debug(cmd)
        for line in node.account.ssh_capture(cmd):
            output += line
        self.logger.debug(output)

    def check_topic_deleted(self):
        topic_list_generator = self.kafka.list_topics()
        for topic in topic_list_generator:
            if self.topic in topic:
                return False
        return True

    @matrix(client_version=[str(DEV_BRANCH)], backend=[S3_BACKEND, GCS_BACKEND])
    def test_tier_metadata_partition_fencing(self, client_version, backend):
        # 1. Setup tiering
        self.kafka.jmx_object_names = TieredStorageMetricsRegistry.ALL_MBEANS
        self.kafka.jmx_attributes = TieredStorageMetricsRegistry.ALL_ATTRIBUTES
        tier_set_configs(
            self.kafka, backend, metadata_replication_factor=self.BROKER_COUNT,
            log_segment_bytes=self.LOG_SEGMENT_BYTES, hotset_ms=1, hotset_bytes=1,
            metadata_num_partitions=1, tier_bucket_prefix=self.TIER_BUCKET_PREFIX)

        self.topic = "test-topic"
        self.kafka.topics = {self.topic: self.TOPIC_CONFIG}

        # 2. Produce data
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=1000, message_validator=is_int,
                                           version=KafkaVersion(client_version))
        self.kafka.start()
        self.producer.start()
        wait_until(lambda: self.producer.each_produced_at_least(self.MIN_RECORDS_PRODUCED),
                   timeout_sec=180, backoff_sec=1,
                   err_msg="Producer did not produce all messages in reasonable amount of time")

        # 3. Trigger fencing on a partition. Then produce more data post fencing, eventually stop the producer.
        self.restart_jmx_tool()
        assert (self.check_fenced_partitions(0))
        fenced_partitions = set([self.PARTITION_ID_TO_BE_FENCED])
        for partition in fenced_partitions:
            self.trigger_fencing(partition)
        wait_until(lambda: self.check_fenced_partitions(len(fenced_partitions)),
                   timeout_sec=600, backoff_sec=2, err_msg="num fenced partitions was not reported as 1")
        wait_until(lambda: self.producer.each_produced_at_least(self.MIN_RECORDS_PRODUCED * 2),
                   timeout_sec=360, backoff_sec=1,
                   err_msg="Producer did not produce all messages in reasonable amount of time")
        self.producer.stop()

        # 4. Ensure tiering is complete, and fenced partitions metric remains the same as before.
        self.add_log_metrics(self.topic, range(0, self.PARTITION_COUNT))
        self.restart_jmx_tool()
        partitions_without_error = list(set(range(0, self.PARTITION_COUNT)) - fenced_partitions)
        wait_until(
            lambda: self.tiering_completed(
                self.topic, partitions=partitions_without_error, ignore_error_partitions=True),
            timeout_sec=360,
            backoff_sec=2,
            err_msg="archive did not complete within timeout for partitions without error")
        assert (self.check_fenced_partitions(len(fenced_partitions)))

        # 5. Verify that produced data can be read by the consumer
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=60000, message_validator=is_int,
                                        version=KafkaVersion(client_version))
        self.consumer.start()
        self.consumer.wait()
        self.validate()

        # 6. Verify that the topic can be deleted, despite a partition being fenced
        self.kafka.delete_topic(self.topic)
        wait_until(lambda: self.check_topic_deleted(),
                   timeout_sec=180, backoff_sec=2, err_msg="topic %s was not fully deleted" % self.topic)

        # 7. Verify that the objects in the object store have been deleted.
        self.object_deletions_completed(backend)

