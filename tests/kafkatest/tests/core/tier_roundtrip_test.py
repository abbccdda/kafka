from os import environ
from time import sleep
from uuid import uuid1

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from ducktape.mark.resource import cluster

from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka import config_property
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.utils.tiered_storage import tier_server_props

class TierRoundtripTest(ProduceConsumeValidateTest):
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

    TIER_S3_BUCKET = "confluent-tier-system-test-us-east-1"
    # The value of log.segment.bytes and number of records to produce should be set such that
    # multiple segments are rolled, tiered to S3 and deleted from the local log.
    LOG_SEGMENT_BYTES = 100 * 1024
    MIN_RECORDS_PRODUCED = 25000

    def __init__(self, test_context):
        super(TierRoundtripTest, self).__init__(test_context=test_context)

        self.topic = "t-{}-{}".format(self.test_context.session_context.session_id, uuid1())
        self.zk = ZookeeperService(test_context, num_nodes=1)

        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk,
                                    jmx_object_names=[
                                        "kafka.server:type=TierFetcher",
                                        "kafka.tier.archiver:type=TierArchiver,name=TotalLag",
                                    ],
                                    jmx_attributes=["BytesFetchedTotal", "Value"],
                                    server_prop_overides=tier_server_props(self.TIER_S3_BUCKET, metadata_replication_factor=1, region="us-east-1") + [
                                        [config_property.LOG_SEGMENT_BYTES, self.LOG_SEGMENT_BYTES],
                                        [config_property.LOG_RETENTION_CHECK_INTERVAL_MS, "5000"],
                                    ],
                                    topics={self.topic: {
                                        "partitions": 1,
                                        "replication-factor": 1,
                                        "configs": {
                                            "min.insync.replicas": 1,
                                            "confluent.tier.enable": True
                                        }}
                                    })

        self.num_producers = 1
        self.num_consumers = 1

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        return super(TierRoundtripTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    def archive_completed(self):
        self.kafka.read_jmx_output_all_nodes()

        max_lag = self.kafka.maximum_jmx_value.get("kafka.tier.archiver:type=TierArchiver,name=TotalLag:Value", 0)
        if (max_lag < 1):
            return False

        # this assumes one kafka node
        last_jmx_entry = sorted(self.kafka.jmx_stats[0].items(), key=lambda kv: kv[0])[-1][1]
        last_lag = last_jmx_entry.get("kafka.tier.archiver:type=TierArchiver,name=TotalLag:Value", -1)
        return last_lag == 0

    def restart_jmx_tool(self):
        for knode in self.kafka.nodes:
            knode.account.kill_java_processes(self.kafka.jmx_class_name(), clean_shutdown=False, allow_fail=True)
            idx = self.kafka.idx(knode)
            self.kafka.started[idx-1] = False
            self.kafka.start_jmx_tool(idx, knode)

    def test_tier_roundtrip(self):
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka, self.topic, throughput=1000, message_validator=is_int)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
             self.topic, consumer_timeout_ms=60000, message_validator=is_int)

        self.kafka.start()
        self.producer.start()

        wait_until(lambda: self.producer.each_produced_at_least(self.MIN_RECORDS_PRODUCED),
            timeout_sec=120, backoff_sec=1,
            err_msg="Producer did not produce all messages in reasonable amount of time")

        self.producer.stop()

        wait_until(lambda: self.archive_completed(),
                   timeout_sec=60, backoff_sec=2, err_msg="archive did not complete within timeout")

        # ensure hot set retention deletes segments
        sleep(7)

        # log-specific beans were not available at startup
        self.kafka.jmx_object_names += ["kafka.log:name=Size,partition=0,topic={},type=Log".format(self.topic),
                                        "kafka.log:name=NumLogSegments,partition=0,topic={},type=Log".format(self.topic)]
        self.restart_jmx_tool()

        self.consumer.start()
        self.consumer.wait()
        self.validate()

        self.kafka.read_jmx_output_all_nodes()
        tier_bytes_fetched = self.kafka.maximum_jmx_value["kafka.server:type=TierFetcher:BytesFetchedTotal"]
        # N.B. for some reason the bean tag order is different in the results. Compare to concatted jmx_object_names above.
        log_size_key = "kafka.log:type=Log,name=Size,topic={},partition=0:Value".format(self.topic)
        log_segs_key = "kafka.log:type=Log,name=NumLogSegments,topic={},partition=0:Value".format(self.topic)
        log_size = self.kafka.maximum_jmx_value[log_size_key]
        log_segments = self.kafka.maximum_jmx_value[log_segs_key]
        self.logger.info("{} bytes fetched from S3 of {} total bytes, in {} segments".format(tier_bytes_fetched, log_size, log_segments))
        bytes_fetched_from_local_log = log_size - tier_bytes_fetched
        assert bytes_fetched_from_local_log <= self.LOG_SEGMENT_BYTES
