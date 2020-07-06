# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.mark import parametrize, matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.utils.tiered_storage import tier_set_configs, TierSupport, TieredStorageMetricsRegistry, S3_BACKEND, GCS_BACKEND, AZURE_BLOCK_BLOB_BACKEND
from kafkatest.services.kafka import config_property

import uuid
import time

class TestTierTopicDeletion(ProduceConsumeValidateTest, TierSupport):
    PARTITIONS = 10
    LOG_SEGMENT_BYTES = 1024 * 1024
    TIER_BUCKET_PREFIX = "system-test-run-" + str(int(round(time.time() * 1000))) + "-" + str(uuid.uuid4()) + "/"

    TOPIC_CONFIG = {
        "partitions": PARTITIONS,
        "replication-factor": 3,
        "configs": {
            "min.insync.replicas": 2,
            "confluent.tier.enable": True
        }
    }

    def __init__(self, test_context):
        super(TestTierTopicDeletion, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()

        # Producer and consumer
        self.producer_throughput = 5000
        self.num_producers = 1
        self.num_consumers = 1

    def add_tiered_storage_metrics(self):
        self.add_log_metrics(self.topic, partitions=range(0, self.PARTITIONS))
        self.kafka.jmx_object_names += [TieredStorageMetricsRegistry.ARCHIVER_LAG.mbean]
        self.restart_jmx_tool()

    def bounce_broker(self, node, clean_shutdown):
        if clean_shutdown:
            self.kafka.restart_node(node, clean_shutdown = True)
        else:
            self.kafka.stop_node(node, clean_shutdown = False)
            wait_until(lambda: len(self.kafka.pids(node)) == 0 and not self.kafka.is_registered(node),
                        timeout_sec=self.kafka.zk_session_timeout + 5,
                        err_msg="Failed to see timely deregistration of \
                                hard-killed broker %s" % str(node.account))
            self.kafka.start_node(node)

    @cluster(num_nodes=6)
    @matrix(hard_bounce_broker=[False, True], backend=[S3_BACKEND, GCS_BACKEND, AZURE_BLOCK_BLOB_BACKEND])
    def test_tier_topic_deletion(self, hard_bounce_broker, backend):
        """
        Test the tier topic deletion pathways by creating a topic with partitions to be archived while bouncing brokers
        and then deleting the topic and bouncing brokers again. Finally we check whether the GCS /S3 bucket or Azure block
        block container contains objects for the topic that we do not expect.
        """

        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk,
                                  jmx_attributes=["Value"],
                                  jmx_object_names=[TieredStorageMetricsRegistry.DELETED_PARTITIONS_COORDINATOR_INPROGRESS_DELETIONS.mbean,
                                      TieredStorageMetricsRegistry.DELETED_PARTITIONS_COORDINATOR_QUEUED_DELETIONS.mbean])
        tier_set_configs(self.kafka, backend, feature=True, enable=False,
                    hotset_bytes=0, hotset_ms=0, metadata_replication_factor=3,
                    log_retention_check_interval=500, log_roll_time=500,
                    hotset_roll_min_bytes=10240, log_segment_bytes=self.LOG_SEGMENT_BYTES,
                    tier_bucket_prefix=self.TIER_BUCKET_PREFIX)
        self.kafka.topics = {self.topic: self.TOPIC_CONFIG}
        self.kafka.start()

        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int)

        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=30000,
                                        message_validator=is_int)

        self.run_produce_consume_validate(core_test_action=lambda:self.producer.each_produced_at_least(2000000))

        # bounce brokers to make sure some segments are fenced
        for node in self.kafka.nodes:
            self.bounce_broker(node, hard_bounce_broker)

        self.add_tiered_storage_metrics()
        wait_until(lambda: self.tiering_completed(self.topic, partitions=range(0, self.PARTITIONS)),
                timeout_sec=240, backoff_sec=2, err_msg="archive has not completed yet")

        self.logger.info("deleting topic " + self.topic)
        self.kafka.delete_topic(self.topic)

        self.remove_log_metrics(self.topic, range(0, self.PARTITIONS))
        self.restart_jmx_tool()

        # bounce brokers after topic delete to trigger partial deletion
        for node in self.kafka.nodes:
            self.bounce_broker(node, hard_bounce_broker)

        self.object_deletions_completed(backend)
