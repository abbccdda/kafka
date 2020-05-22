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
from kafkatest.services.kafka import config_property
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.end_to_end import EndToEndTest
from kafkatest.utils import is_int
from kafkatest.version import LATEST_2_5, DEV_BRANCH, KafkaVersion
from kafkatest.utils.tiered_storage import tier_set_configs, TierSupport, TieredStorageMetricsRegistry, S3_BACKEND, GCS_BACKEND

class TestDowngrade(EndToEndTest, TierSupport):
    PARTITIONS = 3
    REPLICATION_FACTOR = 3

    TOPIC_CONFIG = {
        "partitions": PARTITIONS,
        "replication-factor": REPLICATION_FACTOR,
        "configs": {"min.insync.replicas": 2}
    }

    def __init__(self, test_context):
        super(TestDowngrade, self).__init__(test_context=test_context, topic_config=self.TOPIC_CONFIG)

    def add_tiered_storage_metrics(self):
        self.add_log_metrics(self.topic, partitions=range(0, self.PARTITIONS))
        self.kafka.jmx_object_names += [TieredStorageMetricsRegistry.ARCHIVER_LAG.mbean]
        self.restart_jmx_tool()

    def upgrade_from(self, kafka_version):
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            node.version = DEV_BRANCH
            node.config[config_property.INTER_BROKER_PROTOCOL_VERSION] = str(kafka_version)
            node.config[config_property.MESSAGE_FORMAT_VERSION] = str(kafka_version)
            self.kafka.start_node(node)
            self.wait_until_rejoin()

    def downgrade_to(self, kafka_version):
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            node.version = kafka_version
            del node.config[config_property.INTER_BROKER_PROTOCOL_VERSION]
            del node.config[config_property.MESSAGE_FORMAT_VERSION]
            self.kafka.start_node(node)
            self.wait_until_rejoin()

    def setup_services(self, from_kafka_project, dist_version, kafka_version, compression_types, security_protocol,
                       static_membership, backend):
        self.create_zookeeper()
        self.zk.start()

        self.create_kafka(num_nodes=3,
                          security_protocol=security_protocol,
                          interbroker_security_protocol=security_protocol,
                          project=from_kafka_project,
                          dist_version=dist_version,
                          version=kafka_version,
                          jmx_attributes=["Value"],
                          jmx_object_names=["kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions"])

        tier_set_configs(self.kafka, backend, feature=True, enable=True, hotset_bytes=1, hotset_ms=1)
        self.kafka.start()
        self.add_tiered_storage_metrics()

        self.create_producer(log_level="DEBUG",
                             compression_types=compression_types,
                             version=kafka_version)
        self.producer.start()

    def wait_until_rejoin(self):
        for partition in range(0, self.PARTITIONS):
            wait_until(lambda: len(self.kafka.isr_idx_list(self.topic, partition)) == self.REPLICATION_FACTOR,
                    timeout_sec=60, backoff_sec=1, err_msg="Replicas did not rejoin the ISR in a reasonable amount of time")

    @cluster(num_nodes=7)
    @matrix(from_kafka_project=["confluentplatform"], dist_version=["5.5.0"], version=[str(LATEST_2_5)], compression_types=[["none"]], tier=[True], backend=[S3_BACKEND, GCS_BACKEND])
    def test_upgrade_and_downgrade(self, version, compression_types, security_protocol="PLAINTEXT", from_kafka_project="kafka", dist_version=None,
            static_membership=False, tier=False, backend=None):
        """Test upgrade and downgrade of Kafka cluster from old versions to the current version

        `version` is the Kafka version to upgrade from and downgrade back to

        Downgrades are supported to any version which is at or above the current 
        `inter.broker.protocol.version` (IBP). For example, if a user upgrades from 1.1 to 2.3, 
        but they leave the IBP set to 1.1, then downgrading to any version at 1.1 or higher is 
        supported.

        This test case verifies that producers and consumers continue working during
        the course of an upgrade and downgrade.

        - Start 3 node broker cluster on version 'kafka_version'
        - Start producer the background
        - Roll the cluster to upgrade to the current version with IBP set to 'kafka_version'
        - Roll the cluster to downgrade back to 'kafka_version'
        - Start consumer after the downgrade has completed
        - Finally, validate that every message acked by the producer was consumed by the consumer
        """
        kafka_version = KafkaVersion(version)

        self.setup_services(from_kafka_project, dist_version, kafka_version, compression_types,
                            security_protocol, static_membership, backend)

        self.logger.info("First pass bounce - rolling upgrade")
        self.upgrade_from(kafka_version)

        wait_until(lambda: self.tiering_started(self.topic, partitions=range(0, self.PARTITIONS)),
                   timeout_sec=120, backoff_sec=2, err_msg="archiving did not start within timeout")

        self.logger.info("Second pass bounce - rolling downgrade")
        self.downgrade_to(kafka_version)

        # we delay consumption and validation until after the downgrade
        # to ensure we can still read all of the data from a downgraded TierPartitionState
        self.create_consumer(log_level="DEBUG",
                version=kafka_version,
                static_membership=static_membership)
        self.consumer.start()

        self.run_validation()
        assert self.kafka.check_protocol_errors(self)

        self.logger.info("Third pass bounce - rolling re-upgrade to check new tiered storage error metrics")
        self.upgrade_from(kafka_version)

        self.add_error_metrics()
        self.restart_jmx_tool()
        assert self.kafka.check_protocol_errors(self)

        wait_until(lambda: self.check_cluster_state(),
                timeout_sec=4, backoff_sec=1, err_msg="issue detected with cluster state metrics")
