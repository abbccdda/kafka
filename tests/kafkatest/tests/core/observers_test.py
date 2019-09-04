# Copyright 2019 Confluent Inc.

import time

from collections import defaultdict

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.monitor.jmx import JmxMixin
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int

from uuid import uuid4

class JmxTool(JmxMixin, KafkaPathResolverMixin):
    """
    Simple helper class for using the JmxTool directly instead of as a mix-in
    """
    def __init__(self, text_context, *args, **kwargs):
        JmxMixin.__init__(self, num_nodes=1, *args, **kwargs)
        self.context = text_context

    @property
    def logger(self):
        return self.context.logger

class ObserversTest(ProduceConsumeValidateTest):
    """
    These tests validate observers feature.

    Test if observer/replica placement works correctly. Create a topic with replica and observers.
    Produce some data, stop all replica, make observer leader, produce more data, bring replica back
    up and then let leadership transfer back to replica. Check if producing and consuming works throughout
    the process.
    """

    RACK_AWARE_REPLICA_SELECTOR = "org.apache.kafka.common.replica.RackAwareReplicaSelector"
    # Reduce the metadata refresh interval to 3 seconds to make changes visible faster, default is 5 mins
    METADATA_MAX_AGE_MS = 3000

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ObserversTest, self).__init__(test_context=test_context)

        self.topic = "ObserverTest-{}-{}".format(self.test_context.session_context.session_id, uuid4())
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.num_producers = 1
        self.producer_throughput = 1000
        self.num_consumers = 1
        self.num_partitions = 3
        self.rack_a = "rack-a"
        self.rack_b = "rack-b"
        self.rack_c = "rack-c"
        self.kafka = KafkaService(test_context,
                                  num_nodes=8,
                                  zk=self.zk,
                                  topics={
                                      self.topic: {
                                          "partitions": self.num_partitions,
                                          "replica-placement":
                                          """<(echo '{
                                              "version": 1,
                                              "replicas": [
                                                  {
                                                      "count": 3,
                                                      "constraints": {"rack": "rack-a"}
                                                  },
                                                  {
                                                      "count": 2,
                                                      "constraints": {"rack": "rack-b"}
                                                  }
                                              ],
                                              "observers": [
                                                  {
                                                      "count": 3,
                                                      "constraints": {"rack": "rack-c"}
                                                  }
                                              ]
                                          }')""",
                                          # keep min insync replica to 1 as we will start/stop brokers
                                          "configs": {"min.insync.replicas": 1}
                                      }
                                  },
                                  server_prop_overides=[
                                      ["confluent.observer.feature", "true"],
                                      ["replica.selector.class", self.RACK_AWARE_REPLICA_SELECTOR]
                                  ],
                                  # distribute nodes on rack so that we can place observer and replica
                                  # on separate racks
                                  per_node_server_prop_overrides={
                                      1: [("broker.rack", self.rack_a)],
                                      2: [("broker.rack", self.rack_a)],
                                      3: [("broker.rack", self.rack_a)],
                                      4: [("broker.rack", self.rack_b)],
                                      5: [("broker.rack", self.rack_b)],
                                      6: [("broker.rack", self.rack_c)],
                                      7: [("broker.rack", self.rack_c)],
                                      8: [("broker.rack", self.rack_c)]
                                  })

    def setUp(self):
        self.zk.start()
        self.kafka.start(use_zk_to_create_topic=False)
        # Get the nodes for each rack
        self.rack_a_nodes = set()
        self.rack_b_nodes = set()
        self.rack_c_nodes = set()

        for node in self.kafka.nodes:
            broker_info = self.kafka.get_broker_info(node)
            broker_rack = broker_info["rack"]
            if broker_rack == self.rack_a:
                self.rack_a_nodes.add(node)
            elif broker_rack == self.rack_b:
                self.rack_b_nodes.add(node)
            else:
                self.rack_c_nodes.add(node)

    def min_cluster_size(self):
        return super(ObserversTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    def validate_initial_broker_placement(self):
        """
        Test if the initial broker assignment is as per placement constraint that was provided in create-topic command.
        Also check the leader broker isn't among the observers.
        """
        partition_zero_replicas = self.kafka.replicas(self.topic, 0)
        partition_one_replicas = self.kafka.replicas(self.topic, 1)
        partition_two_replicas = self.kafka.replicas(self.topic, 2)
        rack_a_replicas = set(partition_zero_replicas[0:3] + partition_one_replicas[0:3] + partition_two_replicas[0:3])
        rack_b_replicas = set(partition_zero_replicas[3:5] + partition_one_replicas[3:5] + partition_two_replicas[3:5])
        rack_c_replicas = set(partition_zero_replicas[5:7] + partition_one_replicas[5:7] + partition_two_replicas[5:7])

        assert rack_a_replicas == self.rack_a_nodes, \
            "%s is not on rack-a nodes %s" % (rack_a_replicas, self.rack_a_nodes)
        assert rack_b_replicas == self.rack_b_nodes, \
            "%s is not on rack-b nodes %s" % (rack_b_replicas, self.rack_b_nodes)
        assert rack_c_replicas == self.rack_c_nodes, \
            "%s is not on rack-c nodes %s" % (rack_c_replicas, self.rack_c_nodes)

        isr_set = self.get_node_indexes(self.rack_a_nodes | self.rack_b_nodes)
        self.validate_observers(isr_set)

        # Leader should be among rack-a or rack-b
        self.check_leadership(self.rack_a_nodes | self.rack_b_nodes)

    def validate_observers(self, isr_set):
        """
        Test if brokers on rack-c are observers and observers are not part of isr
        """
        observers = self.get_node_indexes(self.rack_c_nodes)
        wait_until(lambda: self.get_field_from_describe_topic("live_observers") == observers, 30, 1)

        self.check_observers_not_in_isr(isr_set)

    def check_observers_not_in_isr(self, isr_set):
        """
        Make sure that:
        1. observers are not part of isr, and
        2. isr matches to replica set passed in as argument
        """
        wait_until(lambda: self.get_field_from_describe_topic("isr") == isr_set, 30, 1)

        isr = self.get_field_from_describe_topic("isr")
        assert len(self.rack_c_nodes.intersection(isr)) == 0, \
           "isr %s contains rack-c nodes %s" % (isr, self.rack_c_nodes)

    def get_field_from_describe_topic(self, field_name):
        described_topic = self.kafka.describe_topic(self.topic, use_zk_to_describe_topic=False)
        self.logger.debug(described_topic)
        topic_partitions = self.kafka.parse_describe_topic(described_topic)["partitions"]
        all_observers = [set(partition[field_name]) for partition in topic_partitions]
        return set.intersection(*all_observers)

    def get_node_indexes(self, nodes):
        """
        Fetch index of all nodes and return then as a set.
        """
        return set([self.kafka.idx(node) for node in nodes])

    def stop_nodes(self, nodes):
        for node in nodes:
            self.kafka.stop_node(node)

    def start_nodes(self, nodes):
        """
        Stop nodes passed in as argument.
        """
        for node in nodes:
            self.kafka.start_node(node)

    def check_leadership(self, nodes):
        """
        Check if leader (for all partitions) is among the nodes passed in as argument.
        """
        for partition in range(0, self.num_partitions):
            wait_until(lambda : self.kafka.leader(self.topic, partition) in nodes, 30, 1)

    def check_no_leader(self):
        """
        Validate that there is no leader for partition.
        """
        for partition in range(0, self.num_partitions):
            wait_until(lambda : self.kafka.leader(self.topic, partition) is None, 30, 1)

    # 8 nodes for kafka, one for ZK
    @cluster(num_nodes=9)
    def test_observers(self):
        """
        Observer tests.

        Setup:
        1. topic with 3 partitions with replication factor of 7
        2. replica placement with observers/replicas assigned to all 7 brokers created above. Brokers in
           rack-a and rack-b will be in-sync, whereas ones on rack-c will host observers.

        To test:
            - Check initial replica/observer placement is as per replica-placement-constraint
            - Stop brokers on rack-a, leadership should get transferred to brokers in rack-b.
            - Stop brokers in rack-b, the topic partitions should have no leader.
            - Transfer leadership to observers by performing unclean leader election.
            - Validate that one of observers become leader.
            - Restart all stopped brokers, do a preferred leader election, validate that leadership gets transferred
              back to "replica" brokers.
         """
        self.validate_initial_broker_placement()

        # Stop nodes in rack-a and confirm leadership gets transferred to a rack-b node
        self.stop_nodes(self.rack_a_nodes)
        self.logger.debug(self.kafka.describe_topic(self.topic, use_zk_to_describe_topic=False))
        self.check_leadership(self.rack_b_nodes)
        self.validate_observers(self.get_node_indexes(self.rack_b_nodes))

        # Stop rack-b node and confirm that there is no leader
        self.stop_nodes(self.rack_b_nodes)
        self.logger.debug(self.kafka.describe_topic(self.topic, use_zk_to_describe_topic=False))
        self.check_no_leader()

        # Now do unclean leader election to make observer leader
        for partition in range(0, self.num_partitions):
            self.kafka.perform_leader_election(self.topic, partition, "unclean")
        self.logger.debug(self.kafka.describe_topic(self.topic, use_zk_to_describe_topic=False))
        self.check_leadership(self.rack_c_nodes)

        # Bring all nodes back up again and check that leadership transfers back
        self.start_nodes(self.rack_a_nodes | self.rack_b_nodes)
        for partition in range(0, self.num_partitions):
            self.kafka.perform_leader_election(self.topic, partition, "preferred")
        self.logger.debug(self.kafka.describe_topic(self.topic, use_zk_to_describe_topic=False))
        self.check_leadership(self.rack_a_nodes | self.rack_b_nodes)
        self.validate_observers(self.get_node_indexes(self.rack_a_nodes | self.rack_b_nodes))

    # 8 nodes for kafka, one for ZK, one for producer and one for consumer
    @cluster(num_nodes=11)
    def test_produce_consume(self):
        """
        Test if producer and consumer work correctly when unclean leader election is used to
        make observer leader and then leader gets moved to preferred replica.
        """
        self.validate_initial_broker_placement()

        # Stop rack-a and rack-b nodes and perform unclean leader election to make
        # observer leader
        self.stop_nodes(self.rack_a_nodes | self.rack_b_nodes)
        for partition in range(0, self.num_partitions):
            self.kafka.perform_leader_election(self.topic, partition, "unclean")
        self.check_leadership(self.rack_c_nodes)
        self.logger.debug(self.kafka.describe_topic(self.topic, use_zk_to_describe_topic=False))

        self.producer = VerifiableProducer(self.test_context,
                                           self.num_producers,
                                           self.kafka,
                                           self.topic,
                                           throughput=self.producer_throughput,
                                           enable_idempotence=True)

        self.consumer = ConsoleConsumer(self.test_context,
                                        self.num_consumers,
                                        self.kafka,
                                        self.topic,
                                        consumer_timeout_ms=60000,
                                        message_validator=is_int)

        def move_leader_back_to_replica():
            # Bring all replica nodes back up again and perform preferred leader election
            self.start_nodes(self.rack_a_nodes | self.rack_b_nodes)
            self.logger.debug(self.kafka.describe_topic(self.topic, use_zk_to_describe_topic=False))

            for partition in range(0, self.num_partitions):
                self.kafka.perform_leader_election(self.topic, partition, "preferred")
            self.logger.debug(self.kafka.describe_topic(self.topic, use_zk_to_describe_topic=False))
            # Make sure leadership goes back to replica nodes
            self.check_leadership(self.rack_a_nodes | self.rack_b_nodes)

        self.run_produce_consume_validate(core_test_action=lambda: move_leader_back_to_replica())

    # 8 nodes for kafka, one for ZK, one for producer and one for consumer
    @cluster(num_nodes=11)
    def test_fetch_from_observer(self):
        """
        Test if consumers can fetch from observer if their racks match and fetch from follower is enabled.
        """
        self.validate_initial_broker_placement()

        self.producer = VerifiableProducer(self.test_context,
                                           self.num_producers,
                                           self.kafka,
                                           self.topic,
                                           throughput=self.producer_throughput,
                                           enable_idempotence=True)

        consumer_client_id = "console-consumer"
        self.consumer = ConsoleConsumer(self.test_context,
                                        self.num_consumers,
                                        self.kafka,
                                        self.topic,
                                        client_id=consumer_client_id,
                                        group_id="test-consumer-group-1",
                                        consumer_timeout_ms=60000,
                                        message_validator=is_int,
                                        consumer_properties={"client.rack": self.rack_c, "metadata.max.age.ms": self.METADATA_MAX_AGE_MS})

        # Start up and let some data get produced
        self.start_producer_and_consumer()
        time.sleep(self.METADATA_MAX_AGE_MS * 2. / 1000)

        consumer_node = self.consumer.nodes[0]
        consumer_idx = self.consumer.idx(consumer_node)
        read_replica_attribute = "preferred-read-replica"
        read_replica_mbean = ["kafka.consumer:type=consumer-fetch-manager-metrics,client-id=%s,topic=%s,partition=%d" % \
                  (consumer_client_id, self.topic, partition) for partition in range(0, self.num_partitions)]
        jmx_tool = JmxTool(self.test_context, jmx_poll_ms=100)
        jmx_tool.jmx_object_names = read_replica_mbean
        jmx_tool.jmx_attributes = [read_replica_attribute]
        jmx_tool.start_jmx_tool(consumer_idx, consumer_node)

        # Wait for at least one interval of "metadata.max.age.ms"
        time.sleep(self.METADATA_MAX_AGE_MS * 2. / 1000)

        # Read the JMX output
        jmx_tool.read_jmx_output(consumer_idx, consumer_node)

        all_captured_preferred_read_replicas = defaultdict(int)
        self.logger.debug(jmx_tool.jmx_stats)

        for ts, data in jmx_tool.jmx_stats[0].items():
            for k, v in data.items():
                if k.endswith(read_replica_attribute):
                    all_captured_preferred_read_replicas[int(v)] += 1

        self.logger.debug("Saw the following preferred read replicas %s",
                          dict(all_captured_preferred_read_replicas.items()))

        fetch_from_observer_count = 0
        for observer_idx in self.get_node_indexes(self.rack_c_nodes):
            fetch_from_observer_count += all_captured_preferred_read_replicas[observer_idx]

        assert fetch_from_observer_count > 0, \
            "Expected to see observers with rack %s as a preferred replica. Instead got: %s" % \
            (self.rack_c, dict(all_captured_preferred_read_replicas.items()))

        # Validate consumed messages
        self.stop_producer_and_consumer()
        self.validate()
