package io.confluent.databalancer.integration;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import kafka.common.TopicPlacement;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfluentTopicConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class ReplicaPlacementSelfHealingTest extends DataBalancerClusterTestHarness {
    protected static final Logger log = LoggerFactory.getLogger(RemoveBrokerTest.class);
    private static final String TOPIC = "topic";

    @Rule
    public final Timeout globalTimeout = Timeout.millis(Duration.ofMinutes(5).toMillis());

    private static Duration rebalanceFinishTimeout = Duration.ofMinutes(2);
    private static Duration rebalancePollInterval = Duration.ofSeconds(2);

    @Override
    protected Map<Integer, Map<String, String>> brokerOverrideProps() {
        Map<Integer, Map<String, String>> propsByBroker = new HashMap<>();
        Map<String, String> rack0Props = Collections.singletonMap(KafkaConfig.RackProp(), "0");
        Map<String, String> rack1Props = Collections.singletonMap(KafkaConfig.RackProp(), "1");
        Map<String, String> rack2Props = Collections.singletonMap(KafkaConfig.RackProp(), "2");

        propsByBroker.put(0, rack0Props);
        propsByBroker.put(1, rack0Props);
        propsByBroker.put(2, rack1Props);
        propsByBroker.put(3, rack2Props);
        return propsByBroker;
    }

    @Override
    protected int initialBrokerCount() {
        return 4;
    }

    @Override
    protected Properties injectTestSpecificProperties(Properties props) {
        props.put(ConfluentConfigs.BALANCER_AUTO_HEAL_MODE_CONFIG, ConfluentConfigs.BalancerSelfHealMode.ANY_UNEVEN_LOAD.toString());
        return props;
    }

    @Test
    public void testTopicPlacementChangeTriggersSelfHealing() throws InterruptedException, ExecutionException {
        // create an initial topic with replication factor 3 and no observers
        KafkaTestUtils.createTopic(adminClient, TOPIC, 10, 3);

        // topic placement with two sync-replicas on rack 0, one observer on rack 1
        String topicPlacementString = "{\"version\":1,\"replicas\":[{\"count\": 2, \"constraints\":{\"rack\":\"0\"}}], " +
                "\"observers\": [{\"count\": 1, \"constraints\":{\"rack\":\"1\"}}]}";
        alterTopicPlacementConfig(TOPIC, topicPlacementString);

        // expect replica distribution to self-heal to match topic placement above
        verifyTopicPlacement(TOPIC, topicPlacementString);

        // shift observer from rack 1 to rack 2
        topicPlacementString = "{\"version\":1,\"replicas\":[{\"count\": 2, \"constraints\":{\"rack\":\"0\"}}], " +
                "\"observers\": [{\"count\": 1, \"constraints\":{\"rack\":\"2\"}}]}";
        alterTopicPlacementConfig(TOPIC, topicPlacementString);

        // expect replica distribution to self-heal to match topic placement above
        verifyTopicPlacement(TOPIC, topicPlacementString);

        // set topic's topic placement constraint to two sync-replicas on rack 0, one observer on rack 1, one observer
        // on rack 2
        // this requires SBK to increase the replication factor of the topic to satisfy the placement constraint
        String increaseRfPlacement = "{\"version\":1,\"replicas\":[{\"count\": 2, \"constraints\":{\"rack\":\"0\"}}], " +
                "\"observers\": [{\"count\": 1, \"constraints\":{\"rack\":\"1\"}}, " +
                "{\"count\": 1, \"constraints\":{\"rack\":\"2\"}}]}";
        alterTopicPlacementConfig(TOPIC, increaseRfPlacement);

        // expect cluster to self-heal to have two observers, and increase in RF from 3 to 4
        verifyTopicPlacement(TOPIC, increaseRfPlacement);

        // change topic placement to one sync-replica on rack 1, one observer on rack 0
        // this requires SBK to decrease the replication factor of the topic
        String decreaseRfPlacement = "{\"version\":1,\"replicas\":[{\"count\": 1, \"constraints\":{\"rack\":\"1\"}}], " +
                "\"observers\": [{\"count\": 1, \"constraints\":{\"rack\":\"0\"}}]}";
        alterTopicPlacementConfig(TOPIC, decreaseRfPlacement);

        // expect cluster to self-heal and remove a sync-replica and an observer
        verifyTopicPlacement(TOPIC, decreaseRfPlacement);
    }

    private void verifyTopicPlacement(String topic, String topicPlacementString) throws InterruptedException {
        TopicPlacement expectedTopicPlacement = TopicPlacement.parse(topicPlacementString).get();

        info("Verifying that replica distribution for {} matches {}", topic, expectedTopicPlacement);
        TestUtils.waitForCondition(() -> {
            TopicDescription topicDescription = adminClient.describeTopics(Collections.singleton(topic)).all().get().get(topic);
            assertTrue("Expected partitions to be described for topic", !topicDescription.partitions().isEmpty());
            for (TopicPartitionInfo topicPartitionInfo : topicDescription.partitions()) {
                List<Node> observers = topicPartitionInfo.observers();
                List<Node> syncReplicas = new ArrayList<>(topicPartitionInfo.replicas());
                syncReplicas.removeAll(observers);

                // verify that all sync-replica constraints are satisfied
                for (TopicPlacement.ConstraintCount syncReplicaConstraint : expectedTopicPlacement.replicas()) {
                    info("Verifying sync replicas {} match constraints {}", syncReplicas, syncReplicaConstraint);
                    List<Node> matchingSyncReplicas = new ArrayList<>(syncReplicas);
                    matchingSyncReplicas.removeIf(node -> !syncReplicaConstraint.matches(attributes(node)));
                    assertEquals("Expected number of sync replicas to match topic placement",
                            syncReplicaConstraint.count(), matchingSyncReplicas.size());
                }

                // verify that all observer constraints are satisfied
                for (TopicPlacement.ConstraintCount observerConstraint : expectedTopicPlacement.observers()) {
                    info("Verifying observers {} match constraints {}", observers, observerConstraint);
                    List<Node> matchingObservers = new ArrayList<>(observers);
                    matchingObservers.removeIf(node -> !observerConstraint.matches(attributes(node)));
                    assertEquals("Expected number of observers to match topic placement",
                            observerConstraint.count(), matchingObservers.size());
                }
            }
            return true;
        },
        rebalanceFinishTimeout.toMillis(),
        rebalancePollInterval.toMillis(),
        () -> "Change in TopicPlacement did not trigger and complete cluster self-healing in time.");
    }

    private void alterTopicPlacementConfig(String topic, String topicPlacement) throws ExecutionException, InterruptedException {
        ConfigResource topicConfigResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        ConfigEntry configEntry = new ConfigEntry(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, topicPlacement);
        AlterConfigOp addTopicPlacement = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
        info("Changing topic placement for topic {} to {}", topic, topicPlacement);
        adminClient.incrementalAlterConfigs(Collections.singletonMap(topicConfigResource,
                Collections.singleton(addTopicPlacement))).all().get();
        // wait for TopicPlacement update to be propagated
        TestUtils.waitForCondition(() -> {
            Map<ConfigResource, Config> topicConfig = adminClient.describeConfigs(Collections.singleton(
                    topicConfigResource)).all().get();
            Collection<ConfigEntry> configEntries = topicConfig.get(topicConfigResource).entries();
            Optional<ConfigEntry> topicPlacementEntry = configEntries.stream().filter(entry ->
                    entry.name().equals(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG)).findFirst();
            return topicPlacementEntry.isPresent() && topicPlacementEntry.get().value().equals(topicPlacement);
        }, "TopicPlacement change did not complete in time");
        info("Topic placement change complete");
    }

    private Map<String, String> attributes(Node node) {
        return Collections.singletonMap("rack", node.rack());
    }
}
