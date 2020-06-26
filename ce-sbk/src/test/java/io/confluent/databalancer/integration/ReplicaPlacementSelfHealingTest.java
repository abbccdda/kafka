package io.confluent.databalancer.integration;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Category(IntegrationTest.class)
public class ReplicaPlacementSelfHealingTest extends DataBalancerClusterTestHarness {
    protected static final Logger log = LoggerFactory.getLogger(RemoveBrokerTest.class);
    private static final String TOPIC = "topic";

    @Rule
    public final Timeout globalTimeout = Timeout.millis(Duration.ofMinutes(5).toMillis());

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
        DataBalancerIntegrationTestUtils.alterTopicPlacementConfig(adminClient, TOPIC, topicPlacementString);

        // expect replica distribution to self-heal to match topic placement above
        DataBalancerIntegrationTestUtils.verifyTopicPlacement(adminClient, TOPIC, topicPlacementString);

        // shift observer from rack 1 to rack 2
        topicPlacementString = "{\"version\":1,\"replicas\":[{\"count\": 2, \"constraints\":{\"rack\":\"0\"}}], " +
                "\"observers\": [{\"count\": 1, \"constraints\":{\"rack\":\"2\"}}]}";
        DataBalancerIntegrationTestUtils.alterTopicPlacementConfig(adminClient, TOPIC, topicPlacementString);

        // expect replica distribution to self-heal to match topic placement above
        DataBalancerIntegrationTestUtils.verifyTopicPlacement(adminClient, TOPIC, topicPlacementString);

        // set topic's topic placement constraint to two sync-replicas on rack 0, one observer on rack 1, one observer
        // on rack 2
        // this requires SBK to increase the replication factor of the topic to satisfy the placement constraint
        String increaseRfPlacement = "{\"version\":1,\"replicas\":[{\"count\": 2, \"constraints\":{\"rack\":\"0\"}}], " +
                "\"observers\": [{\"count\": 1, \"constraints\":{\"rack\":\"1\"}}, " +
                "{\"count\": 1, \"constraints\":{\"rack\":\"2\"}}]}";
        DataBalancerIntegrationTestUtils.alterTopicPlacementConfig(adminClient, TOPIC, increaseRfPlacement);

        // expect cluster to self-heal to have two observers, and increase in RF from 3 to 4
        DataBalancerIntegrationTestUtils.verifyTopicPlacement(adminClient, TOPIC, increaseRfPlacement);

        // change topic placement to one sync-replica on rack 1, one observer on rack 0
        // this requires SBK to decrease the replication factor of the topic
        String decreaseRfPlacement = "{\"version\":1,\"replicas\":[{\"count\": 1, \"constraints\":{\"rack\":\"1\"}}], " +
                "\"observers\": [{\"count\": 1, \"constraints\":{\"rack\":\"0\"}}]}";
        DataBalancerIntegrationTestUtils.alterTopicPlacementConfig(adminClient, TOPIC, decreaseRfPlacement);

        // expect cluster to self-heal and remove a sync-replica and an observer
        DataBalancerIntegrationTestUtils.verifyTopicPlacement(adminClient, TOPIC, decreaseRfPlacement);
    }

}
