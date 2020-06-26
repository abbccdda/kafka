/*
 Copyright 2020 Confluent Inc.
 */
package io.confluent.databalancer.integration;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/*
 * This test validates that when a new broker arrives and the default configuration is set to
 * not self-heal (not set to ANY_UNEVEN_LOAD), the broker is still added.
 * The success criterion matches that of AddBrokerWithSelfHealing.
 */
@Category(IntegrationTest.class)
public class AddBrokerTest extends DataBalancerClusterTestHarness {
  private static final String TEST_TOPIC = "broker_addition_test_topic";

  @Rule
  final public Timeout globalTimeout = Timeout.millis(Duration.ofMinutes(3).toMillis());

  @Override
  protected int initialBrokerCount() {
    return 3;
  }

  @Override
  protected Properties injectTestSpecificProperties(Properties props) {
    return props;
  }

  @Override
  protected Map<Integer, Map<String, String>> brokerOverrideProps() {
    Map<Integer, Map<String, String>> propsByBroker = new HashMap<>();
    Map<String, String> rack0Props = Collections.singletonMap(KafkaConfig.RackProp(), "0");
    Map<String, String> rack1Props = Collections.singletonMap(KafkaConfig.RackProp(), "1");

    propsByBroker.put(0, rack0Props);
    propsByBroker.put(1, rack0Props);
    propsByBroker.put(2, rack0Props);
    propsByBroker.put(3, rack1Props);
    return propsByBroker;
  }

  @Test
  public void testBrokerAddition() throws InterruptedException {
    KafkaTestUtils.createTopic(adminClient, TEST_TOPIC, 20, 2);

    int newBrokerId = initialBrokerCount();
    info("Adding new broker");
    addBroker(newBrokerId);
    DataBalancerIntegrationTestUtils.verifyReplicasMovedToBroker(adminClient, TEST_TOPIC, newBrokerId);
  }

  @Test
  public void testRemovedBrokerCanBeAdded() throws InterruptedException, ExecutionException {
    KafkaTestUtils.createTopic(adminClient, TEST_TOPIC, 20, 2);
    KafkaServer brokerToRemove = notControllerKafkaServer();

    AtomicBoolean exited = new AtomicBoolean(false);
    removeBroker(brokerToRemove, exited);
    int brokerIdToAdd = brokerToRemove.config().brokerId();
    addBroker(brokerIdToAdd);
    DataBalancerIntegrationTestUtils.verifyReplicasMovedToBroker(adminClient, TEST_TOPIC, brokerIdToAdd);
  }

  @Test
  public void testBrokerAddCreateObservers() throws InterruptedException, ExecutionException {
    KafkaTestUtils.createTopic(adminClient, TEST_TOPIC, 20, 3);
    String topicPlacementString = "{\"version\":1,\"replicas\":[{\"count\": 2, \"constraints\":{\"rack\":\"0\"}}], " +
            "\"observers\": [{\"count\": 1, \"constraints\":{\"rack\":\"1\"}}]}";
    DataBalancerIntegrationTestUtils.alterTopicPlacementConfig(adminClient, TEST_TOPIC, topicPlacementString);
    int newBrokerId = initialBrokerCount();
    addBroker(newBrokerId);
    DataBalancerIntegrationTestUtils.verifyReplicasMovedToBroker(adminClient, TEST_TOPIC, newBrokerId);
    DataBalancerIntegrationTestUtils.verifyTopicPlacement(adminClient, TEST_TOPIC, topicPlacementString);
  }
}
