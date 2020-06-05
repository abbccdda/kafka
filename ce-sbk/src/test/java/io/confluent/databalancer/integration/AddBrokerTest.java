/*
 Copyright 2020 Confluent Inc.
 */
package io.confluent.databalancer.integration;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;

import java.util.Map;
import java.util.Properties;


import static org.apache.kafka.test.TestUtils.waitForCondition;

/*
 * This test validates that when a new broker arrives and the default configuration is set to
 * not self-heal (not set to ANY_UNEVEN_LOAD), the broker is still added.
 * The success criterion matches that of AddBrokerWithSelfHealing.
 */
@Category(IntegrationTest.class)
public class AddBrokerTest extends DataBalancerClusterTestHarness {
  private static final String TEST_TOPIC = "broker_addition_test_topic";

  private static final Duration ADD_FINISH_TIMEOUT = Duration.ofMinutes(2);

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

  @Test
  public void testBrokerAddition() throws Exception {
    KafkaTestUtils.createTopic(adminClient, TEST_TOPIC, 20, 2);

    int newBrokerId = initialBrokerCount() + 1;
    info("Adding new broker");
    addBroker(newBrokerId);

    waitForCondition(() -> {
          // Don't check if we're still reassigning
          if (!adminClient.listPartitionReassignments().reassignments().get().isEmpty()) {
            return false;
          }

          // Look for partitions on the new broker
          Map<String, TopicDescription> topics = adminClient.describeTopics(Collections.singletonList(TEST_TOPIC)).all().get();
          boolean newBrokerHasReplicas = topics.values().stream().anyMatch(
              desc -> desc.partitions().stream().anyMatch(
                  tpInfo -> tpInfo.replicas().stream().anyMatch(node -> node.id() == newBrokerId)
              )
          );
          return newBrokerHasReplicas;
        },
        ADD_FINISH_TIMEOUT.toMillis(),
        "Replicas were not balanced onto the new broker");
  }
}
