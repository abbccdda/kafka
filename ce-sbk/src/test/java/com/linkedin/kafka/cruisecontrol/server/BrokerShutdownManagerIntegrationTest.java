/**
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.server;

import com.linkedin.kafka.cruisecontrol.BrokerShutdownIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.client.BlockingSendClient;
import com.linkedin.kafka.cruisecontrol.common.AdminClientResult;
import com.linkedin.kafka.cruisecontrol.common.KafkaCluster;
import com.linkedin.kafka.cruisecontrol.common.SbkAdminUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import io.confluent.kafka.test.cluster.EmbeddedKafka;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class BrokerShutdownManagerIntegrationTest extends BrokerShutdownIntegrationTestHarness {
  private BrokerShutdownManager shutdownManager;
  private static String apiTimeoutMs = "30000";
  private static String removalShutdownMs = "60000";
  private SbkAdminUtils adminUtils;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    KafkaCruiseControlConfig cruiseControlConfig = config();
    KafkaConfig kafkaConfig = brokerToShutdown().kafkaServer().config();
    BlockingSendClient.Builder builder = new BlockingSendClient.Builder(kafkaConfig,
        new SystemTime(), "client-id", new LogContext());
    adminUtils = new SbkAdminUtils(KafkaCruiseControlUtils.createAdmin(cruiseControlConfig.originals()), cruiseControlConfig);
    shutdownManager = new BrokerShutdownManager(adminUtils, cruiseControlConfig, builder, new SystemTime());
  }

  @Override
  protected void exitProcedure() {
    brokerToShutdown().shutdown();
  }

  @Test
  public void testMaybeShutdownShutsDownBroker() throws Exception {
    boolean didShutdown = shutdownManager.maybeShutdownBroker(brokerToShutdownId(), Optional.of(brokerToShutdownCurrentEpoch()));
    assertTrue("Expected broker to have been proactively shutdown", didShutdown);
    assertBrokerShutdown();
  }

  @Test
  public void testMaybeShutdownShutsDownBrokerAfterRestart() throws Exception {

    int brokerIdToShutdown = brokerToShutdownId();
    // Shutdown the broker
    boolean didShutdown = shutdownManager.maybeShutdownBroker(brokerIdToShutdown,
        Optional.of(brokerToShutdownCurrentEpoch()));
    assertTrue("Expected broker to have been proactively shutdown", didShutdown);

    TestUtils.waitForCondition(() -> getClusterSize() == numBrokers() - 1,
        "Failed to wait for broker to leave cluster");

    // Even though it's been shutdown we need to call shutdown() on the EmbeddedKafka instance
    // or we won't be able to restart it
    EmbeddedKafka kafka = kafkaCluster.kafkas().get(brokerIdToShutdown);
    kafka.shutdown();

    // Restart it
    kafka.startBroker(clusterTime);

    TestUtils.waitForCondition(() -> getClusterSize() == numBrokers(),
        "Failed to wait for broker to rejoin cluster");

    // The epoch will have changed
    Long epoch = kafka.kafkaServer().kafkaController().brokerEpoch();

    // Shut it down again
    didShutdown = shutdownManager.maybeShutdownBroker(brokerIdToShutdown, Optional.of(epoch));
    assertTrue("Expected broker to have been proactively shutdown", didShutdown);
    assertBrokerShutdown();
  }

  private int getClusterSize() throws Exception {
    AdminClientResult<KafkaCluster> clusterResult = adminUtils.describeCluster(10000);
    if (clusterResult.hasException()) {
      throw new ExecutionException("Failed to describe the cluster", clusterResult.exception());
    }
    KafkaCluster cluster = clusterResult.result();
    return cluster.nodes().size();
  }

  private KafkaCruiseControlConfig config() {
    Properties props = new Properties();
    props.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.bootstrapServers());
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, kafkaCluster.zkConnect());
    props.setProperty(KafkaCruiseControlConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, apiTimeoutMs);
    props.setProperty(KafkaCruiseControlConfig.BROKER_REMOVAL_SHUTDOWN_MS_CONFIG, removalShutdownMs);
    return new KafkaCruiseControlConfig(props);
  }
}
