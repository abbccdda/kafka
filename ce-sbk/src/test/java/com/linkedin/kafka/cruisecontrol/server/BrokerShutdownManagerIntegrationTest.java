/**
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.server;

import com.linkedin.kafka.cruisecontrol.BrokerShutdownIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.client.BlockingSendClient;
import com.linkedin.kafka.cruisecontrol.common.SbkAdminUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.util.Optional;
import java.util.Properties;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class BrokerShutdownManagerIntegrationTest extends BrokerShutdownIntegrationTestHarness {
  private BrokerShutdownManager shutdownManager;
  private static String apiTimeoutMs = "30000";
  private static String removalShutdownMs = "60000";

  @Before
  public void setUp() throws Exception {
    super.setUp();
    KafkaCruiseControlConfig cruiseControlConfig = config();
    KafkaConfig kafkaConfig = brokerToShutdown().kafkaServer().config();
    BlockingSendClient.Builder builder = new BlockingSendClient.Builder(kafkaConfig, new Metrics(),
        new SystemTime(), "client-id", new LogContext());
    SbkAdminUtils adminUtils = new SbkAdminUtils(KafkaCruiseControlUtils.createAdmin(cruiseControlConfig.originals()), cruiseControlConfig);
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

  private KafkaCruiseControlConfig config() {
    Properties props = new Properties();
    props.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.bootstrapServers());
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, kafkaCluster.zkConnect());
    props.setProperty(KafkaCruiseControlConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, apiTimeoutMs);
    props.setProperty(KafkaCruiseControlConfig.BROKER_REMOVAL_SHUTDOWN_MS_CONFIG, removalShutdownMs);
    return new KafkaCruiseControlConfig(props);
  }
}
