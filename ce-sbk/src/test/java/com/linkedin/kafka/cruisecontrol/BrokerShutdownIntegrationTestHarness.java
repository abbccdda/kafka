/**
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.client.BlockingSendClient;
import io.confluent.common.EndPoint;
import io.confluent.kafka.test.cluster.EmbeddedKafka;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import kafka.cluster.BrokerEndPoint;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;

/**
 * A test harness for integration tests that involve shutting down a broker via
 * #{@link com.linkedin.kafka.cruisecontrol.server.BrokerShutdownManager}
 */
public abstract class BrokerShutdownIntegrationTestHarness {
  @Rule
  final public Timeout globalTimeout = Timeout.millis(120_000);

  private int brokerToShutdownId = 0;

  protected EmbeddedKafkaCluster kafkaCluster;
  private EmbeddedKafka brokerToShutdown;
  private BlockingSendClient blockingSendClient;
  private AtomicBoolean exited = new AtomicBoolean(false);

  @Before
  public void setUp() throws Exception {
    Exit.setExitProcedure((statusCode, message) -> {
      exited.set(true);
      exitProcedure();
    });
    kafkaCluster = new EmbeddedKafkaCluster();
    kafkaCluster.startZooKeeper();
    kafkaCluster.startBrokers(numBrokers(), new Properties());
    brokerToShutdown = kafkaCluster.kafkas().get(brokerToShutdownId);
    EndPoint endPoint = brokerToShutdown.endPoint();
    BrokerEndPoint brokerToShutdownEndpoint = new BrokerEndPoint(brokerToShutdownId, endPoint.host(), endPoint.port());
    blockingSendClient = new BlockingSendClient.Builder(brokerToShutdown.kafkaServer().config(), new Metrics(),
        new SystemTime(), "client-id", new LogContext())
        .build(brokerToShutdownEndpoint);
  }

  protected int numBrokers() {
    return 3;
  }

  protected void exitProcedure() {
    // noop
  }

  @After
  public void tearDown() {
    if (kafkaCluster != null) {
      kafkaCluster.shutdown();
    }
    Exit.resetExitProcedure();
    exited.set(false);
  }

  /**
   * Asserts that the broker to be shutdown was shut down.
   * Due to the asynchronous nature of shutdown, this method is blocking.
   */
  public void assertBrokerShutdown() throws InterruptedException {
    TestUtils.waitForCondition(this::brokerExitCalled, "Expected Kafka server to exit");
  }

  public boolean brokerExitCalled() {
    return exited.get();
  }

  public EmbeddedKafka brokerToShutdown() {
    return brokerToShutdown;
  }

  public Long brokerToShutdownCurrentEpoch() {
    return brokerToShutdown().kafkaServer().kafkaController().brokerEpoch();
  }

  public int brokerToShutdownId() {
    return brokerToShutdownId;
  }

  public BlockingSendClient blockingSendClient() {
    return blockingSendClient;
  }
}
