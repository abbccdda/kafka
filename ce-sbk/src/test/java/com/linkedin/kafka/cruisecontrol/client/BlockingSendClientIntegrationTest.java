/**
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.client;

import io.confluent.common.EndPoint;
import io.confluent.kafka.test.cluster.EmbeddedKafka;
import kafka.cluster.BrokerEndPoint;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.InitiateShutdownRequest;
import org.apache.kafka.common.requests.InitiateShutdownResponse;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BlockingSendClientIntegrationTest {

  @Rule
  final public Timeout globalTimeout = Timeout.millis(120_000);

  private int numBrokers = 1;
  private int brokerId = 0;

  private EmbeddedKafkaCluster kafkaCluster;
  private EmbeddedKafka broker;
  private BrokerEndPoint brokerEndPoint;
  private BlockingSendClient blockingSendClient;
  private AtomicBoolean exited = new AtomicBoolean(false);

  @Before
  public void setUp() throws Exception {
    Exit.setExitProcedure((statusCode, message) -> exited.set(true));
    kafkaCluster = new EmbeddedKafkaCluster();
    kafkaCluster.startZooKeeper();
    kafkaCluster.startBrokers(numBrokers, new Properties());
    broker = kafkaCluster.kafkas().get(brokerId);
    EndPoint endPoint = broker.endPoint();
    brokerEndPoint = new BrokerEndPoint(brokerId, endPoint.host(), endPoint.port());
    blockingSendClient = new BlockingSendClient.Builder(broker.kafkaServer().config(), new Metrics(),
        new SystemTime(), "client-id", new LogContext())
        .build(brokerEndPoint);
  }

  @After
  public void tearDown() {
    if (kafkaCluster != null) {
      kafkaCluster.shutdown();
    }
    Exit.resetExitProcedure();
    exited.set(false);
  }

  @Test
  public void testSendShutdownRequest() throws Exception {
    long epoch = broker.kafkaServer().kafkaController().brokerEpoch();
    InitiateShutdownResponse response = blockingSendClient.sendShutdownRequest(new InitiateShutdownRequest.Builder(epoch));

    assertEquals(Errors.NONE.code(), response.data().errorCode());
    TestUtils.waitForCondition(() -> exited.get(), "Expected Kafka server to exit");
  }

  @Test
  public void testSendShutdownRequestHitsError() throws Exception {
    long epoch = -111L; // errorful epoch
    InitiateShutdownResponse response = blockingSendClient.sendShutdownRequest(new InitiateShutdownRequest.Builder(epoch));

    assertEquals(Errors.STALE_BROKER_EPOCH.code(), response.data().errorCode());
    assertFalse(exited.get());
  }

  @Test(expected = IOException.class)
  public void testSendRequestOnOfflineBrokerShouldRaiseIOException() throws Exception {
    long epoch = broker.kafkaServer().kafkaController().brokerEpoch();
    broker.shutdown();
    blockingSendClient.sendShutdownRequest(new InitiateShutdownRequest.Builder(epoch));
  }
}
