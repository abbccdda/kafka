/**
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.client;

import kafka.cluster.BrokerEndPoint;
import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.InitiateShutdownRequest;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Optional;
import java.util.Properties;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BlockingSendClientTest {
  private int brokerId = 0;
  private BrokerEndPoint brokerEndpoint = new BrokerEndPoint(brokerId, "localhost", 1000);
  private Metrics metrics = new Metrics();
  private long timeTickMs = 10000L;
  private long socketTimeoutMs = 9000L;
  private Time time = new MockTime(timeTickMs, 0L, 0L);
  private String clientId = "test-blocking-send-client";
  private LogContext logContext = new LogContext();

  private KafkaConfig config;

  @Mock
  private NetworkClient mockNetworkClient;
  @Mock
  private Node mockTargetBroker;

  @Before
  public void setUp() {
    Properties brokerProps = TestUtils.createBrokerConfig(
        brokerId, "localhost:1234", true, true,
        TestUtils.RandomPort(), Option.<SecurityProtocol>empty(), Option.<File>empty(), Option.<Properties>empty(), true,
        false, TestUtils.RandomPort(), false, TestUtils.RandomPort(), false, TestUtils.RandomPort(),
        Option.<String>empty(), 1, false, 1, (short) 1);
    brokerProps.setProperty(KafkaConfig.ControllerSocketTimeoutMsProp(), Long.toString(socketTimeoutMs));
    config = KafkaConfig.fromProps(brokerProps);

    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testBuild() throws IOException {
    BlockingSendClient sendClient = new BlockingSendClient.Builder(config, metrics, time, clientId, logContext).build(brokerEndpoint);
    sendClient.initiateClose();
    sendClient.close();
  }

  @Test
  public void testNetworkClientNotReadyThrowsConnectionExceptionCausedBySocketTimeoutException() throws Exception {
    when(mockNetworkClient.isReady(mockTargetBroker, timeTickMs)).thenReturn(false);
    when(mockNetworkClient.isReady(mockTargetBroker, timeTickMs * 2)).thenReturn(false);

    BlockingSendClient sendClient = new BlockingSendClient(mockTargetBroker, config,
        (int) socketTimeoutMs, time, mockNetworkClient, Optional.empty());
    try {
      sendClient.sendShutdownRequest(new InitiateShutdownRequest.Builder(1));
      fail("Expected sendShutdownRequest() to throw a ConnectionException");
    } catch (ConnectionException e) {
      assertTrue("Expected the ConnectionException's cause to be SocketTimeoutException",
          e.getCause() instanceof SocketTimeoutException);
      verify(mockNetworkClient, times(1)).close(mockTargetBroker.idString());
      verify(mockNetworkClient, times(1)).isReady(mockTargetBroker, timeTickMs);
      verify(mockNetworkClient, times(2)).isReady(mockTargetBroker, timeTickMs * 2);
    }
  }

  /**
   * Internally, the #{@link NetworkClient} can throw exceptions itself while connecting.
   * We should catch them and re-throw as #{@link ConnectionException}
   */
  @Test
  public void testNetworkClientFailedConnectionThrowsConnectionExceptionCausedByIOException() throws Exception {
    when(mockNetworkClient.isReady(mockTargetBroker, timeTickMs)).thenReturn(false);
    when(mockNetworkClient.isReady(mockTargetBroker, timeTickMs * 2)).thenReturn(false);
    when(mockNetworkClient.connectionFailed(mockTargetBroker)).thenReturn(true);

    BlockingSendClient sendClient = new BlockingSendClient(mockTargetBroker, config,
        (int) socketTimeoutMs, time, mockNetworkClient, Optional.empty());
    try {
      sendClient.sendShutdownRequest(new InitiateShutdownRequest.Builder(1));
      fail("Expected sendShutdownRequest() to throw a ConnectionException");
    } catch (ConnectionException e) {
      assertTrue("Expected the ConnectionException's cause to be IOException",
          e.getCause() instanceof IOException);
      verify(mockNetworkClient, times(1)).close(mockTargetBroker.idString());
      verify(mockNetworkClient, times(1)).isReady(mockTargetBroker, timeTickMs);
      verify(mockNetworkClient, times(2)).isReady(mockTargetBroker, timeTickMs * 2);
    }
  }

  @Test
  public void testNetworkClientSendExceptionClosesClientAndIsRethrown() {
    InitiateShutdownRequest.Builder reqBuilder = new InitiateShutdownRequest.Builder(1);
    when(mockNetworkClient.isReady(mockTargetBroker, timeTickMs)).thenReturn(true);
    // NetworkClientUtils#sendAndReceive() should raise an IOException if the client was shutdown before the response was read
    when(mockNetworkClient.active()).thenReturn(false);

    BlockingSendClient sendClient = new BlockingSendClient(mockTargetBroker, config,
        (int) socketTimeoutMs, time, mockNetworkClient, Optional.empty());
    try {
      sendClient.sendShutdownRequest(reqBuilder);
      fail("Expected sendShutdownRequest() to throw an IOException");
    } catch (IOException e) {
      verify(mockNetworkClient, times(1)).close(mockTargetBroker.idString());
      verify(mockNetworkClient, times(1)).isReady(mockTargetBroker, timeTickMs);
    }
  }
}
