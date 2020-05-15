/**
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.server;

import com.linkedin.kafka.cruisecontrol.client.BlockingSendClient;
import com.linkedin.kafka.cruisecontrol.client.ConnectionException;
import com.linkedin.kafka.cruisecontrol.common.AdminClientResult;
import com.linkedin.kafka.cruisecontrol.common.KafkaCluster;
import com.linkedin.kafka.cruisecontrol.common.SbkAdminUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.InitiateShutdownResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.InitiateShutdownResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BrokerShutdownManagerTest {
  private static String apiTimeoutMs = "100";
  private static String removalShutdownMs = "1000";
  private static long mockTimeTickMs = 100;
  private static long brokerEpoch = 1;
  private static int brokerIdToRemove = 3;

  private List<Node> fullCluster = Arrays.asList(
      new Node(0, "", 9000),
      new Node(1, "", 9000),
      new Node(2, "", 9000),
      new Node(brokerIdToRemove, "", 9000)
  );
  private List<Node> clusterWithBrokerRemoved = Arrays.asList(
      new Node(0, "", 9000),
      new Node(1, "", 9000),
      new Node(2, "", 9000)
  );
  private AdminClientResult<KafkaCluster> fullClusterOpt = new AdminClientResult<>(
      new KafkaCluster(fullCluster, null, null, null));
  private AdminClientResult<KafkaCluster> noBrokerClusterOpt = new AdminClientResult<>(
      new KafkaCluster(clusterWithBrokerRemoved, null, null, null));

  private SbkAdminUtils mockAdminUtils;
  private Time mockTime;
  private BrokerShutdownManager removalManager;
  private BlockingSendClient mockShutdownClient;
  private BlockingSendClient.Builder mockShutdownClientBuilder;

  @Before
  public void setUp() {
    mockTime = new MockTime();
    mockAdminUtils = mock(SbkAdminUtils.class);
    mockShutdownClientBuilder = mock(BlockingSendClient.Builder.class);
    mockShutdownClient = mock(BlockingSendClient.class);
    when(mockShutdownClientBuilder.build(any())).thenReturn(mockShutdownClient);

    removalManager = new BrokerShutdownManager(mockAdminUtils, config(), mockShutdownClientBuilder, mockTime);
  }

  @Test
  public void testMaybeShutdownBrokerShutsDownBroker() throws Exception {
    AtomicInteger describeCalls = new AtomicInteger();
    when(mockAdminUtils.describeCluster(Long.parseLong(apiTimeoutMs))).thenAnswer(invocation -> {
      // return broker removed on the second call
      if (describeCalls.incrementAndGet() == 2) {
        return noBrokerClusterOpt;
      } else {
        return fullClusterOpt;
      }
    });
    InitiateShutdownResponse successfulResponse = new InitiateShutdownResponse(
        new InitiateShutdownResponseData().setErrorCode(Errors.NONE.code())
    );
    when(mockShutdownClient.sendShutdownRequest(any())).thenReturn(successfulResponse);

    boolean didShutdown = removalManager.maybeShutdownBroker(brokerIdToRemove, Optional.of(brokerEpoch));
    assertTrue("Expected to have shut down the broker proactively", didShutdown);

    verify(mockShutdownClient, times(1)).sendShutdownRequest(any());
    // if describeCluster was called more than once
    // then shutdown was awaited successfully
    verifyDescribeClusterCalled(2);
  }

  @Test
  public void testMaybeShutdownThrowsExceptionIfUnableToInitiallyGetClusterMetadata() throws Exception {
    Exception e = new Exception("failed!");
    when(mockAdminUtils.describeCluster(Long.parseLong(apiTimeoutMs))).thenReturn(new AdminClientResult<>(e));

    assertThrows(ExecutionException.class,
        () -> removalManager.maybeShutdownBroker(brokerIdToRemove, Optional.of(brokerEpoch)));

    verify(mockShutdownClient, times(0)).sendShutdownRequest(any());
    verifyDescribeClusterCalled(1);
  }

  @Test
  public void testMaybeShutdownDoesNotInitiateShutdownIfBrokerNotPartOfCluster() throws Exception {
    mockDescribeClusterBrokerRemoved();

    boolean didShutdown = removalManager.maybeShutdownBroker(brokerIdToRemove, Optional.of(brokerEpoch));
    assertFalse("Expected to not have proactively shut down broker since it was not part of the cluster",
        didShutdown);

    // no shutdown should have been called
    verify(mockShutdownClient, times(0)).sendShutdownRequest(any());
    verifyDescribeClusterCalled(1);
  }

  @Test
  public void testMaybeShutdownDoesNotInitiateShutdownIfBrokerEpochNotProvided() throws Exception {
    mockDescribeClusterFullCluster();

    assertThrows(IllegalArgumentException.class,
        () -> removalManager.maybeShutdownBroker(brokerIdToRemove, Optional.empty()));

    // no shutdown should have been called
    verify(mockShutdownClient, times(0)).sendShutdownRequest(any());
    verifyDescribeClusterCalled(1);
  }

  @Test
  public void testShutdownBrokerShutsDownBroker() throws Exception {
    InitiateShutdownResponse successfulResponse = new InitiateShutdownResponse(
        new InitiateShutdownResponseData().setErrorCode(Errors.NONE.code())
    );
    when(mockShutdownClient.sendShutdownRequest(any())).thenReturn(successfulResponse);
    mockDescribeClusterBrokerRemoved();

    removalManager.shutdownBroker(mockShutdownClient, brokerIdToRemove, brokerEpoch);

    verify(mockShutdownClient, times(1)).sendShutdownRequest(any());
    verifyDescribeClusterCalled(1); // if describeCluster was called then shutdown was awaited successfully
  }

  @Test
  public void testShutdownBrokerShutsDownBrokerAndThrowsTimeoutExceptionIfItTakesLongToLeaveCluster() throws Exception {
    InitiateShutdownResponse successfulResponse = new InitiateShutdownResponse(
        new InitiateShutdownResponseData().setErrorCode(Errors.NONE.code())
    );
    when(mockShutdownClient.sendShutdownRequest(any())).thenReturn(successfulResponse);
    mockDescribeClusterFullCluster(); // the broker never gets removed

    assertThrows(TimeoutException.class,
        () -> removalManager.shutdownBroker(mockShutdownClient, brokerIdToRemove, brokerEpoch));

    verify(mockShutdownClient, times(1)).sendShutdownRequest(any());
  }

  /**
   * We treat an #{@link java.io.IOException} as a failure to receive
   * the response from the broker and assume that the shutdown was successful
   */
  @Test
  public void testShutdownBrokerHandlesNoShutdownResponse() throws Exception {
    when(mockShutdownClient.sendShutdownRequest(any())).thenThrow(new IOException("disconnected!"));
    mockDescribeClusterBrokerRemoved();

    removalManager.shutdownBroker(mockShutdownClient, brokerIdToRemove, brokerEpoch);

    verify(mockShutdownClient, times(1)).sendShutdownRequest(any());
    verifyDescribeClusterCalled(1); // if describeCluster was called then shutdown was awaited successfully
  }

  @Test
  public void testShutdownBrokerHandlesAndReThrowsConnectionException() throws Exception {
    when(mockShutdownClient.sendShutdownRequest(any())).thenThrow(new ConnectionException("Could not connect!", null));

    ExecutionException exc = assertThrows(ExecutionException.class,
        () -> removalManager.shutdownBroker(mockShutdownClient, brokerIdToRemove, brokerEpoch));
    assertTrue("Expected cause to be ConnectionException", exc.getCause() instanceof ConnectionException);
    verify(mockShutdownClient, times(1)).sendShutdownRequest(any());
  }

  @Test
  public void testShutdownBrokerHandlesAndReThrowsGenericException() throws Exception {
    when(mockShutdownClient.sendShutdownRequest(any())).thenAnswer(invocation -> {
      throw new Exception("Something generic!");
    });

    ExecutionException exc = assertThrows(ExecutionException.class,
        () -> removalManager.shutdownBroker(mockShutdownClient, brokerIdToRemove, brokerEpoch));

    assertTrue("Expected cause to be Exception", exc.getCause() instanceof Exception);
    verify(mockShutdownClient, times(1)).sendShutdownRequest(any());
  }

  @Test
  public void testShutdownBrokerHandlesAndThrowsExceptionOnResponseAPIError() throws Exception {
    InitiateShutdownResponse staleEpochErrorResponse = new InitiateShutdownResponse(
        new InitiateShutdownResponseData().setErrorCode(Errors.STALE_BROKER_EPOCH.code()).setErrorMessage("Stale epoch!")
    );
    when(mockShutdownClient.sendShutdownRequest(any())).thenReturn(staleEpochErrorResponse);

    ApiException exc = assertThrows(ApiException.class,
        () -> removalManager.shutdownBroker(mockShutdownClient, brokerIdToRemove, brokerEpoch));

    assertTrue("Expected exception to be StaleBrokerEpochException", exc instanceof StaleBrokerEpochException);
    verify(mockShutdownClient, times(1)).sendShutdownRequest(any());
  }

  @Test
  public void testAwaitBrokerShutdownDescribeClusterCallsFailRetriesAndWaitsUntilTimeoutAndThrowsException()
      throws InterruptedException {
    AtomicInteger describeCalls = new AtomicInteger();

    when(mockAdminUtils.describeCluster(Long.parseLong(apiTimeoutMs))).thenAnswer(invocation -> {
      // simulate a couple of stale calls
      if (describeCalls.incrementAndGet() % 2 == 0) {
        return new AdminClientResult<>(new Exception("error"));
      } else {
        return fullClusterOpt;
      }
    });

    try {
      removalManager.awaitBrokerShutdown(mockTimeTickMs * 10, brokerIdToRemove);
      fail("Expected a TimeoutException to be thrown");
    } catch (TimeoutException e) {
      // success
    }

    verifyDescribeClusterCalled(11);
  }

  @Test
  public void testAwaitBrokerShutdownStopsWhenDescribeClusterReturnsBrokerMissing()
      throws InterruptedException {
    AtomicInteger describeCalls = new AtomicInteger();
    when(mockAdminUtils.describeCluster(Long.parseLong(apiTimeoutMs))).thenAnswer(invocation -> {
      // simulate a couple of stale calls
      if (describeCalls.incrementAndGet() == 5) {
        return noBrokerClusterOpt;
      } else {
        return fullClusterOpt;
      }
    });

    removalManager.awaitBrokerShutdown(mockTimeTickMs * 10, brokerIdToRemove);
    verifyDescribeClusterCalled(5);
  }

  private void mockDescribeClusterBrokerRemoved() throws InterruptedException {
    when(mockAdminUtils.describeCluster(Long.parseLong(apiTimeoutMs)))
        .thenReturn(new AdminClientResult<>(new KafkaCluster(clusterWithBrokerRemoved, null, null, null)));
  }

  private void mockDescribeClusterFullCluster() throws InterruptedException {
    when(mockAdminUtils.describeCluster(Long.parseLong(apiTimeoutMs)))
        .thenReturn(new AdminClientResult<>(new KafkaCluster(fullCluster, null, null, null)));
  }

  private void verifyDescribeClusterCalled(int times) throws InterruptedException {
    verify(mockAdminUtils, times(times)).describeCluster(Long.parseLong(apiTimeoutMs));
  }

  private KafkaCruiseControlConfig config() {
    Properties props = new Properties();
    props.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap.servers");
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, "connect:1234");
    props.setProperty(KafkaCruiseControlConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, apiTimeoutMs);
    props.setProperty(KafkaCruiseControlConfig.BROKER_REMOVAL_SHUTDOWN_MS_CONFIG, removalShutdownMs);
    return new KafkaCruiseControlConfig(props);
  }
}
