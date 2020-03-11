/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NoReassignmentInProgressException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;

public class ExecutorAdminUtilsTest {
  private final Node _controllerNode = new Node(0, "host0", 100);
  private final Node _simpleNode = new Node(1, "host1", 100);
  private final List<Node> _nodes = Arrays.asList(_controllerNode, _simpleNode);
  private final Integer[] _nodeIds = _nodes.stream().map(Node::id).collect(Collectors.toList()).toArray(new Integer[0]);

  private final List<String> _topics = Collections.singletonList(TOPIC0);
  private final int _p0 = 0;
  private final int _p1 = 1;
  private final int _p2 = 2;
  private final TopicPartition _t0P0 = new TopicPartition(TOPIC0, _p0);
  private final TopicPartition _t0P1 = new TopicPartition(TOPIC0, _p1);
  private final TopicPartition _t0P2 = new TopicPartition(TOPIC0, _p2);
  private final TopicDescription _topic0TopicDescription =
      new TopicDescription(TOPIC0, false, Collections.singletonList(
          defaultTopicPartitionInfo(_t0P0)
      ));

  private final long _defaultDescribeTopicsTimeoutMs = KafkaCruiseControlConfig.DEFAULT_DESCRIBE_TOPICS_RESPONSE_TIMEOUT_MS;

  @Test
  public void testCancelInterBrokerReplicaMovements() throws ExecutionException, InterruptedException {
    int expectedSuccessfulCancellations = 2;
    Map<TopicPartition, Optional<NewPartitionReassignment>> expectedPartitionCancellations = new HashMap<>();
    expectedPartitionCancellations.put(_t0P0, Optional.empty());
    expectedPartitionCancellations.put(_t0P1, Optional.empty());
    expectedPartitionCancellations.put(_t0P2, Optional.empty());
    Map<TopicPartition, KafkaFuture<Void>> response = new HashMap<>();
    KafkaFuture<Void> mockTP0Future = EasyMock.mock(KafkaFuture.class);

    mockTP0Future.get();
    EasyMock.expectLastCall().andAnswer(() -> {
      return null;
    });
    response.put(_t0P0, mockTP0Future);
    KafkaFuture<Void> mockTP1Future = EasyMock.mock(KafkaFuture.class);
    response.put(_t0P1, mockTP1Future);
    mockTP1Future.get();
    EasyMock.expectLastCall().andAnswer(() -> {
      return null;
    });
    KafkaFuture<Void> mockTP2Future = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockTP2Future.get()).andThrow(new NoReassignmentInProgressException(""));
    response.put(_t0P2, mockTP2Future);
    EasyMock.replay(mockTP0Future, mockTP1Future, mockTP2Future);

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    AlterPartitionReassignmentsResult resultsMock = EasyMock.mock(AlterPartitionReassignmentsResult.class);
    EasyMock.expect(resultsMock.values()).andReturn(response);
    EasyMock.expect(mockAdminClient.alterPartitionReassignments(expectedPartitionCancellations)).andReturn(resultsMock);
    EasyMock.replay(resultsMock, mockAdminClient);

    int receivedNumSuccessfulCancellations =
        ExecutorAdminUtils.cancelInterBrokerReplicaMovements(Arrays.asList(_t0P0, _t0P1, _t0P2), mockAdminClient);

    Assert.assertEquals(expectedSuccessfulCancellations, receivedNumSuccessfulCancellations);
  }

  @Test
  public void testCancelInterBrokerReplicaMovementsWhenUnsupported() throws ExecutionException, InterruptedException {
    int expectedSuccessfulCancellations = 0;
    Map<TopicPartition, Optional<NewPartitionReassignment>> expectedPartitionCancellations = new HashMap<>();
    expectedPartitionCancellations.put(_t0P0, Optional.empty());
    expectedPartitionCancellations.put(_t0P1, Optional.empty());
    expectedPartitionCancellations.put(_t0P2, Optional.empty());
    Map<TopicPartition, KafkaFuture<Void>> response = new HashMap<>();
    KafkaFuture<Void> mockTP0Future = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockTP0Future.get()).andThrow(new ExecutionException("failure", new UnsupportedVersionException("Unsupported!")));
    response.put(_t0P0, mockTP0Future);
    response.put(_t0P1, mockTP0Future);
    response.put(_t0P2, mockTP0Future);
    EasyMock.replay(mockTP0Future);
    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    AlterPartitionReassignmentsResult resultsMock = EasyMock.mock(AlterPartitionReassignmentsResult.class);
    EasyMock.expect(resultsMock.values()).andReturn(response);
    EasyMock.expect(mockAdminClient.alterPartitionReassignments(expectedPartitionCancellations)).andReturn(resultsMock);
    EasyMock.replay(resultsMock, mockAdminClient);

    int receivedNumSuccessfulCancellations =
        ExecutorAdminUtils.cancelInterBrokerReplicaMovements(Arrays.asList(_t0P0, _t0P1, _t0P2), mockAdminClient);

    Assert.assertEquals(expectedSuccessfulCancellations, receivedNumSuccessfulCancellations);
  }

  @Test
  public void testGetReplicasForPartitionReturnsReplicasForPartition()
      throws InterruptedException, ExecutionException, TimeoutException {
    KafkaCruiseControlConfig config = config(10L);
    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    Map<String, TopicDescription> topicDescriptions = new HashMap<>();
    topicDescriptions.put(TOPIC0, _topic0TopicDescription);
    KafkaCruiseControlUnitTestUtils.mockDescribeTopics(mockAdminClient, _topics, topicDescriptions, 10);
    EasyMock.replay(mockAdminClient);

    List<Integer> receivedReplicas = ExecutorAdminUtils.getReplicasForPartition(mockAdminClient, _t0P0, config);
    Assert.assertArrayEquals(_nodeIds, receivedReplicas.toArray(new Integer[0]));
  }

  @Test
  public void testGetReplicasForPartitionReturnsEmptyReplicasForPartitionDuringExceptions()
      throws InterruptedException, ExecutionException, TimeoutException {
    KafkaCruiseControlConfig config = config();
    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);

    // 1. Mock #describeTopics() to throw an exception
    EasyMock.expect(mockAdminClient.describeTopics(_topics)).andThrow(new InvalidTopicException());
    EasyMock.replay(mockAdminClient);

    List<Integer> receivedReplicas = ExecutorAdminUtils.getReplicasForPartition(mockAdminClient, _t0P0, config);
    Assert.assertEquals(0, receivedReplicas.size());

    EasyMock.reset(mockAdminClient);
    // 2. Mock #describeTopics().all().get() to throw an exception
    DescribeTopicsResult mockDescribeTopicsResult = EasyMock.mock(DescribeTopicsResult.class);
    KafkaFuture<Map<String, TopicDescription>> mockKafkaFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockKafkaFuture.get(_defaultDescribeTopicsTimeoutMs, TimeUnit.MILLISECONDS))
            .andThrow(new TimeoutException());
    EasyMock.expect(mockDescribeTopicsResult.all()).andReturn(mockKafkaFuture);
    EasyMock.expect(mockAdminClient.describeTopics(_topics)).andReturn(mockDescribeTopicsResult);
    // cannot mock properly due to 1. EasyMock requiring both matchers to be concise at once
    // and 2. DescribeTopicsOptions being compared by reference, resulting in an invalid expectation
    EasyMock.expect(mockAdminClient
        .describeTopics(EasyMock.<Collection<String>>anyObject(),
            EasyMock.anyObject(DescribeTopicsOptions.class)))
        .andReturn(mockDescribeTopicsResult);
    EasyMock.replay(mockDescribeTopicsResult, mockKafkaFuture, mockAdminClient);

    receivedReplicas = ExecutorAdminUtils.getReplicasForPartition(mockAdminClient, _t0P0, config);
    Assert.assertEquals(0, receivedReplicas.size());
  }

  @Test
  public void testGetReplicasForPartitionReturnsEmptyReplicasForPartitionIfPartitionNotPresent()
      throws InterruptedException, ExecutionException, TimeoutException {
    KafkaCruiseControlConfig config = config();
    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    Map<String, TopicDescription> topicDescriptions = new HashMap<>();
    topicDescriptions.put(TOPIC0, _topic0TopicDescription);
    KafkaCruiseControlUnitTestUtils.mockDescribeTopics(mockAdminClient, _topics, topicDescriptions, _defaultDescribeTopicsTimeoutMs);
    EasyMock.replay(mockAdminClient);

    List<Integer> receivedReplicas = ExecutorAdminUtils.getReplicasForPartition(mockAdminClient,
                                                                                new TopicPartition(TOPIC0, 101),
                                                                                config);
    Assert.assertEquals(0, receivedReplicas.size());
  }

  private TopicPartitionInfo defaultTopicPartitionInfo(TopicPartition tp) {
    return new TopicPartitionInfo(tp.partition(), _controllerNode, _nodes, _nodes);
  }

  private KafkaCruiseControlConfig config() {
    return config(null);
  }

  private KafkaCruiseControlConfig config(Long timeoutMs) {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    if (timeoutMs != null) {
      props.setProperty(KafkaCruiseControlConfig.DESCRIBE_TOPICS_RESPONSE_TIMEOUT_MS_CONFIG, timeoutMs.toString());
    }
    return new KafkaCruiseControlConfig(props);
  }
}
