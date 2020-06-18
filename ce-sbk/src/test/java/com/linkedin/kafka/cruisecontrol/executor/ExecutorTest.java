/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetector;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaClientsIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import com.yammer.metrics.core.MetricsRegistry;
import java.time.Duration;
import io.confluent.databalancer.operation.BalanceOpExecutionCompletionCallback;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import kafka.controller.ReplicaAssignment;
import kafka.server.ConfigType;
import kafka.server.ConfigType$;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfluentTopicConfig;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.Mockito;


import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.configResourcesForBrokers;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC2;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC3;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.AUTO_THROTTLE;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.NO_THROTTLE;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorNotification.ActionAgent.CRUISE_CONTROL;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorNotification.ActionAgent.EXECUTION_COMPLETION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorNotification.ActionAgent.UNKNOWN;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExecutorTest extends CCKafkaClientsIntegrationTestHarness {
  private static final int PARTITION = 0;
  private static final int REPLICA_FETCH_MAX_BYTES = 10000;
  private static final TopicPartition TP0 = new TopicPartition(TOPIC0, PARTITION);
  private static final TopicPartition TP1 = new TopicPartition(TOPIC1, PARTITION);
  private static final TopicPartition TP2 = new TopicPartition(TOPIC2, PARTITION);
  private static final TopicPartition TP3 = new TopicPartition(TOPIC3, PARTITION);
  private static final String RANDOM_UUID = "random_uuid";
  private static final String DESCRIBE_TOPICS_RESPONSE_TIMEOUT_MS = "10000";
  private MetricsRegistry metricsRegistry;
  private Map<String, TopicDescription> topicsCreated = new HashMap<>();

  @Override
  public int clusterSize() {
    return 2;
  }

  /** Overriding the default configuration for tests added in Confluent's fork. */
  @Override
  protected Map<Object, Object> overridingProps() {
    Map<Object, Object> props = new HashMap<>();
    props.put(KafkaConfig.DeleteTopicEnableProp(), "true");
    props.put(KafkaConfig.ReplicaFetchMaxBytesProp(), String.valueOf(REPLICA_FETCH_MAX_BYTES));
    return props;
  }

  @Override
  protected String rackForNode(int nodeId) {
    return String.valueOf(nodeId);
  }

  @Before
  public void setUp() {
    super.setUp();
    metricsRegistry = new MetricsRegistry();
  }

  @After
  public void tearDown() {
    super.tearDown();
    metricsRegistry.shutdown();
  }

  @Test
  public void testBasicBalanceMovement() throws InterruptedException {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
                                                                              "ExecutorTestMetricGroup",
                                                                              "BasicBalanceMovement",
                                                                              false);
    try {
      Collection<ExecutionProposal> proposals = getBasicProposals();
      executeAndVerifyProposals(kafkaZkClient, proposals, proposals).shutdown();
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Test
  public void testRebalanceObserverMovement() throws InterruptedException {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
            "ExecutorTestMetricGroup",
            "BasicBalanceMovement",
            false);
    try {
      Collection<ExecutionProposal> proposals = getTopicPlacementProposals(kafkaZkClient);
      executeAndVerifyProposals(kafkaZkClient, proposals, proposals).shutdown();
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }


  @Test
  public void testRebalanceCancellation() throws InterruptedException, ExecutionException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());
    ConfluentAdmin admin = KafkaCruiseControlUtils.createAdmin(config.originals());
    try {
      Map<String, TopicDescription> topicDescriptions = createTopics();
      int initialLeader = topicDescriptions.get(TP0.topic()).partitions().get(TP0.partition()).leader().id();
      Executor executor = fillUpAndMoveBasicTopicPartition0(1L);

      assertReassignmentsStarted(admin, 1);
      assertEquals(0, executor.numCancelledReassignments());

      executor.stopExecution();

      assertReassignmentsCanceled(admin, initialLeader, executor);
      executor.shutdown();
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(admin);
    }
  }

  private void assertReassignmentsStarted(ConfluentAdmin admin, int numExpectedStartedReassignments) throws InterruptedException {
    waitForAssert(() -> {
      // assert reassignment is in progress
      assertEquals(numExpectedStartedReassignments, admin.listPartitionReassignments().reassignments().get().size());
      return true;
    }, 10000, "Should have started reassigning");
  }

  /**
   * Assert that the reassignment for #{@code TP0} is canceled
   */
  private void assertReassignmentsCanceled(ConfluentAdmin admin, int initialLeader, Executor executor) throws InterruptedException {
    int expectedNumCanceled = 1;
    Set<String> topic = new HashSet<>();
    topic.add(TP0.topic());

    waitForAssert(() -> {
      assertEquals(0, admin.listPartitionReassignments().reassignments().get().size());
      // assert reassignment is cancelled and didn't occur
      assertEquals(initialLeader, admin.describeTopics(topic).all().get().get(TP0.topic()).partitions().get(TP0.partition()).leader().id());
      return true;
    },  5000, "Should have reverted the reassignment");

    waitForAssert(() -> {
      assertEquals(expectedNumCanceled, executor.numCancelledReassignments());
      return true;
    },  5000, "Reassignment cancellation should be reflected in the metrics");
  }

  @Test
  public void testExecutionProposalCompletionCbHandlesCompletion() throws InterruptedException, ExecutionException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());
    ConfluentAdmin admin = KafkaCruiseControlUtils.createAdmin(config.originals());
    try {
      Map<String, TopicDescription> topicDescriptions = createTopics();
      BalanceOpExecutionCompletionCallback mockCb = Mockito.mock(BalanceOpExecutionCompletionCallback.class);

      ExecutionProposal proposal = getBasicTopicPartition0Proposal();
      Executor executor =  executeProposals(executor(), Collections.singletonList(proposal), 1L, mockCb);

      waitUntilExecutionFinishes(executor);

      Mockito.verify(mockCb).accept(Mockito.eq(true), Mockito.isNull());
      executor.shutdown();
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(admin);
    }
  }

  @Test
  public void testExecutionProposalCompletionCbHandlesStop() throws InterruptedException, ExecutionException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());
    ConfluentAdmin admin = KafkaCruiseControlUtils.createAdmin(config.originals());
    try {
      Map<String, TopicDescription> topicDescriptions = createTopics();
      int initialLeader = topicDescriptions.get(TP0.topic()).partitions().get(TP0.partition()).leader().id();
      // TODO: Icky suppression check. Make a general interface?
      BalanceOpExecutionCompletionCallback mockCb = Mockito.mock(BalanceOpExecutionCompletionCallback.class);
      ExecutionProposal proposal = getBasicTopicPartition0Proposal();
      produceData(TP0.topic(), REPLICA_FETCH_MAX_BYTES * 3);

      Executor executor =  executeProposals(executor(), Collections.singletonList(proposal), 1L, mockCb);

      assertReassignmentsStarted(admin, 1);
      executor.stopExecution();

      waitUntilExecutionFinishes(executor);
      Mockito.verify(mockCb).accept(Mockito.eq(false), Mockito.isNull());
      executor.shutdown();
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(admin);
    }
  }


  /**
   * Assert that the reservation cannot be acquired by another thread while it is held by the testing thread.
   */
  @Test
  public void testAbortAndAcquireThrowsIllegalStateExceptionWhenLockAcquired() throws Exception {
    Executor executor = executor();
    // get reservation
    try (AutoCloseable ignored = executor.reserveAndAbortOngoingExecutions(Duration.ofSeconds(5))) {
      assertFalse("Expected Executor to not be reserved for the current thread", executor.isReservedByOther());
      assertTrue("Expected the executor's reservation to be taken", executor._reservation.isReserved());
      assertIllegalStateExceptionThrownWhileReservationIsHeld(executor, () -> {
        try {
          executor.reserveAndAbortOngoingExecutions(Duration.ofMillis(100));
        } catch (TimeoutException e) {
          throw new RuntimeException(e);
        }
      });
      assertAbortAndAcquireThrowsIllegalStateExceptionWhenReservationAcquired(executor);
      assertTrue("Expected the executor's reservation to be taken", executor._reservation.isReserved());
      assertFalse("Expected Executor to not be reserved for the current thread", executor.isReservedByOther());
    }
  }

  /**
   * Spin up a new thread that wouldn't be able to get the executor's reservation,
   *   assert that the underlying reservation is taken by another (the main test) thread
   */
  private void assertAbortAndAcquireThrowsIllegalStateExceptionWhenReservationAcquired(Executor executor) throws InterruptedException {
    AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
    Thread th = new Thread(() -> {
      try {
        assertTrue("Expected the executor to be reserved", executor.isReservedByOther());
        executor.reserveAndAbortOngoingExecutions(Duration.ofMillis(100));
        exceptionRef.set(new Exception("Expected Executor reservation to be held by another thread and this thread to not be able to reserve it."));
      } catch (Throwable e) {
        exceptionRef.set(e);
      }
    });
    th.start();
    th.join();

    assertNotNull("Expected exception to be populated", exceptionRef.get());
    assertTrue("Expected populated exception to be IllegalStateException", exceptionRef.get() instanceof IllegalStateException);
    assertTrue("Expected the executor's reservation to be taken", executor._reservation.isReserved());
  }

  /**
   * Asserts that an #{@link IllegalStateException} is thrown by the given #{@code runnable}
   * while the Executor is reserved
   */
  private void assertIllegalStateExceptionThrownWhileReservationIsHeld(Executor executor, Runnable runnable) throws InterruptedException {
    AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
    Thread th = new Thread(() -> {
      try {
        assertTrue("Expected the executor to be reserved", executor.isReservedByOther());
        runnable.run();
        exceptionRef.set(new Exception("Expected Executor reservation to be held by another thread and this thread to not be able to reserve it."));
      } catch (Throwable e) {
        exceptionRef.set(e);
      }
    });
    th.start();
    th.join();

    assertNotNull("Expected exception to be populated", exceptionRef.get());
    assertTrue("Expected populated exception to be IllegalStateException", exceptionRef.get() instanceof IllegalStateException);
    assertTrue("Expected the executor's reservation to be taken", executor._reservation.isReserved());
  }

  /**
   * Assert that #{@link Executor#reserveAndAbortOngoingExecutions(Duration)} aborts the current execution and acquires the reservation
   */
  @Test
  public void testAbortAndAcquireCancelsCurrentRebalanceAndGetsReservation() throws InterruptedException, ExecutionException, TimeoutException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());

    Duration abortionTimeout = Duration.ofMillis(10000L); // give the Executor some time to abort the execution
    ConfluentAdmin admin = KafkaCruiseControlUtils.createAdmin(config.originals());
    try {
      Map<String, TopicDescription> topicDescriptions = createTopics();
      int initialLeader = topicDescriptions.get(TP0.topic()).partitions().get(TP0.partition()).leader().id();
      Executor executor = fillUpAndMoveBasicTopicPartition0(1L);

      assertReassignmentsStarted(admin, 1);
      assertEquals(0, executor.numCancelledReassignments());

      try (Executor.ReservationHandle ignored = executor.reserveAndAbortOngoingExecutions(abortionTimeout)) {
        assertFalse("Expected ongoing execution to be canceled", executor.hasOngoingExecution());

        // assert execution canceled/reverted
        assertReassignmentsCanceled(admin, initialLeader, executor);

        // assert lock is taken
        assertTrue("Expected the executor's reservation to be taken", executor._reservation.isReserved());
        assertTrue("Expected reservation to be held by the test thread",
            executor._reservation.isReservedByMe());
        assertFalse("Expected the Executor to not be reserved for the test thread that holds the reservation",
            executor.isReservedByOther());
      }

      executor.shutdown();
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(admin);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotAcquireReservationTwice() throws TimeoutException {
    Executor executor = executor();
    executor.reserveAndAbortOngoingExecutions(Duration.ofSeconds(1));
    executor.reserveAndAbortOngoingExecutions(Duration.ofSeconds(1));
  }

  /**
   * Trigger a slow reassignment and assert that:
   * 1. #{@link Executor#reserveAndAbortOngoingExecutions(Duration)} tries to abort the current execution but gives up after the timeout passes
   * 2. #{@link Executor#reserveAndAbortOngoingExecutions(Duration)} releases its reservation after it gives up waiting for execution abortion
   */
  @Test
  public void testAbortAndAcquireShortAbortTimeoutThrowsExceptionAndGivesUpReservation() throws ExecutionException, InterruptedException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());

    Duration smallExecutionTimeout = Duration.ofMillis(50L); // very small timeout to ensure we time out
    ConfluentAdmin admin = KafkaCruiseControlUtils.createAdmin(config.originals());
    try {
      Map<String, TopicDescription> topicDescriptions = createTopics();
      int initialLeader = topicDescriptions.get(TP0.topic()).partitions().get(TP0.partition()).leader().id();
      Executor executor = fillUpAndMoveBasicTopicPartition0(1L);

      assertReassignmentsStarted(admin, 1);
      assertEquals(0, executor.numCancelledReassignments());

      try {
        executor.reserveAndAbortOngoingExecutions(smallExecutionTimeout);
      } catch (TimeoutException te) {
        assertFalse("Expected the reservation to not be taken", executor._reservation.isReserved());
        assertFalse("Expected the Executor's reservation to not be held", executor.isReservedByOther());
      }

      assertReassignmentsCanceled(admin, initialLeader, executor);

      executor.shutdown();
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(admin);
    }
  }

  @Test
  public void testProposalsCannotBeExecutedWhileReservationHeld() throws TimeoutException, InterruptedException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());

    Duration abortionTimeout = Duration.ofMillis(50L);
    ConfluentAdmin admin = KafkaCruiseControlUtils.createAdmin(config.originals());
    try {
      Executor executor = executor();

      try (Executor.ReservationHandle ignored = executor.reserveAndAbortOngoingExecutions(abortionTimeout)) {
        // assert reservation is taken
        assertTrue("Expected the executor's reservation to be taken", executor._reservation.isReserved());
        assertTrue("Expected reservation to be held by the test thread",
            executor._reservation.isReservedByMe());
        assertFalse("Expected the Executor to not be reserved for the test thread that holds the reservation",
            executor.isReservedByOther());

        assertIllegalStateExceptionThrownWhileReservationIsHeld(executor, () -> {
          ExecutionProposal proposal;
          try {
            proposal = getBasicTopicPartition0Proposal();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          executeProposals(executor, Collections.singletonList(proposal), 1L);
        });
      }

      assertReassignmentsStarted(admin, 0);
      assertEquals(0, executor.numCancelledReassignments());

      executor.shutdown();
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(admin);
    }
  }

  @Test
  public void testMoveNonExistingPartition() throws InterruptedException {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
                                                                              "ExecutorTestMetricGroup",
                                                                              "MoveNonExistingPartition",
                                                                              false);
    try {
      Map<String, TopicDescription> topicDescriptions = createTopics();
      int initialLeader0 = topicDescriptions.get(TOPIC0).partitions().get(0).leader().id();
      int initialLeader1 = topicDescriptions.get(TOPIC1).partitions().get(0).leader().id();

      ExecutionProposal proposal0 =
          new ExecutionProposal(TP0, 0, new ReplicaPlacementInfo(initialLeader0),
                                Collections.singletonList(new ReplicaPlacementInfo(initialLeader0)),
                                Collections.singletonList(initialLeader0 == 0 ? new ReplicaPlacementInfo(1) :
                                                                                new ReplicaPlacementInfo(0)), Collections.emptyList(), Collections.emptyList());
      ExecutionProposal proposal1 =
          new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(initialLeader1),
                                Arrays.asList(new ReplicaPlacementInfo(initialLeader1),
                                              initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                    new ReplicaPlacementInfo(0)),
                                Arrays.asList(initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                    new ReplicaPlacementInfo(0),
                                              new ReplicaPlacementInfo(initialLeader1)), Collections.emptyList(), Collections.emptyList());
      ExecutionProposal proposal2 =
          new ExecutionProposal(TP2, 0, new ReplicaPlacementInfo(initialLeader0),
                                Collections.singletonList(new ReplicaPlacementInfo(initialLeader0)),
                                Collections.singletonList(initialLeader0 == 0 ? new ReplicaPlacementInfo(1) :
                                                                                new ReplicaPlacementInfo(0)), Collections.emptyList(), Collections.emptyList());
      ExecutionProposal proposal3 =
          new ExecutionProposal(TP3, 0, new ReplicaPlacementInfo(initialLeader1),
                                Arrays.asList(new ReplicaPlacementInfo(initialLeader1),
                                              initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                    new ReplicaPlacementInfo(0)),
                                Arrays.asList(initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                    new ReplicaPlacementInfo(0),
                                              new ReplicaPlacementInfo(initialLeader1)), Collections.emptyList(), Collections.emptyList());

      Collection<ExecutionProposal> proposalsToExecute = Arrays.asList(proposal0, proposal1, proposal2, proposal3);
      Collection<ExecutionProposal> proposalsToCheck = Arrays.asList(proposal0, proposal1);
      executeAndVerifyProposals(kafkaZkClient, proposalsToExecute, proposalsToCheck).shutdown();
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Test
  public void testMoveDeletedTopic() throws InterruptedException, ExecutionException {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
        "ExecutorTestMetricGroup",
        "MoveNonExistingTopic",
        false);
    try {
      Map<String, TopicDescription> topicDescriptions = createTopics();
      int initialLeader0 = topicDescriptions.get(TOPIC0).partitions().get(0).leader().id();
      int initialLeader1 = topicDescriptions.get(TOPIC1).partitions().get(0).leader().id();

      ExecutionProposal proposal0 =
          new ExecutionProposal(TP0, 0, new ReplicaPlacementInfo(initialLeader0),
              Collections.singletonList(new ReplicaPlacementInfo(initialLeader0)),
              Collections.singletonList(initialLeader0 == 0 ? new ReplicaPlacementInfo(1) :
                  new ReplicaPlacementInfo(0)), Collections.emptyList(), Collections.emptyList());
      ExecutionProposal proposal1 =
          new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(initialLeader1),
              Arrays.asList(new ReplicaPlacementInfo(initialLeader1),
                  initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                      new ReplicaPlacementInfo(0)),
              Arrays.asList(initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                      new ReplicaPlacementInfo(0),
                  new ReplicaPlacementInfo(initialLeader1)), Collections.emptyList(), Collections.emptyList());

      Collection<ExecutionProposal> proposalsToExecute = Arrays.asList(proposal0, proposal1);
      Collection<ExecutionProposal> proposalsToCheck = Arrays.asList(proposal1);

      Executor executor = createExecutor();
      LoadMonitor loadMonitor = EasyMock.mock(LoadMonitor.class);

      // Initialize the proposal
      executor.initProposalExecution(proposalsToExecute, Collections.emptySet(), null, null, null,
                                     null, RANDOM_UUID);

      // Delete the topic after the proposal has been initialized but before it is executed.
      deleteTopics(Collections.singletonList(TOPIC0));

      // Execute the proposal
      executor.startExecution(
          executor.new ProposalExecutionRunnable(loadMonitor, null, null, 1024L, null)
      );

      waitAndVerifyProposals(kafkaZkClient, executor, proposalsToCheck);

      executor.shutdown();
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Test
  public void testTimeoutLeaderActions() throws InterruptedException {
    createTopics();
    // The proposal tries to move the leader. We fake the replica list to be unchanged so there is no replica
    // movement, but only leader movement.
    ExecutionProposal proposal =
        new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(1),
                              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)), Collections.emptyList(), Collections.emptyList());

    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());
    Time time = new MockTime();
    MetadataClient mockMetadataClient = EasyMock.createMock(MetadataClient.class);
    // Fake the metadata to never change so the leader movement will timeout.
    Node node0 = new Node(0, "host0", 100);
    Node node1 = new Node(1, "host1", 100);
    Node[] replicas = new Node[2];
    replicas[0] = node0;
    replicas[1] = node1;
    PartitionInfo partitionInfo = new PartitionInfo(TP1.topic(), TP1.partition(), node1, replicas, replicas);
    Cluster cluster = new Cluster("id", Arrays.asList(node0, node1), Collections.singleton(partitionInfo),
                                  Collections.emptySet(), Collections.emptySet());
    MetadataClient.ClusterAndPlacements clusterAndPlacements = new MetadataClient.ClusterAndPlacements(cluster, Collections.emptyMap());
    MetadataClient.ClusterAndGeneration clusterAndGeneration = new MetadataClient.ClusterAndGeneration(clusterAndPlacements, 0);
    EasyMock.expect(mockMetadataClient.refreshMetadata()).andReturn(clusterAndGeneration).anyTimes();
    EasyMock.expect(mockMetadataClient.cluster()).andReturn(clusterAndGeneration.cluster()).anyTimes();
    mockMetadataClient.close();
    EasyMock.expectLastCall().andAnswer(() -> {
        return null;
    });
    EasyMock.replay(mockMetadataClient);

    Collection<ExecutionProposal> proposalsToExecute = Collections.singletonList(proposal);
    Executor executor = new Executor(configs, time, KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry),
            mockMetadataClient, 86400000L, 43200000L, null, getMockAnomalyDetector(RANDOM_UUID));
    executor.setExecutionMode(false);
    executor.executeProposals(proposalsToExecute,
                              Collections.emptySet(),
                              null,
                              EasyMock.mock(LoadMonitor.class),
                              null,
                              null,
                              null,
                              null,
                              NO_THROTTLE,
                              RANDOM_UUID,
                              null);
    // Wait until the execution to start so the task timestamp is set to time.milliseconds.
    while (executor.state().state() != ExecutorState.State.LEADER_MOVEMENT_TASK_IN_PROGRESS) {
      Thread.sleep(10);
    }
    // Sleep over 180000 (the hard coded timeout) with some margin for inter-thread synchronization.
    time.sleep(200000);
    // The execution should finish.
    waitUntilExecutionFinishes(executor);

    executor.shutdown();
  }

  @Test
  public void testExecutorSendNotificationForUserTask() throws InterruptedException {
    Collection<ExecutionProposal> proposals = getBasicProposals();
    String uuid = "user-task-uuid";
    // XXX: USER is no longer known. Remove test entirely?
    executeAndVerifyNotification(proposals, uuid, UNKNOWN, EXECUTION_COMPLETION, true);
  }

  @Test
  public void testReplicationThrottling() throws InterruptedException {
    executeProposalWithReplicationThrottling(null);
  }

  @Test
  public void testReplicationThrottlingOverride() throws InterruptedException {
    executeProposalWithReplicationThrottling(2000000L);
  }

  private void executeProposalWithReplicationThrottling(Long override) throws InterruptedException {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
            "LoadMonitorTaskRunnerGroup",
            "LoadMonitorTaskRunnerSetup",
            false);
    try {
        KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());

        Map<String, TopicDescription> topicDescriptions = createTopics();
        int oldReplica = topicDescriptions.get(TP0.topic()).partitions().get(0).leader().id();
        int newReplica = oldReplica == 0 ? 1 : 0;

        ExecutionProposal proposal =
                new ExecutionProposal(TP0, 0, new ReplicaPlacementInfo(oldReplica),
                        Collections.singletonList(new ReplicaPlacementInfo(oldReplica)),
                        Collections.singletonList(new ReplicaPlacementInfo(newReplica)), Collections.emptyList(), Collections.emptyList());

        Time time = new MockTime();
        MetadataClient metadataClient = new MetadataClient(config,
                -1L,
                time);

        // Set throttled replicas for topic1 to test that they are not removed by executing the task for topic 0
        Properties topic1Props = kafkaZkClient.getEntityConfigs(ConfigType.Topic(), TOPIC1);
        topic1Props.put(KafkaConfig.LeaderReplicationThrottledReplicasProp(), "0:0,0:1");
        topic1Props.put(KafkaConfig.FollowerReplicationThrottledReplicasProp(), "0:0,0:1");
        kafkaZkClient.setOrCreateEntityConfigs(ConfigType.Topic(), TOPIC1, topic1Props);

        Executor executor = new Executor(config, time, KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry),
                metadataClient, 86400000L, 43200000L, null, getMockAnomalyDetector(RANDOM_UUID));
        executor.setExecutionMode(false);
        executor.executeProposals(Collections.singletonList(proposal),
                Collections.emptySet(),
                null,
                EasyMock.mock(LoadMonitor.class),
                null,
                null,
                null,
                null,
                override,
            RANDOM_UUID,
                null);
        String expectedThrottle = override == null ? null : override.toString();
        String expectedReplicaLeaderThrottle = override == null ? null : "0:0,0:1";
        String expectedReplicaFollowerThrottle = override == null ? null : "0:0,0:1";
        waitForAssert(() -> {
            for (Integer broker : _brokers.keySet()) {
                verifyThrottleInZk(kafkaZkClient, ConfigType.Broker(), valueOf(broker), expectedThrottle);
            }
            // topic 0 should be throttled depending on the override, topic 1 should have the fixed replica throttle config
            verifyThrottleInZk(kafkaZkClient, ConfigType.Topic(), TOPIC0, expectedReplicaLeaderThrottle, expectedReplicaFollowerThrottle);
            verifyThrottleInZk(kafkaZkClient, ConfigType.Topic(), TOPIC1, "0:0,0:1");
            return true;
        }, 5000, "Should have properly throttled during the reassignment");
        waitUntilExecutionFinishes(executor);

        executor.shutdown();

        // Verify that throttles were removed after the rebalance finished
        for (Integer broker : _brokers.keySet()) {
            verifyThrottleInZk(kafkaZkClient, ConfigType.Broker(), valueOf(broker), null);
        }
        // topic 1 throttle configs should not have been cleared
        verifyThrottleInZk(kafkaZkClient, ConfigType.Topic(), TOPIC0, null);
        verifyThrottleInZk(kafkaZkClient, ConfigType.Topic(), TOPIC1, "0:0,0:1");
    } finally {
        KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Test
  public void testUpdateThrottleWithInitialThrottle() throws InterruptedException {
    updateThrottleForOngoingProposal(100L);
  }

  @Test
  public void testUpdateThrottleWithoutInitialThrottle() throws InterruptedException {
    updateThrottleForOngoingProposal(null);
  }

  private void updateThrottleForOngoingProposal(Long initialThrottle) throws InterruptedException {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
            "LoadMonitorTaskRunnerGroup",
            "LoadMonitorTaskRunnerSetup",
            false);

    try {
        Properties props = getExecutorProperties();
        KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

        Time time = new MockTime();
        MetadataClient metadataClient = new MetadataClient(config,
                -1L,
                time);

        Map<String, TopicDescription> topicDescriptions = createTopics();
        int leader0 = topicDescriptions.get(TOPIC0).partitions().get(0).leader().id();
        int leader1 = topicDescriptions.get(TOPIC1).partitions().get(0).leader().id();
        ExecutionProposal proposal0 =
                new ExecutionProposal(TP0, 0, new ReplicaPlacementInfo(leader0),
                        Collections.singletonList(new ReplicaPlacementInfo(leader0)),
                        Collections.singletonList(new ReplicaPlacementInfo(leader0 == 0 ? 1 : 0)), Collections.emptyList(), Collections.emptyList());
        ExecutionProposal proposal1 =
                new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(leader1),
                        Collections.singletonList(new ReplicaPlacementInfo(leader1)),
                        Collections.singletonList(new ReplicaPlacementInfo(leader1 == 0 ? 1 : 0)), Collections.emptyList(), Collections.emptyList());

        // Set concurrency such that multiple batches are submitted, to ensure that the requested throttle is not overwritten
        // Because the Executor doesn't update ZK if nothing has changed, only one update with the new value will be seen
        Executor executor = new Executor(config, time, KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry),
                metadataClient, 86400000L, 43200000L, null, getMockAnomalyDetector(RANDOM_UUID));
        executor.setExecutionMode(false);
        if (initialThrottle != null) {
            executor.updateThrottle(initialThrottle);
            waitForAssert(() -> {
                for (Integer broker : _brokers.keySet()) {
                    verifyThrottleInZk(kafkaZkClient, ConfigType.Broker(), valueOf(broker), Long.toString(initialThrottle));
                }
                return true;
            }, 3000, "Should have set the initial throttle");
        }
        executor.executeProposals(Arrays.asList(proposal0, proposal1),
                Collections.emptySet(),
                null,
                EasyMock.mock(LoadMonitor.class),
                1,
                1,
                1,
                null,
                initialThrottle,
            RANDOM_UUID,
                null);

        String expectedThrottle = initialThrottle == null ? null : Long.toString(initialThrottle);
        waitForAssert(() -> {
            for (Integer broker : _brokers.keySet()) {
                verifyThrottleInZk(kafkaZkClient, ConfigType.Broker(), valueOf(broker), expectedThrottle);
            }

            // We've verified that the initial throttle is applied, update the throttle during the reassignment
            // and ensure it is reflected in ZK
            try {
                waitForAssert(() -> {
                    String newExpectedThrottle = "1000000";
                    boolean succeeded = executor.updateThrottle(Long.parseLong(newExpectedThrottle));
                    assertTrue("Throttle rate update failed", succeeded);
                    for (Integer broker : _brokers.keySet()) {
                        verifyThrottleInZk(kafkaZkClient, ConfigType.Broker(), valueOf(broker), newExpectedThrottle);
                    }
                    return true;
                }, 5000, "The manual throttle update should have applied during the reassignment");
            } catch (InterruptedException e) {
                return false;
            }

            return true;
        }, 10000, "Should have properly throttled during the reassignment");

        waitUntilExecutionFinishes(executor);

        executor.shutdown();

        // Verify that throttles were removed after the rebalance finished
        for (Integer broker : _brokers.keySet()) {
            verifyThrottleInZk(kafkaZkClient, ConfigType.Broker(), valueOf(broker), null);
        }
        verifyThrottleInZk(kafkaZkClient, ConfigType.Topic(), TOPIC0, null);
        verifyThrottleInZk(kafkaZkClient, ConfigType.Topic(), TOPIC1, null);
    } finally {
        KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  /**
   * Verifies that the given throttle is present in ZooKeeper
   */
  private void verifyThrottleInZk(KafkaZkClient kafkaZkClient, String entityType,
                                  String entityName, String expectedLeaderThrottle,
                                  String expectedReplicaThrottle) throws AssertionError {
    if (entityType.equals(ConfigType.Broker())) {
      Properties dynamicConfig = kafkaZkClient.getEntityConfigs(ConfigType.Broker(), entityName);
      String leaderThrottle = dynamicConfig.getProperty(KafkaConfig.LeaderReplicationThrottledRateProp());
      String followerThrottle = dynamicConfig.getProperty(KafkaConfig.FollowerReplicationThrottledRateProp());
      assertEquals(expectedLeaderThrottle, leaderThrottle);
      assertEquals(expectedReplicaThrottle, followerThrottle);
    } else {
      Properties throttledReplicaConfig = kafkaZkClient.getEntityConfigs(ConfigType.Topic(), entityName);
      String throttledReplicaLeaderThrottle = throttledReplicaConfig.getProperty(KafkaConfig.LeaderReplicationThrottledReplicasProp());
      String throttledReplicaFollowerThrottle = throttledReplicaConfig.getProperty(KafkaConfig.FollowerReplicationThrottledReplicasProp());
      assertEquals(expectedLeaderThrottle, throttledReplicaLeaderThrottle);
      assertEquals(expectedReplicaThrottle, throttledReplicaFollowerThrottle);
    }
  }

  private void verifyThrottleInZk(KafkaZkClient kafkaZkClient, String entityType,
                                  String entityName, String expectedThrottle) {
    verifyThrottleInZk(kafkaZkClient, entityType, entityName, expectedThrottle, expectedThrottle);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRemoveBroker() throws InterruptedException {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
        "ExecutorTestMetricGroup",
        "RemoveBroker",
        false);
    try {
      Map<String, TopicDescription> topicDescriptions = createTopics();
      // remove broker 1
      List<ExecutionProposal> removeBrokerProposals = new ArrayList<>();
      for (Map.Entry<String, TopicDescription> topicDescriptionEntry : topicDescriptions.entrySet()) {
        for (TopicPartitionInfo partitionInfo : topicDescriptionEntry.getValue().partitions()) {
          removeBrokerProposals.add(new ExecutionProposal(
              new TopicPartition(topicDescriptionEntry.getKey(), partitionInfo.partition()),
              0,
              new ReplicaPlacementInfo(partitionInfo.leader().id()),
              partitionInfo.replicas().stream().map(node -> new ReplicaPlacementInfo(node.id())).collect(Collectors.toList()),
              Collections.singletonList(new ReplicaPlacementInfo(0)),
              Collections.emptyList(),
              Collections.emptyList()
          ));
        }
      }

      BalanceOpExecutionCompletionCallback mockCallback = Mockito.mock(BalanceOpExecutionCompletionCallback.class);
      Executor executor = executor();
      executor.setExecutionMode(false);
      executor.executeProposals(removeBrokerProposals, Collections.emptySet(), null, EasyMock.mock(LoadMonitor.class), null,
          null, null, null,
          NO_THROTTLE, RANDOM_UUID, mockCallback);

      waitAndVerifyProposals(kafkaZkClient, executor, removeBrokerProposals);
      Mockito.verify(mockCallback).accept(true, null);
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Test
  public void testProposalExecutionRunnableCallsCallbackWithException() {
    AnomalyDetector mockAnomalyDetector = Mockito.mock(AnomalyDetector.class);
    LoadMonitor mockLoadMonitor = Mockito.mock(LoadMonitor.class);
    // Throw one catchable exception during the execution.
    Mockito.doAnswer(invocation -> {
      throw new NullPointerException("haha got you");
    }).when(mockLoadMonitor).pauseMetricSampling(Mockito.anyString());

    Executor executor = createExecutor(mockAnomalyDetector);

    // Bleah, the ProposalExecutionRunnable depends on "global" (Executor) state. Initialize that.
    executor.initProposalExecution(Collections.emptySet(), Collections.emptySet(), null, null, null,
            null, RANDOM_UUID);

    BalanceOpExecutionCompletionCallback callbackMock = Mockito.mock(BalanceOpExecutionCompletionCallback.class);
    // pass in some nulls to trigger an NPE in the run() method,
    // and the null LoadMonitor will result in another in the finally() cleanup.
    Executor.ProposalExecutionRunnable runnable = executor.new ProposalExecutionRunnable(mockLoadMonitor, null, null, null, callbackMock);
    runnable.run();
    Mockito.verify(callbackMock).accept(Mockito.eq(false), Mockito.any(NullPointerException.class));
  }

  @Test
  public void testExecutorSendNotificationForSelfHealing() throws InterruptedException {
    Collection<ExecutionProposal> proposals = getBasicProposals();
    String uuid = AnomalyType.GOAL_VIOLATION.toString() + "-uuid";
    executeAndVerifyNotification(proposals, uuid, CRUISE_CONTROL, EXECUTION_COMPLETION, false);
  }

  private void executeAndVerifyNotification(Collection<ExecutionProposal> proposalsToExecute,
                                            String uuid,
                                            ExecutorNotification.ActionAgent startedBy,
                                            ExecutorNotification.ActionAgent endedBy,
                                            boolean expectUserTaskInfo) {

    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());
    ExecutorNotifier mockExecutorNotifier = EasyMock.mock(ExecutorNotifier.class);
    Capture<ExecutorNotification> captureNotification = Capture.newInstance(CaptureType.FIRST);

    mockExecutorNotifier.sendNotification(EasyMock.capture(captureNotification));
    EasyMock.expectLastCall();

    EasyMock.replay(mockExecutorNotifier);

    Executor executor = new Executor(configs, new SystemTime(), KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry),
            null, 86400000L, 43200000L, mockExecutorNotifier, getMockAnomalyDetector(uuid));
    executor.setExecutionMode(false);
    executor.executeProposals(proposalsToExecute, Collections.emptySet(), null, EasyMock.mock(LoadMonitor.class), null,
                              null, null, null, NO_THROTTLE, uuid, null);
    waitUntilExecutionFinishes(executor);

    executor.shutdown();

    ExecutorNotification notification = captureNotification.getValue();
    assertEquals(notification.startedBy(), startedBy);
    assertEquals(notification.endedBy(), endedBy);
    assertEquals(notification.actionUuid(), uuid);
  }

  private Collection<ExecutionProposal> getBasicProposals() throws InterruptedException {
    Map<String, TopicDescription> topicDescriptions = createTopics();
    int initialLeader1 = topicDescriptions.get(TOPIC1).partitions().get(0).leader().id();

    ExecutionProposal proposal0 = getBasicTopicPartition0Proposal();
    ExecutionProposal proposal1 =
        new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(initialLeader1),
                              Arrays.asList(new ReplicaPlacementInfo(initialLeader1),
                                            initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                  new ReplicaPlacementInfo(0)),
                              Arrays.asList(initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                  new ReplicaPlacementInfo(0),
                              new ReplicaPlacementInfo(initialLeader1)), Collections.emptyList(), Collections.emptyList());
    return Arrays.asList(proposal0, proposal1);
  }

  private Collection<ExecutionProposal> getTopicPlacementProposals(KafkaZkClient kafkaZkClient) throws InterruptedException {
    String initialPlacement = "{\"version\":1,\"replicas\":[{\"count\": 1, \"constraints\":{\"rack\":\"0\"}}]," +
            " \"observers\": [{\"count\": 1, \"constraints\":{\"rack\":\"1\"}}]}";
    TopicDescription topicDescription = createTopicWithPlacement(TOPIC1, initialPlacement).get(TOPIC1);
    Properties topicConfig = kafkaZkClient.getEntityConfigs(ConfigType.Topic(), TOPIC1);
    // flip placements so that we can execute a movement
    String finalPlacement = "{\"version\":1,\"replicas\":[{\"count\": 1, \"constraints\":{\"rack\":\"1\"}}]," +
            " \"observers\": [{\"count\": 1, \"constraints\":{\"rack\":\"0\"}}]}";
    topicConfig.put(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, finalPlacement);
    kafkaZkClient.setOrCreateEntityConfigs(ConfigType.Topic(), TOPIC1, topicConfig);
    int initialLeader = topicDescription.partitions().get(TP1.partition()).leader().id();
    List<ReplicaPlacementInfo> oldReplicas = Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1));
    // observers must be suffix of replicas
    List<ReplicaPlacementInfo> newReplicas = Arrays.asList(new ReplicaPlacementInfo(1), new ReplicaPlacementInfo(0));
    List<ReplicaPlacementInfo> oldObservers = Collections.singletonList(new ReplicaPlacementInfo(1));
    List<ReplicaPlacementInfo> newObservers = Collections.singletonList(new ReplicaPlacementInfo(0));
    return Arrays.asList(new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(initialLeader),
            oldReplicas, newReplicas, oldObservers, newObservers));
  }

  private Executor fillUpAndMoveBasicTopicPartition0(long replicationThrottle) throws InterruptedException, ExecutionException {
    return fillUpAndMoveBasicTopicPartition0(replicationThrottle, executor());
  }

  private Executor fillUpAndMoveBasicTopicPartition0(long replicationThrottle, Executor executor) throws InterruptedException, ExecutionException {
    ExecutionProposal proposal = getBasicTopicPartition0Proposal();
    produceData(TP0.topic(), REPLICA_FETCH_MAX_BYTES * 3);

    return executeProposals(executor, Collections.singletonList(proposal), replicationThrottle, null);
  }

  private ExecutionProposal getBasicTopicPartition0Proposal() throws InterruptedException {
    int initialLeader = createTopics().get(TP0.topic()).partitions().get(TP0.partition()).leader().id();
    return new ExecutionProposal(TP0, 0, new ReplicaPlacementInfo(initialLeader),
        Collections.singletonList(new ReplicaPlacementInfo(initialLeader)),
        Collections.singletonList(initialLeader == 0 ? new ReplicaPlacementInfo(1) :
            new ReplicaPlacementInfo(0)), Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testUserTriggeredExecutionDuringSelfHealingPause() throws Exception {
    Map<String, TopicDescription> topicDescriptions = createTopics();
    int initialLeader0 = topicDescriptions.get(TOPIC0).partitions().get(0).leader().id();
    int initialLeader1 = topicDescriptions.get(TOPIC1).partitions().get(0).leader().id();

    ExecutionProposal proposal0 =
            new ExecutionProposal(TP0, 0, new ReplicaPlacementInfo(initialLeader0),
                    Collections.singletonList(new ReplicaPlacementInfo(initialLeader0)),
                    Collections.singletonList(new ReplicaPlacementInfo(initialLeader0 == 0 ? 1 : 0)), Collections.emptyList(), Collections.emptyList());
    ExecutionProposal proposal1 =
            new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(initialLeader1),
                    Arrays.asList(new ReplicaPlacementInfo(initialLeader1), new ReplicaPlacementInfo(initialLeader1 == 0 ? 1 : 0)),
                    Arrays.asList(new ReplicaPlacementInfo(initialLeader1 == 0 ? 1 : 0), new ReplicaPlacementInfo(initialLeader1)), Collections.emptyList(), Collections.emptyList());

    Collection<ExecutionProposal> proposals = Arrays.asList(proposal0, proposal1);

    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());
    Executor executor = new Executor(configs, new SystemTime(), KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry),
            null, 86400000L, 43200000L, null, getMockAnomalyDetector(RANDOM_UUID));
    executor.setExecutionMode(false);
    executor.executeProposals(proposals, Collections.emptySet(), null, EasyMock.mock(LoadMonitor.class), null,
            null, null, null, NO_THROTTLE, RANDOM_UUID, null);
    waitUntilExecutionFinishes(executor);

    // Execute proposals again with userTriggeredExecution set to true to bypass self-healing pause. Should not raise an IllegalStateException
    executor.executeProposals(proposals, Collections.emptySet(), null, EasyMock.mock(LoadMonitor.class), null,
            null, null, null, NO_THROTTLE, RANDOM_UUID, null);

    executor.shutdown();
  }

  @Test
  public void testThrottleIsRemovedUponStartUpWhenNoReassignmentAreRunning() throws Exception {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
            "ExecutorTestMetricGroup",
            "BrokerDiesWhenMovePartitions",
            false);
    Properties props = getExecutorProperties();
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    Map<String, TopicDescription> topicDescriptions = createTopics();
    int oldReplica = topicDescriptions.get(TOPIC0).partitions().get(0).leader().id();
    int newReplica = oldReplica == 0 ? 1 : 0;

    Time time = new MockTime();
    MetadataClient metadataClient = new MetadataClient(config,
            -1L,
            time);

    String rate = "10000000";

    Properties dynamicConfigs = new Properties();
    dynamicConfigs.put(KafkaConfig.LeaderReplicationThrottledRateProp(), rate);
    dynamicConfigs.put(KafkaConfig.FollowerReplicationThrottledRateProp(), rate);

    for (Integer broker: _brokers.keySet()) {
      kafkaZkClient.setOrCreateEntityConfigs(ConfigType$.MODULE$.Broker(), String.valueOf(broker), dynamicConfigs);
    }

    Properties topicDynamicConfigs = new Properties();
    topicDynamicConfigs.put(KafkaConfig$.MODULE$.LeaderReplicationThrottledReplicasProp(), String.format("0:%d", oldReplica));
    topicDynamicConfigs.put(KafkaConfig$.MODULE$.FollowerReplicationThrottledReplicasProp(), String.format("0:%d", newReplica));
    kafkaZkClient.setOrCreateEntityConfigs(ConfigType$.MODULE$.Topic(), TOPIC0, topicDynamicConfigs);

    Thread.sleep(500);
    Executor executor = new Executor(config, time, KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry),
            metadataClient, 86400000L, 43200000L, null, getMockAnomalyDetector(RANDOM_UUID));
    executor.startUp();
    Thread.sleep(100);

    // Verify that throttles were removed as part of startup
    for (Integer broker: _brokers.keySet()) {
      Properties dynamicConfig = kafkaZkClient.getEntityConfigs(ConfigType.Broker(), valueOf(broker));
      assertNull(dynamicConfig.get(KafkaConfig.LeaderReplicationThrottledRateProp()));
      assertNull(dynamicConfig.get(KafkaConfig.FollowerReplicationThrottledRateProp()));
    }
    Properties topicConfig = kafkaZkClient.getEntityConfigs(ConfigType.Topic(), TOPIC0);
    assertNull(topicConfig.get(KafkaConfig.LeaderReplicationThrottledReplicasProp()));
    assertNull(topicConfig.get(KafkaConfig.FollowerReplicationThrottledReplicasProp()));

    executor.shutdown();
    KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
  }

  @Test
  public void testThrottleComputationThrowsException() throws InterruptedException {
    LoadMonitor mockLoadMonitor = EasyMock.mock(LoadMonitor.class);
    String msg = "throttle computation failed";
    mockLoadMonitor.pauseMetricSampling(EasyMock.anyString());
    EasyMock.expectLastCall();
    mockLoadMonitor.resumeMetricSampling(EasyMock.anyString());
    EasyMock.expectLastCall();
    EasyMock.expect(mockLoadMonitor.computeThrottle()).andThrow(new IllegalStateException(msg));
    EasyMock.replay(mockLoadMonitor);

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());

    Map<String, TopicDescription> topicDescriptions = createTopics();
    int oldReplica = topicDescriptions.get(TP1.topic()).partitions().get(0).leader().id();
    int newReplica = oldReplica == 0 ? 1 : 0;

    ExecutionProposal proposal =
            new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(oldReplica),
                    Collections.singletonList(new ReplicaPlacementInfo(oldReplica)),
                    Collections.singletonList(new ReplicaPlacementInfo(newReplica)), Collections.emptyList(), Collections.emptyList());

    Time time = new MockTime();
    MetadataClient metadataClient = new MetadataClient(config,
            -1L,
            time);

    AtomicReference<ExecutorNotification> testNotification = new AtomicReference<>();
    ExecutorNotifier notifier = new ExecutorNotifier() {
      @Override
      public void sendNotification(ExecutorNotification notification) {
        testNotification.set(notification);
      }

      @Override
      public void configure(Map<String, ?> configs) {

      }
    };
    Executor executor = new Executor(config, time, KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry),
            metadataClient, 86400000L, 43200000L, notifier, getMockAnomalyDetector(RANDOM_UUID));
    executor.setExecutionMode(false);
    executor.executeProposals(Collections.singletonList(proposal),
            Collections.emptySet(),
            null,
            mockLoadMonitor,
            null,
            null,
            null,
            null,
            AUTO_THROTTLE,
            RANDOM_UUID,
            null);
    waitUntilExecutionFinishes(executor);

    assertFalse(testNotification.get().executionSucceeded());
    assertTrue(testNotification.get().exception() instanceof RuntimeException);
    assertTrue(testNotification.get().exception().getCause() instanceof IllegalStateException);
    assertTrue(String.format(
        "Did not get expected exception message. Expected to contain [%s], but instead it was [%s]",
        msg, testNotification.get().exception().getMessage()),
        testNotification.get().exception().getMessage().contains(msg));
    EasyMock.verify(mockLoadMonitor);
  }

  private void mockDescribeConfigs(KafkaAdminClient adminClient, List<Integer> brokers) throws ExecutionException, InterruptedException {
    Collection<ConfigResource> configResources = configResourcesForBrokers(brokers);
    Map<String, List<ConfigEntry>> results = new HashMap<>();
    for (Integer brokerId : brokers) {
      results.put(Integer.toString(brokerId), Collections.emptyList());
    }

    KafkaCruiseControlUnitTestUtils.mockDescribeConfigs(adminClient, configResources, results);
  }

  @Test
  public void testThrottleIsComputedOncePerExecution() throws InterruptedException, ExecutionException {
    // Create a strict mock so that we will throw if we attempt to compute the throttle more than once
    LoadMonitor mockLoadMonitor = EasyMock.createStrictMock(LoadMonitor.class);
    mockLoadMonitor.pauseMetricSampling(EasyMock.anyString());
    EasyMock.expectLastCall();
    EasyMock.expect(mockLoadMonitor.computeThrottle()).andReturn(5000000L);
    mockLoadMonitor.resumeMetricSampling(EasyMock.anyString());
    EasyMock.expectLastCall();
    EasyMock.replay(mockLoadMonitor);

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());

    Map<String, TopicDescription> topicDescriptions = createTopics();
    int oldReplica = topicDescriptions.get(TP1.topic()).partitions().get(0).leader().id();
    int newReplica = oldReplica == 0 ? 1 : 0;

    ExecutionProposal proposal =
            new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(oldReplica),
                    Collections.singletonList(new ReplicaPlacementInfo(oldReplica)),
                    Collections.singletonList(new ReplicaPlacementInfo(newReplica)), Collections.emptyList(), Collections.emptyList());

    Time time = new MockTime();
    MetadataClient metadataClient = new MetadataClient(config,
            -1L,
            time);

    AtomicReference<ExecutorNotification> testNotification = new AtomicReference<>();
    ExecutorNotifier notifier = new ExecutorNotifier() {
      @Override
      public void sendNotification(ExecutorNotification notification) {
        testNotification.set(notification);
      }

      @Override
      public void configure(Map<String, ?> configs) {

      }
    };
    KafkaAdminClient adminClient = EasyMock.mock(KafkaAdminClient.class);
    List<Integer> expectedThrottledBrokers = Arrays.asList(0, 1);
    mockDescribeConfigs(adminClient, expectedThrottledBrokers);
    EasyMock.replay(adminClient);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(
            KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(), "CruiseControlExecutor",
                    "Executor", false), adminClient, AUTO_THROTTLE, true);
    Executor executor = new Executor(config, time, KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry),
            metadataClient, 86400000L, 43200000L, notifier, getMockAnomalyDetector(RANDOM_UUID),
            KafkaCruiseControlUtils.createAdmin(config.originals()), throttleHelper);
    executor.setExecutionMode(false);
    executor.executeProposals(Collections.singletonList(proposal),
            Collections.emptySet(),
            null,
            mockLoadMonitor,
            null,
            null,
            null,
            null,
            AUTO_THROTTLE,
            RANDOM_UUID,
            null);
    waitUntilExecutionFinishes(executor);

    // Execution should have succeeded and the throttle helper value should have been reset to AUTO_THROTTLE
    assertTrue(testNotification.get().executionSucceeded());
    assertEquals(AUTO_THROTTLE, throttleHelper.getThrottleRate().longValue());
    EasyMock.verify(mockLoadMonitor);
  }

  private Map<String, TopicDescription> createTopicWithPlacement(String topic, String topicPlacement) throws InterruptedException {
    ConfluentAdmin adminClient = KafkaCruiseControlUtils.createAdmin(Collections.singletonMap(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
    try {
      NewTopic topicWithPlacement = new NewTopic(topic, Optional.of(1), Optional.empty());
      topicWithPlacement.configs(Collections.singletonMap(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, topicPlacement));
      CreateTopicsResult topicsResult = adminClient.createTopics(Arrays.asList(topicWithPlacement));
      // make sure topic creation succeeded
      assertEquals(topicsResult.numPartitions(topic).get(), Integer.valueOf(1));
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee.getMessage());
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }
    return waitForTopicMetadataPropagation(Arrays.asList(topic));
  }

  private Map<String, TopicDescription> waitForTopicMetadataPropagation(List<String> topics) throws InterruptedException {
    // We need to use the admin clients to query the metadata from two different brokers to make sure that
    // both brokers have the latest metadata. Otherwise the Executor may get confused when it does not
    // see expected topics in the metadata.
    Map<String, TopicDescription> topicDescriptions0 = null;
    Map<String, TopicDescription> topicDescriptions1 = null;
    do {
      ConfluentAdmin adminClient0 = KafkaCruiseControlUtils.createAdmin(Collections.singletonMap(
              AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
      ConfluentAdmin adminClient1 = KafkaCruiseControlUtils.createAdmin(Collections.singletonMap(
              AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(1).plaintextAddr()));
      try {
        topicDescriptions0 = adminClient0.describeTopics(topics).all().get();
        topicDescriptions1 = adminClient1.describeTopics(topics).all().get();
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } catch (ExecutionException ee) {
        // Let it go.
      } finally {
        KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient0);
        KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient1);
      }
    } while (topicDescriptions0 == null || topicDescriptions0.size() < topics.size()
            || topicDescriptions1 == null || topicDescriptions1.size() < topics.size());

    topicsCreated = topicDescriptions0;
    return topicsCreated;
  }

  private Map<String, TopicDescription> createTopics() throws InterruptedException {
    if (topicsCreated.size() != 0) {
      return topicsCreated;
    }
    ConfluentAdmin adminClient = KafkaCruiseControlUtils.createAdmin(Collections.singletonMap(
                              AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
    try {
      adminClient.createTopics(Arrays.asList(new NewTopic(TOPIC0, 1, (short) 1),
                                             new NewTopic(TOPIC1, 1, (short) 2)));
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }

    return waitForTopicMetadataPropagation(Arrays.asList(TOPIC0, TOPIC1));
  }

  private void deleteTopics(Collection<String> topics) throws ExecutionException, InterruptedException {
    ConfluentAdmin adminClient = KafkaCruiseControlUtils.createAdmin(Collections.singletonMap(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
    try {
      DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);
      deleteTopicsResult.all().get();
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }
  }

  private AnomalyDetector getMockAnomalyDetector(String anomalyId) {
    AnomalyDetector mockAnomalyDetector = EasyMock.mock(AnomalyDetector.class);
    mockAnomalyDetector.maybeClearOngoingAnomalyDetectionTimeMs();
    EasyMock.expectLastCall().times(1, 2);
    mockAnomalyDetector.markSelfHealingFinished(anomalyId);
    EasyMock.replay(mockAnomalyDetector);
    return mockAnomalyDetector;
  }

  private Executor executeAndVerifyProposals(KafkaZkClient kafkaZkClient,
                                         Collection<ExecutionProposal> proposalsToExecute,
                                         Collection<ExecutionProposal> proposalsToCheck) {
      return executeAndVerifyProposals(kafkaZkClient, proposalsToExecute, proposalsToCheck, executor());
  }

  private Executor executeAndVerifyProposals(KafkaZkClient kafkaZkClient,
                                         Collection<ExecutionProposal> proposalsToExecute,
                                         Collection<ExecutionProposal> proposalsToCheck,
                                         Executor executor) {
    executeProposals(executor, proposalsToExecute);
    waitAndVerifyProposals(kafkaZkClient, executor, proposalsToCheck);
    return executor;
  }

  /**
   * A helper method that wraps #{@link TestUtils#waitForCondition(TestCondition testCondition, long maxWaitMs,
   * String conditionDetails)} with a condition that can throw #{@link AssertionError}
   */
  private void waitForAssert(TestCondition testCondition, long maxWaitMs, String conditionDetails)
      throws InterruptedException {
    TestUtils.waitForCondition(() -> {
      try {
        return testCondition.conditionMet();
      } catch (AssertionError e) {
      }

      return false;
    }, maxWaitMs, conditionDetails);
  }

  private Executor executeProposals(Executor executor, Collection<ExecutionProposal> proposalsToExecute) {
    return executeProposals(executor, proposalsToExecute, NO_THROTTLE);
  }

  private Executor executeProposals(Executor executor, Collection<ExecutionProposal> proposalsToExecute, Long replicationThrottle) {
    return executeProposals(executor, proposalsToExecute, replicationThrottle, null);
  }
  private Executor executeProposals(Executor executor, Collection<ExecutionProposal> proposalsToExecute, Long replicationThrottle,
                                    BalanceOpExecutionCompletionCallback completionCb) {
    executor.setExecutionMode(false);
    executor.executeProposals(proposalsToExecute, Collections.emptySet(), null, EasyMock.mock(LoadMonitor.class), null,
        null, null, null,
        replicationThrottle, RANDOM_UUID, completionCb);

    return executor;
  }

  private Executor executor() {
    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());

    return new Executor(configs, new SystemTime(), KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry),
            null, 86400000L, 43200000L, null, getMockAnomalyDetector(RANDOM_UUID));
  }

  @SuppressWarnings("deprecation")
  private void waitAndVerifyProposals(KafkaZkClient kafkaZkClient,
                                      Executor executor,
                                      Collection<ExecutionProposal> proposalsToCheck) {
    Map<TopicPartition, Integer> replicationFactors = new HashMap<>();
    for (ExecutionProposal proposal : proposalsToCheck) {
      TopicPartition tp = new TopicPartition(proposal.topic(), proposal.partitionId());
      int replicationFactor = kafkaZkClient.getReplicasForPartition(tp).size();
      replicationFactors.put(tp, replicationFactor);
    }

    waitUntilExecutionFinishes(executor);

    for (ExecutionProposal proposal : proposalsToCheck) {
      scala.collection.immutable.Set<String> topics = JavaConverters.asScalaSet(Collections.singleton(proposal.topic())).toSet();
      Map<TopicPartition, ReplicaAssignment> replicaAssignments = JavaConverters.mapAsJavaMap(kafkaZkClient.getFullReplicaAssignmentForTopics(topics));
      TopicPartition tp = new TopicPartition(proposal.topic(), proposal.partitionId());
      ReplicaAssignment replicaAssignment = replicaAssignments.get(tp);

      int expectedReplicationFactor = proposal.newReplicas().size();
      assertEquals("Replication factor for partition " + tp + " should be " + expectedReplicationFactor,
                   expectedReplicationFactor, replicaAssignment.replicas().size());

      if (proposal.hasReplicaAction()) {
        for (ReplicaPlacementInfo r : proposal.newReplicas()) {
          assertTrue("The partition should have moved for " + tp, replicaAssignment.replicas().contains(r.brokerId()));
        }
      }

      for (ReplicaPlacementInfo r : proposal.newObservers()) {
        assertTrue("The observer should have been moved for " + tp, replicaAssignment.observers().contains(r.brokerId()));
      }

      assertEquals("The leader should have moved for " + tp,
                   proposal.newLeader().brokerId(), kafkaZkClient.getLeaderForPartition(tp).get());
    }
  }

  private Executor createExecutor() {
    return createExecutor(getMockAnomalyDetector(RANDOM_UUID));
  }

  private Executor createExecutor(AnomalyDetector mockAnomalyDetector) {
    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());
    Executor executor = new Executor(configs, new SystemTime(), KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry),
        null, 86400000L, 43200000L, null, mockAnomalyDetector);
    executor.setExecutionMode(false);

    return executor;
  }

  private void waitUntilExecutionFinishes(Executor executor) {
    long now = System.currentTimeMillis();
    while ((executor.hasOngoingExecution() || executor.state().state() != ExecutorState.State.NO_TASK_IN_PROGRESS)
        && System.currentTimeMillis() < now + 30000) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    if (executor.state().state() != ExecutorState.State.NO_TASK_IN_PROGRESS) {
      fail("The execution did not finish in 30 seconds.");
    }
  }

  private Properties getExecutorProperties() {
    Properties props = new Properties();
    String capacityConfigFile = this.getClass().getClassLoader().getResource("DefaultCapacityConfig.json").getFile();
    props.setProperty(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, capacityConfigFile);
    props.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.setProperty(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper().connectionString());
    props.setProperty(KafkaCruiseControlConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG, "10");
    props.setProperty(KafkaCruiseControlConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "1000");
    props.setProperty(KafkaCruiseControlConfig.DESCRIBE_TOPICS_RESPONSE_TIMEOUT_MS_CONFIG, DESCRIBE_TOPICS_RESPONSE_TIMEOUT_MS);
    return props;
  }
}
