// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.coordinator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.authorizer.utils.JsonMapper;
import io.confluent.security.store.kafka.KafkaStoreConfig;
import io.confluent.security.store.kafka.coordinator.MetadataServiceAssignment.AssignmentError;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetadataServiceCoordinatorTest {

  private final Time time = new MockTime();
  private final Metrics metrics = new Metrics();
  private ExecutorService executor;
  private Cluster cluster;
  private String coordinatorId;
  private Map<String, NodeMetadata> activeNodes;
  private MockClient mockClient;
  private ConsumerNetworkClient coordinatorClient;
  private MetadataServiceCoordinator coordinator;
  private RebalanceListener rebalanceListener;

  @Before
  public void setUp() throws Exception {

    cluster = TestUtils.clusterWith(5, Collections.emptyMap());
    activeNodes = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      URL url = new URL("http://host" + i + ":8000");
      activeNodes.put(String.valueOf(i), new NodeMetadata(Collections.singleton(url)));
    }
    coordinatorId = "2";

    LogContext logContext = new LogContext("test");
    Metadata metadata = new Metadata(10, 60 * 60 * 1000L, new LogContext(), new ClusterResourceListeners());
    mockClient = new MockClient(time, metadata);
    mockClient.updateMetadata(TestUtils.metadataUpdateWith(5, Collections.emptyMap()));
    coordinatorClient = new ConsumerNetworkClient(logContext,
        mockClient,
        metadata,
        time,
        10,
        30000,
        3000);
    rebalanceListener = new RebalanceListener();
    Properties props = new Properties();
    props.put(KafkaStoreConfig.BOOTSTRAP_SERVERS_PROP, "localhost:9092");
    KafkaStoreConfig config = new KafkaStoreConfig(props);
    coordinator = new MetadataServiceCoordinator(logContext,
        coordinatorClient,
        activeNodes.get(coordinatorId),
        new ConsumerConfig(config.coordinatorConfigs()),
        metrics,
        "test",
        time,
        rebalanceListener);
  }

  @After
  public void tearDown() throws Exception {
    if (executor != null)
      executor.shutdownNow();
    if (coordinator != null)
      coordinator.close();
    if (coordinatorClient != null)
      coordinatorClient.close();
    if (executor != null)
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    metrics.close();

    KafkaTestUtils.verifyThreadCleanup();
  }

  @Test
  public void testWriterElection() throws Exception {
    assertNull(rebalanceListener.currentAssignment);
    rebalanceListener.verifyWriter(null, -1);

    verifyWriterElection("0", "3");
  }

  @Test
  public void testWriterReelection() throws Exception {
    verifyWriterElection("0", "1");

    for (int generation = 2; generation < 4; generation++) {
      coordinator.onWriterResigned();
      addJoinGroupResponse("0", generation);
      String writerId = String.valueOf(generation);
      addSyncGroupResponse(writerId, AssignmentError.NONE);
      coordinator.ensureActiveGroup();
      rebalanceListener.verifyWriter(writerId, generation);
    }
  }

  @Test
  public void testGroupLeader() throws Exception {
    verifyWriterElection(coordinatorId, "1");
  }

  @Test
  public void testGroupFollower() throws Exception {
    verifyWriterElection("1", "2");
  }

  @Test
  public void testMultipleListeners() throws Exception {
    activeNodes.clear();
    for (int i = 0; i < 5; i++) {
      URL httpUrl = new URL("http://host" + i + ":8091");
      URL httpsUrl = new URL("https://host" + i + ":8090");
      activeNodes.put(String.valueOf(i), new NodeMetadata(Arrays.asList(httpsUrl, httpUrl)));
    }
    verifyWriterElection("1", "3");
    assertEquals(new URL("http://host3:8091"),
        rebalanceListener.currentAssignment.writerNodeMetadata().url("http"));
    assertEquals(new URL("https://host3:8090"),
        rebalanceListener.currentAssignment.writerNodeMetadata().url("https"));
    for (NodeMetadata node : rebalanceListener.currentAssignment.nodes().values()) {
      assertEquals(2, node.urls().size());
      assertEquals(8091, node.url("http").getPort());
      assertEquals(8090, node.url("https").getPort());
    }
  }

  @Test
  public void testDuplicateUrlsAssignment() throws Exception {
    activeNodes.put("10", activeNodes.get("1"));
    addFindCoordinatorResponse(2, Errors.NONE);
    addJoinGroupResponse(coordinatorId, 1);
    executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> coordinator.ensureActiveGroup());

    TestUtils.waitForCondition(() -> {
      ClientRequest request = mockClient.requests().peek();
      return request != null && request.apiKey() == ApiKeys.SYNC_GROUP;
    }, "SyncGroup request not received");

    SyncGroupRequest syncRequest = (SyncGroupRequest) mockClient.requests().peek().requestBuilder().build();
    MetadataServiceAssignment assignment = JsonMapper.fromByteBuffer(
        syncRequest.groupAssignments().get(coordinatorId), MetadataServiceAssignment.class);
    assertEquals(AssignmentError.DUPLICATE_URLS.errorCode(), assignment.error());
  }

  @Test
  public void testDuplicateUrlsResponse() throws Exception {
    activeNodes.put("10", activeNodes.get("1"));
    addFindCoordinatorResponse(2, Errors.NONE);
    addJoinGroupResponse(coordinatorId, 1);
    addSyncGroupResponse("3", AssignmentError.DUPLICATE_URLS);
    coordinator.ensureActiveGroup();
    assertNotNull(rebalanceListener.currentAssignment);
    assertEquals(AssignmentError.DUPLICATE_URLS.errorCode(),
        rebalanceListener.currentAssignment.error());
  }

  private void verifyWriterElection(String groupLeaderId, String writerId) {
    addFindCoordinatorResponse(2, Errors.NONE);
    addJoinGroupResponse(groupLeaderId, 1);
    addSyncGroupResponse(writerId, AssignmentError.NONE);
    coordinator.ensureActiveGroup();
    rebalanceListener.verifyWriter(writerId, 1);
  }

  private void addFindCoordinatorResponse(int leaderId, Errors error) {
    mockClient.prepareResponse(new FindCoordinatorResponse(new FindCoordinatorResponseData()
            .setErrorCode(Errors.NONE.code())
            .setNodeId(leaderId)));
  }

  private void addJoinGroupResponse(String leaderId, int generationId) {
    List<JoinGroupResponseData.JoinGroupResponseMember> members = new ArrayList<>();
    for (Map.Entry<String, NodeMetadata> entry : activeNodes.entrySet()) {
      members.add(new JoinGroupResponseData.JoinGroupResponseMember()
              .setMemberId(entry.getKey())
              .setMetadata(entry.getValue().serialize().array()));
    }

    mockClient.prepareResponse(
            new JoinGroupResponse(new JoinGroupResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGenerationId(generationId)
                    .setProtocolName(MetadataServiceCoordinator.PROTOCOL)
                    .setMemberId(coordinatorId)
                    .setLeader(leaderId)
                    .setMembers(members)));
  }

  private void addSyncGroupResponse(String writerId, AssignmentError error) {
    MetadataServiceAssignment assignment = new MetadataServiceAssignment(
        error.errorCode(),
        activeNodes,
        writerId,
        activeNodes.get(writerId));
    mockClient.prepareResponse(new SyncGroupResponse(new SyncGroupResponseData()
        .setErrorCode(Errors.NONE.code())
        .setAssignment(JsonMapper.toByteArray(assignment))));
  }

  private class RebalanceListener implements MetadataServiceRebalanceListener {

    private final AtomicInteger assignCount = new AtomicInteger();
    private final AtomicInteger revokeCount = new AtomicInteger();
    private final AtomicReference<String> error = new AtomicReference<>();
    private volatile MetadataServiceAssignment currentAssignment;
    private volatile int currentGenerationId = -1;

    @Override
    public void onAssigned(MetadataServiceAssignment assignment, int generationId) {
      assignCount.incrementAndGet();
      if (currentAssignment != null) {
        if (!assignment.equals(currentAssignment))
          error.compareAndSet(null, "onAssigned invoked without revoking previous assignment");
        else if (generationId == currentGenerationId)
          error.compareAndSet(null, "onAssigned invoked multiple times for same assignment");
        else
          error.compareAndSet(null, "onAssigned generation change without revoking previous assignment");
      }
      this.currentAssignment = assignment;
      this.currentGenerationId = generationId;
    }

    @Override
    public void onRevoked(int generationId) {
      revokeCount.incrementAndGet();
      if (currentGenerationId != generationId)
        error.compareAndSet(null, "onRevoked invoked with unexpeced generationId " + generationId);
      currentAssignment = null;
      currentGenerationId = -1;
    }

    @Override
    public void onWriterResigned(int generationId) {
      if (generationId == currentGenerationId) {
        currentAssignment = null;
        currentGenerationId = -1;
      }
    }

    String writerId() {
      MetadataServiceAssignment assignment = currentAssignment;
      return assignment == null ? null : assignment.writerMemberId();
    }

    void verifyWriter(String writerId, int generation) {
      assertEquals(writerId, writerId());
      assertEquals(generation, currentGenerationId);
      assertNull(error.get());
      assertEquals(Math.max(0, generation), assignCount.get());
      assertEquals(Math.max(0, generation), revokeCount.get());
      if (writerId != null)
        assertEquals(activeNodes, currentAssignment.nodes());
    }
  }
}
