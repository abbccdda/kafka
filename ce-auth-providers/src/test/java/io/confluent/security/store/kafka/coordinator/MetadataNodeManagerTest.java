// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.coordinator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.store.kafka.KafkaStoreConfig;
import io.confluent.security.store.kafka.clients.Writer;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetadataNodeManagerTest {

  private final Time time = new MockTime();
  private Set<URL> activeUrls;
  private Map<String, NodeMetadata> activeNodes;
  private MockNodeManager nodeManager;

  @Before
  public void setUp() throws Exception {
    activeUrls = new HashSet<>(5);
    activeNodes = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      URL url = new URL("http://host" + i + ":8000");
      activeNodes.put(String.valueOf(i), new NodeMetadata(Collections.singleton(url)));
      activeUrls.add(url);
    }
    nodeManager = createNodeManager("http://host1:8000");
  }

  @After
  public void tearDown() {
    if (nodeManager != null)
      nodeManager.close(Duration.ofSeconds(10));
    KafkaTestUtils.verifyThreadCleanup();
  }

  @Test
  public void testAssignRevoke() throws Exception {
    assertEquals(Collections.emptySet(), nodeManager.activeNodeUrls("http"));

    String writerId = String.valueOf(1);
    URL writerUrl = activeNodes.get(writerId).url("http");
    MetadataServiceAssignment assignment = assignment((short) 0, writerId);
    assertEquals(writerId, assignment.writerMemberId());
    assertEquals(MetadataServiceAssignment.LATEST_VERSION, assignment.version());
    nodeManager.onAssigned(assignment, 1);
    waitForMasterWriter(writerUrl);
    assertTrue(nodeManager.metadataWriter.active.get());
    assertEquals(1, nodeManager.metadataWriter.generationId);

    nodeManager.onRevoked(1);
    waitForNoMasterWriter();
    assertFalse(nodeManager.metadataWriter.active.get());

    writerId = String.valueOf(2);
    writerUrl = activeNodes.get(writerId).url("http");
    nodeManager.onAssigned(assignment((short) 0, writerId), 2);
    waitForMasterWriter(writerUrl);
    assertFalse(nodeManager.metadataWriter.active.get());
  }

  @Test
  public void testActiveNodes() throws Exception {
    String writerId = String.valueOf(1);
    URL writerUrl = activeNodes.get(writerId).url("http");
    MetadataServiceAssignment assignment = assignment((short) 0, writerId);
    nodeManager.onAssigned(assignment, 1);
    waitForMasterWriter(writerUrl);
    assertEquals(activeUrls, nodeManager.activeNodeUrls("http"));

    nodeManager.onRevoked(1);
    assertEquals(activeUrls, nodeManager.activeNodeUrls("http"));

    URL newUrl = new URL("http://newhost:8000");
    activeNodes.put("newmember", new NodeMetadata(Collections.singleton(newUrl)));
    activeUrls.add(newUrl);
    writerId = String.valueOf(1);
    writerUrl = activeNodes.get(writerId).url("http");
    nodeManager.onAssigned(assignment((short) 0, writerId), 2);
    waitForMasterWriter(writerUrl);
    assertEquals(activeUrls, nodeManager.activeNodeUrls("http"));

    nodeManager.activeNodeUrls("somestrangeprotocol").forEach(url ->
        assertNotNull("No active node URL should not be null, even for unrecognized protocols", url)
    );
  }

  @Test
  public void testAssignWithoutRevoke() throws Exception {
    String writerId = String.valueOf(1);
    URL writerUrl = activeNodes.get(writerId).url("http");
    nodeManager.onAssigned(assignment((short) 0, writerId), 3);
    waitForMasterWriter(writerUrl);
    assertTrue(nodeManager.metadataWriter.active.get());

    writerId = String.valueOf(3);
    writerUrl = activeNodes.get(writerId).url("http");
    nodeManager.onAssigned(assignment((short) 0, writerId), 4);
    waitForMasterWriter(writerUrl);
    assertFalse(nodeManager.metadataWriter.active.get());

    writerId = String.valueOf(1);
    writerUrl = activeNodes.get(writerId).url("http");
    nodeManager.onAssigned(assignment((short) 0, writerId), 5);
    waitForMasterWriter(writerUrl);
    assertTrue(nodeManager.metadataWriter.active.get());
    assertEquals(activeUrls, nodeManager.activeNodeUrls("http"));
  }

  @Test
  public void testAssignmentError() throws Exception {
    String writerId = String.valueOf(1);
    URL writerUrl = activeNodes.get(writerId).url("http");
    nodeManager.onAssigned(assignment((short) 0, writerId), 3);
    waitForMasterWriter(writerUrl);
    assertEquals(activeUrls, nodeManager.activeNodeUrls("http"));
    assertTrue(nodeManager.metadataWriter.active.get());

    writerId = String.valueOf(3);
    nodeManager.onAssigned(assignment((short) 1, writerId), 4);
    waitForNoMasterWriter();
    assertEquals(activeUrls, nodeManager.activeNodeUrls("http"));
    assertFalse(nodeManager.metadataWriter.active.get());
  }

  @Test
  public void testWriterResign() throws Exception {
    String writerId = String.valueOf(1);
    URL writerUrl = activeNodes.get(writerId).url("http");
    nodeManager.onAssigned(assignment((short) 0, writerId), 3);
    waitForMasterWriter(writerUrl);
    assertTrue(nodeManager.metadataWriter.active.get());
    assertEquals(3, nodeManager.metadataWriter.generationId);

    // Test older generation resign is ignored
    nodeManager.onWriterResigned(2);
    waitForMasterWriter(writerUrl);
    assertTrue(nodeManager.metadataWriter.active.get());
    assertEquals(3, nodeManager.metadataWriter.generationId);

    // Test current generation resign is processed
    nodeManager.onWriterResigned(3);
    waitForNoMasterWriter();
    assertEquals(activeUrls, nodeManager.activeNodeUrls("http"));
    assertFalse(nodeManager.metadataWriter.active.get());
    assertEquals(-1, nodeManager.metadataWriter.generationId);
  }

  private MockNodeManager createNodeManager(String url) throws Exception {
    Properties props = new Properties();
    props.put(KafkaStoreConfig.BOOTSTRAP_SERVERS_PROP, "localhost:9092");
    KafkaStoreConfig config = new KafkaStoreConfig(props);
    MockWriter writer = new MockWriter();
    MockNodeManager nodeManager = new MockNodeManager(new URL(url), config, writer, time);
    nodeManager.start();
    return nodeManager;
  }

  private MetadataServiceAssignment assignment(short error, String writerId) {
    return new MetadataServiceAssignment(error, activeNodes, writerId, activeNodes.get(writerId));
  }

  private void waitForMasterWriter(URL writerUrl) throws Exception {
    TestUtils.waitForCondition(() -> writerUrl.equals(nodeManager.masterWriterUrl("http")),
        "Master writer not updated");
  }

  private void waitForNoMasterWriter() throws Exception {
    TestUtils.waitForCondition(() -> nodeManager.masterWriterUrl("http") == null,
        "Master writer not removed");
  }

  private static class MockNodeManager extends MetadataNodeManager {

    final MockWriter metadataWriter;

    MockNodeManager(URL nodeUrl,
        KafkaStoreConfig config,
        Writer metadataWriter,
        Time time) {
      super(Collections.singleton(nodeUrl), config, metadataWriter, time);
      this.metadataWriter = (MockWriter) metadataWriter;
    }

    @Override
    protected KafkaClient createKafkaClient(ConsumerConfig coordinatorConfig,
        Metadata metadata,
        Time time,
        LogContext logContext) {
      MockClient client = new MockClient(time, metadata);
      client.updateMetadata(TestUtils.metadataUpdateWith(1, Collections.singletonMap("_confluent-security-auth", 2)));
      return client;
    }
  }

  private static class MockWriter implements Writer {

    AtomicBoolean active = new AtomicBoolean();
    int generationId = -1;

    @Override
    public void startWriter(int generationId) {
      this.generationId = generationId;
      this.active.set(true);
    }

    @Override
    public void stopWriter(Integer generationId) {
      if (generationId == null || this.generationId == generationId) {
        this.generationId = -1;
        this.active.set(false);
      }
    }

    @Override
    public boolean ready() {
      return active.get();
    }
  }
}
