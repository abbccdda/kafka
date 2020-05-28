// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.provider.ldap.LdapConfig;
import io.confluent.security.auth.provider.ldap.LdapGroupManager;
import io.confluent.security.auth.provider.ldap.LdapStore;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.AuthKey;
import io.confluent.security.auth.store.data.AuthValue;
import io.confluent.security.auth.store.data.StatusKey;
import io.confluent.security.auth.store.data.StatusValue;
import io.confluent.security.auth.store.data.UserKey;
import io.confluent.security.auth.store.data.UserValue;
import io.confluent.security.auth.store.external.ExternalStoreListener;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.utils.JsonMapper;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.store.MetadataStoreStatus;
import io.confluent.security.store.kafka.KafkaStoreConfig;
import io.confluent.security.store.kafka.clients.StatusListener;
import io.confluent.security.store.kafka.clients.Writer;
import io.confluent.security.store.kafka.coordinator.MetadataNodeManager;
import io.confluent.security.store.kafka.coordinator.MetadataServiceAssignment;
import io.confluent.security.store.kafka.coordinator.MetadataServiceAssignment.AssignmentError;
import io.confluent.security.store.kafka.coordinator.MetadataServiceCoordinator;
import io.confluent.security.store.kafka.coordinator.NodeMetadata;
import io.confluent.security.test.utils.RbacTestUtils;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.naming.Context;
import javax.naming.NamingException;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;

public class MockAuthStore extends KafkaAuthStore {

  static final String MOCK_LDAP_URL = "mock:389";

  private final int numAuthTopicPartitions;
  final Map<Integer, NodeMetadata> nodes;
  final Cluster cluster;
  private final Map<Integer, Long> consumedOffsets;
  private final ScheduledExecutorService executor;
  private final int nodeId;
  private final MetadataResponse metadataResponse;
  private final AtomicInteger coordinatorGeneration = new AtomicInteger();
  final AtomicInteger assignCount = new AtomicInteger();
  final AtomicInteger revokeCount = new AtomicInteger();
  volatile MockLdapStore ldapStore = new MockLdapStore(Collections::emptyMap);
  volatile MockProducer<AuthKey, AuthValue> producer;
  volatile MockConsumer<AuthKey, AuthValue> consumer;
  private volatile MockNodeManager nodeManager;
  private volatile long produceDelayMs;
  private volatile long consumeDelayMs;
  private volatile MockClient coordinatorClient;
  volatile Header header;

  public MockAuthStore(RbacRoles roles,
                       Time time,
                       Scope scope,
                       int numAuthTopicPartitions,
                       int nodeId) {
    super(roles, time, scope, KafkaTestUtils.serverInfo("clusterA"), numAuthTopicPartitions);
    this.nodeId = nodeId;
    this.numAuthTopicPartitions = numAuthTopicPartitions;

    cluster = TestUtils.clusterWith(5, KafkaAuthStore.AUTH_TOPIC, numAuthTopicPartitions);
    metadataResponse = TestUtils.metadataUpdateWith(5,
        Collections.singletonMap(KafkaAuthStore.AUTH_TOPIC, numAuthTopicPartitions));
    nodes = cluster.nodes().stream()
        .collect(Collectors.toMap(Node::id, node -> nodeMetadata(node.id())));

    this.consumedOffsets = new HashMap<>(numAuthTopicPartitions);
    for (int i = 0; i < numAuthTopicPartitions; i++)
      this.consumedOffsets.put(i, -1L);
    this.executor = Executors.newSingleThreadScheduledExecutor();
  }

  public void configureDelays(long produceDelayMs, long consumeDelayMs) {
    this.produceDelayMs = produceDelayMs;
    this.consumeDelayMs = consumeDelayMs;
  }

  @Override
  public void startService(Collection<URL> nodeUrls) {
    super.startService(nodeUrls);

    try {
      TestUtils.waitForCondition(() -> coordinatorClient != null, "Coordinator client not created");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Consumer<AuthKey, AuthValue> createConsumer(Map<String, Object> configs) {
    consumer = RbacTestUtils.mockConsumer(cluster, numAuthTopicPartitions);
    return consumer;
  }

  @Override
  protected Producer<AuthKey, AuthValue> createProducer(Map<String, Object> configs) {
    producer = new MockProducer<AuthKey, AuthValue>(cluster, false, null, null, null) {
      @Override
      public synchronized Future<RecordMetadata> send(ProducerRecord<AuthKey, AuthValue> record, Callback callback) {
        if (header != null)
          record.headers().add(header);
        Future<RecordMetadata> future = super.send(record, callback);
        onSend(record, executor);
        return future;
      }
    };
    return producer;
  }

  @Override
  protected AdminClient createAdminClient(Map<String, Object> configs) {
    MockAdminClient adminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0));
    List<TopicPartitionInfo> topicPartitionInfos = new ArrayList<>(numAuthTopicPartitions);
    for (int i = 0; i < numAuthTopicPartitions; i++)
      topicPartitionInfos.add(new TopicPartitionInfo(i, cluster.nodeById(0), cluster.nodes(), Collections.<Node>emptyList()));
    adminClient.addTopic(true, AUTH_TOPIC, topicPartitionInfos, null);
    return adminClient;
  }

  @Override
  protected MetadataNodeManager createNodeManager(Collection<URL> nodeUrls,
                                                  KafkaStoreConfig config,
                                                  KafkaAuthWriter writer,
                                                  Time time) {
    nodeManager = new MockNodeManager(nodeUrls, config, writer, time);
    return nodeManager;
  }

  @Override
  protected KafkaAuthWriter createWriter(int numPartitions,
                                          KafkaStoreConfig clientConfig,
                                          DefaultAuthCache authCache,
                                          StatusListener statusListener,
                                          Time time) {
    return new KafkaAuthWriter(
        AUTH_TOPIC,
        numPartitions,
        clientConfig,
        createProducer(clientConfig.producerConfigs(AUTH_TOPIC)),
        () -> createAdminClient(clientConfig.adminClientConfigs()),
        authCache,
        statusListener,
        time) {

      @Override
      protected LdapStore createLdapStore(Map<String, ?> configs, DefaultAuthCache authCache) {
        return new LdapStore(authCache, this, time) {
          @Override
          protected LdapGroupManager createLdapGroupManager(ExternalStoreListener<UserKey, UserValue> listener) {
            return ldapStore.createLdapGroupManager(configs, time, listener);
          }
        };
      }
    };
  }

  @Override
  public void close() {
    super.close();
    executor.shutdownNow();
    try {
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Collection<URL> urls() {
    return nodes.get(nodeId).urls();
  }

  public URL url(String protocol) {
    return nodes.get(nodeId).url(protocol);
  }

  public int prepareMasterWriter(int nodeId) {
    int oldGeneration = coordinatorGeneration.get();
    coordinatorClient.prepareResponse(joinGroupResponse(coordinatorGeneration.incrementAndGet()));
    coordinatorClient.prepareResponse(syncGroupResponse(nodeId));
    return oldGeneration;
  }

  public void makeMasterWriter(int nodeId) {
    int oldGeneration = prepareMasterWriter(nodeId);
    int expectedAssignCount = assignCount.get() + 1;
    int expectedRevokeCount = revokeCount.get() + 1;

    try {
      if (nodeId != this.nodeId)
        nodeManager.onWriterResigned(oldGeneration);
      else
        nodeManager.onWriterResigned();
      TestUtils.waitForCondition(() -> revokeCount.get() == expectedRevokeCount, "Writer not revoked");

      if (nodeId != -1) {
        TestUtils.waitForCondition(() -> this.masterWriterUrl("http") != null, "Writer not elected");
        assertEquals(expectedAssignCount, assignCount.get());
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void addNewGenerationStatusRecord(int generationId) {
    for (int i = 0; i < numAuthTopicPartitions; i++) {
      ConsumerRecord<AuthKey, AuthValue> status = new ConsumerRecord<>(KafkaAuthStore.AUTH_TOPIC, i,
          producer.history().size(),
          new StatusKey(i),
          new StatusValue(MetadataStoreStatus.INITIALIZING, generationId,
              clientConfig().brokerId, null));
      consumer.addRecord(status);
    }
  }

  public ConsumerRecord<AuthKey, AuthValue> consumerRecord(ProducerRecord<AuthKey, AuthValue> record) {
    long offset = consumedOffsets.get(record.partition()) + 1;
    consumedOffsets.put(record.partition(), offset);
    return new ConsumerRecord<>(KafkaAuthStore.AUTH_TOPIC, record.partition(),
        offset, record.key(), record.value());
  }

  protected void onSend(ProducerRecord<AuthKey, AuthValue> record, ScheduledExecutorService executor) {
    executor.schedule(() -> producer.completeNext(), produceDelayMs, TimeUnit.MILLISECONDS);
    executor.schedule(() -> consumer.addRecord(consumerRecord(record)),
        consumeDelayMs, TimeUnit.MILLISECONDS);
  }

  private KafkaClient createCoordinatorClient(Time time, Metadata metadata) {
    coordinatorClient = new MockClient(time, metadata);
    coordinatorClient.prepareMetadataUpdate(metadataResponse);
    coordinatorClient.prepareResponse(new FindCoordinatorResponse(new FindCoordinatorResponseData().setErrorCode(Errors.NONE.code()).setNodeId(nodeId)));
    coordinatorClient.prepareResponse(joinGroupResponse(coordinatorGeneration.incrementAndGet()));
    coordinatorClient.prepareResponse(syncGroupResponse(nodeId));
    return coordinatorClient;
  }

  private String memberId(int nodeId) {
    return "member" + nodeId;
  }

  private NodeMetadata nodeMetadata(int nodeId) {
    try {
      return new NodeMetadata(Arrays.asList(new URL("http://server" + nodeId + ":8089"),
          new URL("https://server" + nodeId + ":8090")));
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  private JoinGroupResponse joinGroupResponse(int generationId) {
    List<JoinGroupResponseData.JoinGroupResponseMember> members = new ArrayList<>();
    for (Map.Entry<Integer, NodeMetadata> entry : nodes.entrySet()) {
      members.add(new JoinGroupResponseData.JoinGroupResponseMember()
              .setMemberId(entry.getKey().toString())
              .setMetadata(entry.getValue().serialize().array()));
    }

    return new JoinGroupResponse(new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(generationId)
            .setProtocolName(MetadataServiceCoordinator.PROTOCOL)
            .setMemberId("0")
            .setLeader("0")
            .setMembers(members));
  }

  private SyncGroupResponse syncGroupResponse(int writeNodeId) {
    Map<String, NodeMetadata> nodesByMemberId = nodes.entrySet().stream()
        .collect(Collectors.toMap(e -> memberId(e.getKey()), Map.Entry::getValue));
    String writerMemberId = writeNodeId == -1 ? null : memberId(writeNodeId);
    MetadataServiceAssignment assignment = new MetadataServiceAssignment(
        AssignmentError.NONE.errorCode(),
        nodesByMemberId,
        writerMemberId,
        writeNodeId == -1 ? null : nodesByMemberId.get(writerMemberId));
    return new SyncGroupResponse(new SyncGroupResponseData()
        .setErrorCode(Errors.NONE.code())
        .setAssignment(JsonMapper.toByteArray(assignment)));
  }

  public static MockAuthStore create(RbacRoles rbacRoles,
                                     Time time,
                                     Scope scope,
                                     int numAuthTopicPartitions,
                                     int nodeId) {
    MockAuthStore store = new MockAuthStore(rbacRoles, time, scope, numAuthTopicPartitions, nodeId);
    Map<String, Object> configs = new HashMap<>();
    configs.put("confluent.metadata.bootstrap.servers", "localhost:9092,localhost:9093");
    store.configure(configs);
    store.startReader();
    return store;
  }

  static class MockLdapStore {
    volatile Supplier<Map<String, Set<String>>> ldapGroups;
    MockLdapStore(Supplier<Map<String, Set<String>>> ldapGroups) {
      this.ldapGroups = ldapGroups;
    }

    LdapGroupManager createLdapGroupManager(Map<String, ?> configs,
                                            Time time,
                                            ExternalStoreListener<UserKey, UserValue> listener) {
      return new LdapGroupManager(new LdapConfig(configs), time, listener) {
        @Override
        protected void searchAndProcessResults() throws NamingException, IOException {
          if (configs.get("ldap." + Context.PROVIDER_URL).equals(MOCK_LDAP_URL)) {
            ldapGroups.get().forEach((k, v) -> {
              KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, k);
              Set<KafkaPrincipal> groups = v.stream()
                  .map(name -> new KafkaPrincipal("Group", name))
                  .collect(Collectors.toSet());
              listener.update(new UserKey(user), new UserValue(groups));
            });
          } else
            super.searchAndProcessResults();
        }
      };
    }
  }

  private class MockNodeManager extends MetadataNodeManager {
    MockNodeManager(Collection<URL> nodeUrls,
        KafkaStoreConfig config,
        Writer metadataWriter,
        Time time) {
      super(nodeUrls, config, metadataWriter, time);
    }

    @Override
    protected KafkaClient createKafkaClient(ConsumerConfig coordinatorConfig,
        Metadata metadata,
        Time time,
        LogContext logContext) {
      return createCoordinatorClient(time, metadata);
    }

    @Override
    public synchronized void onAssigned(MetadataServiceAssignment assignment, int generationId) {
      assignCount.incrementAndGet();
      super.onAssigned(assignment, generationId);
    }

    @Override
    public synchronized void onRevoked(int generationId) {
      super.onRevoked(generationId);
      revokeCount.incrementAndGet();
    }

    @Override
    public void close(Duration timeout) {
      // To avoid processing pending requests, just close without waiting
      super.close(Duration.ZERO);
    }

    public void onWriterResigned() {
      super.onWriterResigned();
    }
  }
}
