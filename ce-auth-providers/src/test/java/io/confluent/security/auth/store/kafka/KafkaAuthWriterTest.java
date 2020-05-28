// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.provider.ldap.LdapConfig;
import io.confluent.security.auth.provider.ldap.LdapGroupManager;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.AuthEntryType;
import io.confluent.security.auth.store.data.AuthKey;
import io.confluent.security.auth.store.data.AuthValue;
import io.confluent.security.auth.store.data.RoleBindingKey;
import io.confluent.security.auth.store.data.RoleBindingValue;
import io.confluent.security.auth.store.data.UserKey;
import io.confluent.security.auth.store.data.UserValue;
import io.confluent.security.auth.store.external.ExternalStoreListener;
import io.confluent.security.auth.store.kafka.MockAuthStore.MockLdapStore;
import io.confluent.security.authorizer.acl.AclRule;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.PermissionType;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourcePatternFilter;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.rbac.InvalidRoleBindingException;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.store.MetadataStoreStatus;
import io.confluent.security.store.NotMasterWriterException;
import io.confluent.security.store.kafka.clients.JsonSerde;
import io.confluent.security.test.utils.RbacTestUtils;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.naming.Context;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaAuthWriterTest {

  private final Time time = new MockTime();
  private final int numPartitions = 2;
  private final KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
  private final KafkaPrincipal bob = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Bob");
  private final Scope rootScope = Scope.intermediateScope("testOrg");
  private final Scope clusterA = new Scope.Builder("testOrg").withKafkaCluster("clusterA").build();
  private final Scope clusterB = new Scope.Builder("testOrg").withKafkaCluster("clusterB").build();
  private final Scope anotherClusterA = new Scope.Builder("anotherOrg").withKafkaCluster("clusterA").build();
  private final Scope invalidScope = new Scope(Collections.emptyList(), Collections.singletonMap("", "invalid"));
  private final int storeNodeId = 1;

  private RbacRoles rbacRoles;
  private MockAuthStore authStore;
  private KafkaAuthWriter authWriter;
  private DefaultAuthCache authCache;

  @Before
  public void setUp() throws Exception {
    rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
    authStore = MockAuthStore.create(rbacRoles, time, rootScope, numPartitions, storeNodeId);
    authStore.startService(authStore.urls());
    assertNotNull(authStore.writer());
    authWriter = authStore.writer();
    authCache = authStore.authCache();
    TestUtils.waitForCondition(() -> authStore.masterWriterUrl("http") != null, "Writer not elected");
    TestUtils.waitForCondition(() -> authWriter.ready(), "Writer not ready");
  }

  @After
  public void tearDown() {
    if (authStore != null)
      authStore.close();
    KafkaTestUtils.verifyThreadCleanup();
  }

  @Test
  public void testWriterElection() throws Exception {
    assertEquals(new URL("http://server1:8089"), authStore.masterWriterUrl("http"));
    assertEquals(new URL("https://server1:8090"), authStore.masterWriterUrl("https"));

    int newWriter = storeNodeId + 1;
    authStore.makeMasterWriter(newWriter);
    TestUtils.waitForCondition(() -> !authStore.url("http").equals(authStore.masterWriterUrl("http")),
        "Rebalance not completed");
    TestUtils.waitForCondition(() -> !authStore.writer().ready(), "Writer ready flag not reset after rebalance");
    assertEquals(new URL("http://server2:8089"), authStore.masterWriterUrl("http"));
    assertEquals(new URL("https://server2:8090"), authStore.masterWriterUrl("https"));

    newWriter = storeNodeId;
    authStore.makeMasterWriter(newWriter);
    TestUtils.waitForCondition(() -> authStore.url("http").equals(authStore.masterWriterUrl("http")),
        "Rebalance not completed");
    TestUtils.waitForCondition(() -> authStore.writer().ready(), "Writer not ready after rebalance");

    assertEquals(authStore.nodes.values().stream().map(n -> n.url("http")).collect(Collectors.toSet()),
        authStore.activeNodeUrls("http"));
    assertEquals(authStore.nodes.values().stream().map(n -> n.url("https")).collect(Collectors.toSet()),
        authStore.activeNodeUrls("https"));
  }

  @Test
  public void testClusterScopeAssignment() throws Exception {
    authWriter.addClusterRoleBinding(alice, "ClusterAdmin", clusterA).toCompletableFuture().join();
    assertEquals(Collections.emptySet(), rbacResources(alice, "ClusterAdmin", clusterA));

    authWriter.addClusterRoleBinding(bob, "Operator", clusterB).toCompletableFuture().join();
    assertEquals(Collections.emptySet(), rbacResources(bob, "Operator", clusterB));
    assertNull(rbacResources(bob, "Operator", clusterA));
    assertNull(rbacResources(bob, "ClusterAdmin", clusterB));

    authWriter.addClusterRoleBinding(alice, "Operator", clusterA).toCompletableFuture().join();
    assertEquals(Collections.emptySet(), rbacResources(alice, "Operator", clusterA));
    assertEquals(Collections.emptySet(), rbacResources(alice, "ClusterAdmin", clusterA));

    RbacTestUtils.deleteRoleBinding(authCache, alice, "ClusterAdmin", clusterA);
    assertNull(rbacResources(alice, "ClusterAdmin", clusterA));
    assertEquals(Collections.emptySet(), rbacResources(alice, "Operator", clusterA));
    RbacTestUtils.deleteRoleBinding(authCache, alice, "Operator", clusterA);
    assertNull(rbacResources(alice, "Operator", clusterA));
    assertEquals(Collections.emptySet(), rbacResources(bob, "Operator", clusterB));
  }

  @Test
  public void testResourceScopeBinding() throws Exception {
    Collection<ResourcePattern> aliceResources = resources("aliceTopicA", "aliceGroupB");
    authWriter.replaceResourceRoleBinding(alice, "Reader", clusterA, aliceResources).toCompletableFuture().join();
    assertEquals(aliceResources, rbacResources(alice, "Reader", clusterA));
    Collection<ResourcePattern> resources2 = resources("aliceTopicA", "aliceGroupD");
    authWriter.addResourceRoleBinding(alice, "Reader", clusterA, resources2).toCompletableFuture().join();
    assertEquals(3, rbacResources(alice, "Reader", clusterA).size());
    aliceResources.addAll(resources2);
    assertEquals(aliceResources, rbacResources(alice, "Reader", clusterA));

    // Add resources without assigning first, this should assign role with resources
    Collection<ResourcePattern> bobResources = resources("bobTopic", "bobGroup");
    authWriter.addResourceRoleBinding(bob, "Writer", clusterB, bobResources).toCompletableFuture().join();
    assertEquals(bobResources, rbacResources(bob, "Writer", clusterB));
    assertNull(rbacResources(bob, "Writer", clusterA));

    // Set resources with group principal
    KafkaPrincipal finance = new KafkaPrincipal("Group", "finance");
    Collection<ResourcePattern> financeResources = resources("financeTopic", "financeGroup");
    authWriter.replaceResourceRoleBinding(finance, "Writer", clusterB, financeResources).toCompletableFuture().join();
    assertEquals(financeResources, rbacResources(finance, "Writer", clusterB));
    financeResources = resources("financeTopic2", "financeGroup");
    authWriter.replaceResourceRoleBinding(finance, "Writer", clusterB, financeResources).toCompletableFuture().join();
    assertEquals(financeResources, rbacResources(finance, "Writer", clusterB));

    // Remove role
    authWriter.removeRoleBinding(bob, "Writer", clusterA).toCompletableFuture().join();
    assertEquals(bobResources, rbacResources(bob, "Writer", clusterB));
    authWriter.removeRoleBinding(bob, "Writer", clusterB).toCompletableFuture().join();
    assertNull(rbacResources(bob, "Writer", clusterB));

    // Remove role resources
    authWriter.removeResourceRoleBinding(alice, "Reader", clusterA,
        resourceFilters("some.topic", "some.group")).toCompletableFuture().join();
    assertEquals(aliceResources, rbacResources(alice, "Reader", clusterA));
    authWriter.removeResourceRoleBinding(alice, "Reader", clusterA,
        Collections.singleton(groupResource("aliceGroupB").toFilter())).toCompletableFuture().join();
    aliceResources.remove(groupResource("aliceGroupB"));
    assertEquals(aliceResources, rbacResources(alice, "Reader", clusterA));
    authWriter.removeResourceRoleBinding(alice, "Reader", clusterA,
        aliceResources.stream().map(ResourcePattern::toFilter).collect(Collectors.toSet())).toCompletableFuture().join();
    assertNull(rbacResources(alice, "Reader", clusterA));
  }

  @Test
  public void testResourceRemoveFilter() throws Exception {
    ResourceType topicType = new ResourceType("Topic");
    ResourceType groupType = new ResourceType("Group");
    ResourcePattern prefixedFinanceTopic = new ResourcePattern(topicType, "finance", PatternType.PREFIXED);
    ResourcePattern prefixedFinanceGroup = new ResourcePattern(groupType, "finance", PatternType.PREFIXED);
    ResourcePattern literalFinanceGroup = new ResourcePattern(topicType, "financeTopicA", PatternType.LITERAL);
    ResourcePattern literalAliceGroup = new ResourcePattern(groupType, "aliceGroup", PatternType.LITERAL);
    Collection<ResourcePattern> aliceResources = new HashSet<>();
    aliceResources.add(prefixedFinanceTopic);
    aliceResources.add(prefixedFinanceGroup);
    aliceResources.add(literalFinanceGroup);
    aliceResources.add(literalAliceGroup);
    authWriter.addResourceRoleBinding(alice, "Reader", clusterA, aliceResources).toCompletableFuture().join();
    assertEquals(aliceResources, rbacResources(alice, "Reader", clusterA));

    authWriter.removeResourceRoleBinding(alice, "Reader", clusterA,
        Utils.mkSet(new ResourcePatternFilter(topicType, "financeTopicA", PatternType.MATCH)))
        .toCompletableFuture().join();
    assertEquals(Utils.mkSet(prefixedFinanceGroup, literalAliceGroup), rbacResources(alice, "Reader", clusterA));

    authWriter.replaceResourceRoleBinding(alice, "Reader", clusterA, aliceResources).toCompletableFuture().join();
    authWriter.removeResourceRoleBinding(alice, "Reader", clusterA,
        Utils.mkSet(new ResourcePatternFilter(null, "financeTopicA", PatternType.MATCH)))
        .toCompletableFuture().join();
    assertEquals(Utils.mkSet(literalAliceGroup), rbacResources(alice, "Reader", clusterA));

    authWriter.replaceResourceRoleBinding(alice, "Reader", clusterA, aliceResources).toCompletableFuture().join();
    authWriter.removeResourceRoleBinding(alice, "Reader", clusterA,
        Utils.mkSet(new ResourcePatternFilter(null, "financeTopicA", PatternType.ANY)))
        .toCompletableFuture().join();
    assertEquals(Utils.mkSet(literalAliceGroup, prefixedFinanceGroup, prefixedFinanceTopic),
        rbacResources(alice, "Reader", clusterA));
  }

  @Test
  public void testConcurrentUpdates() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    int nThreads = 5;
    int iterationsPerThread = 10;

    try {
      for (int i = 0; i < nThreads; i++) {
        int id = i;
        executorService.submit(() -> {
          for (int j = 0; j < iterationsPerThread; j++) {
            int k = id * iterationsPerThread + j;
            authWriter.addResourceRoleBinding(alice, "Reader", clusterA,
                resources("name" + k, "name" + k));
            authWriter.createAcls(clusterA, Collections.singletonList(
                topicBinding("Alice", "topic" + k, new Operation("Write"), PermissionType.ALLOW)));
          }
        });
      }

      waitForUpdate(() -> rbacResources(alice, "Reader", clusterA), nThreads * iterationsPerThread * 2);
      waitForUpdate(() -> authCache.aclRules(clusterA).values(), nThreads * iterationsPerThread);

      for (int i = 0; i < nThreads; i++) {
        int id = i;
        executorService.submit(() -> {
          for (int j = 0; j < iterationsPerThread; j++) {
            int k = id * iterationsPerThread + j;
            authWriter.removeResourceRoleBinding(alice, "Reader", clusterA,
                Collections.singletonList(new ResourcePatternFilter(ResourceType.ALL, "name" + k, PatternType.LITERAL)));
            authWriter.deleteAcls(clusterA,
                Collections.singletonList(new AclBindingFilter(
                    new org.apache.kafka.common.resource.ResourcePatternFilter(org.apache.kafka.common.resource.ResourceType.ANY, "topic" + k, PatternType.ANY),
                    AccessControlEntryFilter.ANY)), r -> true);
          }
        });
      }

      waitForUpdate(() -> rbacResources(alice, "Reader", clusterA), 0);
      waitForUpdate(() -> authCache.aclRules(clusterA).values(), 0);
    } finally {
      executorService.shutdownNow();
    }
  }

  /**
   * Verifies that headers injected by tracing/monitoring frameworks don't cause any issues.
   */
  @Test
  public void testHeaders() throws Exception {
    RecordHeader header = new RecordHeader("TRACE", "test".getBytes("UTF-8"));
    authStore.header = header;
    authWriter.addClusterRoleBinding(alice, "ClusterAdmin", clusterA).toCompletableFuture().join();
    assertEquals(Collections.emptySet(), rbacResources(alice, "ClusterAdmin", clusterA));

    Headers headers = new RecordHeaders().add(header);
    JsonSerde<RoleBindingKey> keySerde = JsonSerde.serde(RoleBindingKey.class, true);
    RoleBindingKey key = new RoleBindingKey(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"),
        "ClusterAdmin", clusterA);
    assertEquals(key, keySerde.deserialize("topic1", keySerde.serialize("topic2", key)));
    assertEquals(key, keySerde.deserialize("topic1", headers, keySerde.serialize("topic2", headers, key)));

    JsonSerde<RoleBindingValue> valueSerde = JsonSerde.serde(RoleBindingValue.class, false);
    RoleBindingValue value = new RoleBindingValue(Collections.emptySet());
    assertEquals(value, valueSerde.deserialize("topic1", valueSerde.serialize("topic2", value)));
    assertEquals(value, valueSerde.deserialize("topic1", headers, valueSerde.serialize("topic2", headers, value)));
  }

  @Test(expected = InvalidRequestException.class)
  public void testClusterScopeAddResources() throws Exception {
    authWriter.addResourceRoleBinding(bob, "Operator", clusterA, resources("topicA", "groupB"));
  }

  @Test(expected = InvalidRequestException.class)
  public void testClusterScopeRemoveResources() throws Exception {
    authWriter.removeResourceRoleBinding(bob, "Operator", clusterA, resourceFilters("topicA", "groupB"));
  }

  @Test(expected = InvalidRequestException.class)
  public void testClusterScopeSetResources() throws Exception {
    authWriter.replaceResourceRoleBinding(bob, "Operator", clusterA, resources("topicA", "groupB"));
  }

  @Test(expected = InvalidRequestException.class)
  public void testResourceScopeBindingWithoutResources() throws Exception {
    authWriter.addClusterRoleBinding(alice, "Reader", clusterA);
  }

  @Test(expected = InvalidRequestException.class)
  public void testResourceScopeSetEmptyResources() throws Exception {
    authWriter.replaceResourceRoleBinding(alice, "Reader", clusterA, Collections.emptySet());
  }


  @Test(expected = InvalidRoleBindingException.class)
  public void testUnknownRoleAddBinding() throws Exception {
    authWriter.addClusterRoleBinding(bob, "SomeRole", clusterA);
  }

  @Test(expected = InvalidRoleBindingException.class)
  public void testUnknownRoleAddResources() throws Exception {
    authWriter.addResourceRoleBinding(bob, "SomeRole", clusterA, resources("topicA", "groupB"));
  }

  @Test(expected = InvalidRoleBindingException.class)
  public void testUnknownRoleSetResources() throws Exception {
    authWriter.replaceResourceRoleBinding(bob, "SomeRole", clusterA, resources("topicA", "groupB"));
  }

  @Test(expected = InvalidRoleBindingException.class)
  public void testUnknownRoleRemoveResources() throws Exception {
    authWriter.removeResourceRoleBinding(bob, "SomeRole", clusterA, resourceFilters("topicA", "groupB"));
  }

  @Test(expected = InvalidRoleBindingException.class)
  public void testUnknownRoleRemoveBinding() throws Exception {
    authWriter.removeRoleBinding(bob, "SomeRole", clusterA);
  }

  @Test(expected = InvalidScopeException.class)
  public void testUnknownScopeAddBinding() throws Exception {
    authWriter.addClusterRoleBinding(alice, "Operator", anotherClusterA);
  }

  @Test(expected = InvalidScopeException.class)
  public void testUnknownScopeAddResources() throws Exception {
    authWriter.addResourceRoleBinding(alice, "Reader", anotherClusterA, resources("topicA", "groupB"));
  }

  @Test(expected = InvalidScopeException.class)
  public void testUnknownScopeSetResources() throws Exception {
    authWriter.replaceResourceRoleBinding(alice, "Reader", anotherClusterA, resources("topicA", "groupB"));
  }

  @Test(expected = InvalidScopeException.class)
  public void testUnknownScopeRemoveResources() throws Exception {
    authWriter.removeResourceRoleBinding(alice, "Reader", anotherClusterA, resourceFilters("topicA", "groupB"));
  }

  @Test(expected = InvalidScopeException.class)
  public void testUnknownScopeRemoveBinding() throws Exception {
    authWriter.removeRoleBinding(alice, "Operator", anotherClusterA);
  }

  @Test(expected = InvalidScopeException.class)
  public void testInvalidScopeAddBinding() throws Exception {
    authWriter.addClusterRoleBinding(alice, "Operator", invalidScope);
  }

  @Test(expected = InvalidScopeException.class)
  public void testInvalidScopeAddResources() throws Exception {
    authWriter.addResourceRoleBinding(alice, "Reader", invalidScope, resources("topicA", "groupB"));
  }

  @Test(expected = InvalidScopeException.class)
  public void testInvalidScopeSetResources() throws Exception {
    authWriter.replaceResourceRoleBinding(alice, "Reader", invalidScope, resources("topicA", "groupB"));
  }

  @Test(expected = InvalidScopeException.class)
  public void testInvalidScopeRemoveResources() throws Exception {
    authWriter.removeResourceRoleBinding(alice, "Reader", invalidScope, resourceFilters("topicA", "groupB"));
  }

  @Test(expected = InvalidScopeException.class)
  public void testInvalidScopeRemoveBinding() throws Exception {
    authWriter.removeRoleBinding(alice, "Operator", invalidScope);
  }

  @Test(expected = InvalidScopeException.class)
  public void testInvalidScopeAddAclBinding() throws Exception {
    authWriter.createAcls(invalidScope, Collections.emptyList());
  }

  @Test(expected = InvalidScopeException.class)
  public void testInvalidScopeDeleteAclBinding() throws Exception {
    authWriter.deleteAcls(invalidScope, Collections.emptyList(), null);
  }

  @Test(expected = NotMasterWriterException.class)
  public void testNoMasterWriter() throws Exception {
    authStore.makeMasterWriter(-1);
    TestUtils.waitForCondition(() -> !authStore.url("http").equals(authStore.masterWriterUrl("http")),
        "Not rebalancing");
    authWriter.addClusterRoleBinding(bob, "Operator", clusterA);
  }

  @Test(expected = NotMasterWriterException.class)
  public void testNewMasterWriter() throws Exception {
    authStore.makeMasterWriter(storeNodeId + 1);
    TestUtils.waitForCondition(() -> !authStore.url("http").equals(authStore.masterWriterUrl("http")),
        "Rebalance not complete");
    authWriter.addClusterRoleBinding(bob, "Operator", clusterA);
  }

  @Test
  public void testWriterReelectionBeforeProduceComplete() throws Exception {
    TestUtils.waitForCondition(() -> authCache.status(0) == MetadataStoreStatus.INITIALIZED,
        "Auth store not initialized");
    authStore.configureDelays(Long.MAX_VALUE, Long.MAX_VALUE); // Don't complete produce/consume

    CompletionStage<Void> stage1 = authWriter.addResourceRoleBinding(bob, "Reader", clusterA,
        resources("topicB", "groupB"));
    CompletionStage<Void> stage2 = authWriter.replaceResourceRoleBinding(bob, "Reader", clusterA,
        resources("topicA", "groupA"));
    authWriter.stopWriter(1);
    authWriter.startWriter(2);
    authStore.producer.completeNext();

    // Write shouldn't complete even though local generation changed
    assertFalse(stage1.toCompletableFuture().isDone());
    assertFalse(stage2.toCompletableFuture().isDone());

    // Write should complete successfully if it is consumed before the new generation status record
    List<ProducerRecord<AuthKey, AuthValue>> sent = authStore.producer.history();
    authStore.consumer.addRecord(authStore.consumerRecord(sent.get(sent.size() - 2)));
    stage1.toCompletableFuture().get(10, TimeUnit.SECONDS);

    // Pending write should fail when new generation status record appears
    authStore.addNewGenerationStatusRecord(3);
    verifyFailure(stage2, NotMasterWriterException.class);
  }

  @Test
  public void testWriterReelectionBeforeConsumeComplete() throws Exception {
    TestUtils.waitForCondition(() -> authCache.status(0) == MetadataStoreStatus.INITIALIZED,
        "Auth store not initialized");
    authStore.configureDelays(Long.MAX_VALUE, Long.MAX_VALUE); // Don't complete produce/consume
    CompletionStage<Void> stage = authWriter.addClusterRoleBinding(bob, "Operator", clusterA);

    authStore.addNewGenerationStatusRecord(2);
    verifyFailure(stage, NotMasterWriterException.class);
  }

  @Test
  public void testProduceFailureDuringInitialization() throws Exception {
    authStore.close();
    createAuthStoreWithProduceFailure(new NotLeaderForPartitionException(), 1, AuthEntryType.STATUS);
    startAuthStore(authStore, null, 0);
    TestUtils.waitForCondition(() -> authStore.assignCount.get() == 2, "Writer not assigned");
    TestUtils.waitForCondition(() -> authStore.masterWriterUrl("http") != null, "Writer not started");
    TestUtils.waitForCondition(() -> authStore.writer().ready(), "Writer not ready");
  }

  @Test
  public void testProduceFailureDuringInitializationWithLdap() throws Exception {
    authStore.close();
    createAuthStoreWithProduceFailure(new AuthenticationException("Test exception"), 1, AuthEntryType.STATUS);
    Supplier<Map<String, Set<String>>> ldapGroupSupplier =
        () -> Collections.singletonMap("user1", Collections.singleton("group1"));
    startAuthStore(authStore, new MockLdapStore(ldapGroupSupplier), 10000);
    TestUtils.waitForCondition(() -> authStore.assignCount.get() == 2, "Writer not assigned");
    TestUtils.waitForCondition(() -> authStore.masterWriterUrl("http") != null, "Writer not started");
    TestUtils.waitForCondition(() -> authStore.writer().ready(), "Writer not ready");
  }

  @Test
  public void testLdapProduceFailureDuringInitialization() throws Exception {
    authStore.close();
    createAuthStoreWithProduceFailure(new KafkaException("Test exception"), 1, AuthEntryType.USER);
    Supplier<Map<String, Set<String>>> ldapGroupSupplier =
        () -> Collections.singletonMap("user1", Collections.singleton("group1"));
    startAuthStore(authStore, new MockLdapStore(ldapGroupSupplier), 10000);
    TestUtils.waitForCondition(() -> authStore.assignCount.get() == 2, "Writer not assigned");
    TestUtils.waitForCondition(() -> authStore.masterWriterUrl("http") != null, "Writer not started");
    TestUtils.waitForCondition(() -> authStore.writer().ready(), "Writer not ready");
  }

  @Test
  public void testLdapProduceFailureAfterInitialization() throws Exception {
    authStore.close();
    createAuthStoreWithProduceFailure(new KafkaException("Test exception"), 3, AuthEntryType.USER);
    Supplier<Map<String, Set<String>>> ldapGroupSupplier =
        () -> Collections.singletonMap("user1", Collections.singleton("group1"));
    startAuthStore(authStore, new MockLdapStore(ldapGroupSupplier), 10);
    TestUtils.waitForCondition(() -> authStore.writer().ready(), "Writer not ready");

    TestUtils.waitForCondition(() -> authStore.assignCount.get() == 2, "Writer not assigned");
    TestUtils.waitForCondition(() -> authStore.masterWriterUrl("http") != null, "Writer not started");
    TestUtils.waitForCondition(() -> authStore.writer().ready(), "Writer not ready");
  }

  @Test
  public void testRoleBindingProduceFailure() throws Exception {
    authStore.close();
    KafkaException testException = new KafkaException("Test exception");
    createAuthStoreWithProduceFailure(testException, 1, AuthEntryType.ROLE_BINDING);
    startAuthStore(authStore, null, 0);
    TestUtils.waitForCondition(() -> authStore.writer().ready(), "Writer not ready");
    Throwable e = assertThrows(ExecutionException.class, () ->
        authWriter.addClusterRoleBinding(alice, "ClusterAdmin", clusterA).toCompletableFuture().get(10, TimeUnit.SECONDS));
    assertEquals(testException, e.getCause());

    authWriter.addClusterRoleBinding(alice, "ClusterAdmin", clusterA).toCompletableFuture().join();
    assertEquals(1, authStore.assignCount.get());
  }

  @Test
  public void testStatusProduceFailureAfterInitialization() throws Exception {
    authStore.close();
    int failureIndex = 3 * numPartitions + 1; // INIITALZING and 2 INITIALIZED produced for each partition during startup
    createAuthStoreWithProduceFailure(new KafkaException("Test exception"), failureIndex, AuthEntryType.STATUS);
    Supplier<Map<String, Set<String>>> ldapGroupSupplier =
        () -> Collections.singletonMap("user1", Collections.singleton("group1"));
    startAuthStore(authStore, new MockLdapStore(ldapGroupSupplier), 10000);
    TestUtils.waitForCondition(() -> authStore.assignCount.get() == 1, "Writer not assigned");
    TestUtils.waitForCondition(() -> authStore.writer().ready(), "Writer not ready");

    authWriter.writeExternalStatus(MetadataStoreStatus.INITIALIZED, null, 1);
    TestUtils.waitForCondition(() -> authStore.assignCount.get() == 1, "Writer not reassigned");
    TestUtils.waitForCondition(() -> authStore.masterWriterUrl("http") != null, "Writer not started");
  }

  @Test
  public void testLdapSearchFailureDuringInitialization() throws Exception {
    authStore.close();
    authStore = new MockAuthStore(rbacRoles, time, rootScope, numPartitions, storeNodeId);
    AtomicInteger failures = new AtomicInteger(1);
    Supplier<Map<String, Set<String>>> ldapGroupSupplier = () -> {
      if (failures.getAndDecrement() > 0) {
        return Collections.singletonMap("user1", Collections.singleton("group1"));
      } else {
        authStore.prepareMasterWriter(storeNodeId);
        throw new RuntimeException("Search failed");
      }
    };
    startAuthStore(authStore, new MockLdapStore(ldapGroupSupplier), 10000);
    TestUtils.waitForCondition(() -> authStore.assignCount.get() == 1, "Writer not assigned");
    TestUtils.waitForCondition(() -> authStore.masterWriterUrl("http") != null, "Writer not started");
    TestUtils.waitForCondition(() -> authStore.writer().ready(), "Writer not ready");
  }

  @Test
  public void testStopWriterAbortsLdapStoreStartup() throws Exception {
    authStore.close();
    authStore = new MockAuthStore(rbacRoles, time, rootScope, numPartitions, storeNodeId);
    List<LdapGroupManager> groupManagers = new ArrayList<>();
    Semaphore semaphore = new Semaphore(0);
    MockLdapStore mockLdapStore = new MockLdapStore(Collections::emptyMap) {
      @Override
      LdapGroupManager createLdapGroupManager(Map<String, ?> configs, Time time,
          ExternalStoreListener<UserKey, UserValue> listener) {
        try {
          semaphore.acquire();
        } catch (InterruptedException e) {
          // ignore
        }
        LdapGroupManager groupManager = super.createLdapGroupManager(configs, time, listener);
        groupManagers.add(groupManager);
        return groupManager;
      }
    };
    startAuthStore(authStore, mockLdapStore, 10000);
    TestUtils.waitForCondition(semaphore::hasQueuedThreads, "Group manager not created during startup");
    authWriter.stopWriter(1);
    assertFalse("Start up not aborted", semaphore.hasQueuedThreads());
    assertEquals(1, groupManagers.size());
    assertThrows(IllegalStateException.class, () -> groupManagers.get(0).groups("testuser"));
    semaphore.release();
    authWriter.startWriter(2);
    TestUtils.waitForCondition(() -> groupManagers.size() >= 2, "New group manager not created during startup");
    assertEquals(2, groupManagers.size());
  }

  @Test
  public void testMultipleWriterElections() throws Exception {
    int generationId = 1;
    for (int i = 0; i < 3; i++) {
      TestUtils.waitForCondition(() -> authWriter.ready(), "Writer not ready");
      authWriter.addResourceRoleBinding(alice, "Reader", clusterA,
          resources("topic" + i, "group" + i)).toCompletableFuture().get(10, TimeUnit.SECONDS);

      authWriter.stopWriter(generationId);
      authWriter.startWriter(++generationId);
    }
  }

  @Test
  public void testStopDifferentGeneration() throws Exception {
    int generationId = 1;
    authStore.close();
    Semaphore writeSemaphore = new Semaphore(0);
    Semaphore rebalanceSemaphore = new Semaphore(0);
    authStore = new MockAuthStore(rbacRoles, time, rootScope, numPartitions, storeNodeId) {
      @Override
      protected void onSend(ProducerRecord<AuthKey, AuthValue> record, ScheduledExecutorService executor) {
        if (record.key() instanceof RoleBindingKey) {
          executor.submit(() -> {
            try {
              rebalanceSemaphore.release();
              assertTrue(writeSemaphore.tryAcquire(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          });
        }
        super.onSend(record, executor);
      }
    };
    startAuthStore(authStore, null, 0);
    for (int i = 0; i < 3; i++) {
      TestUtils.waitForCondition(() -> authWriter.ready(), "Writer not ready");
      CompletionStage<Void> stage1 = authWriter.addResourceRoleBinding(alice, "Reader", clusterA,
          resources("topic" + i, "group" + i));

      assertTrue(rebalanceSemaphore.tryAcquire(10, TimeUnit.SECONDS));
      authWriter.stopWriter(generationId - 1);
      authWriter.startWriter(++generationId);
      writeSemaphore.release();

      // Create a separate update request after rebalance and ensure that the request completes
      // without an exception. Using AclBinding here to avoid the test semaphores used to
      // control rebalance timing.
      TestUtils.waitForCondition(() -> authWriter.ready(), "Writer not ready");
      CompletionStage<Void> stage2 = authWriter.createAcls(clusterA,
          topicBinding("Alice", "Topic" + i, new Operation("Read"), PermissionType.ALLOW));

      try {
        stage1.toCompletableFuture().get(10, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        // Ignore failure due to rebalance
        assertEquals(NotMasterWriterException.class, e.getCause().getClass());
      }
      stage2.toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testAclBinding() {
    // 1. create alice topicA binding
    AclBinding aliceTopicABinding = topicBinding("Alice", "TopicA", new Operation("Read"), PermissionType.ALLOW);
    authWriter.createAcls(clusterA, aliceTopicABinding).toCompletableFuture().join();

    //verify the created alice topicA binding
    assertEquals(Collections.singleton(aliceTopicABinding), aclRules(clusterA, aliceTopicABinding.toFilter()));

    // 2. create alice topicB binding
    AclBinding aliceTopicBBinding = topicBinding("Alice", "TopicB", new Operation("Read"), PermissionType.ALLOW);
    authWriter.createAcls(clusterA, aliceTopicBBinding).toCompletableFuture().join();

    //verify the created alice topicB binding
    assertEquals(Collections.singleton(aliceTopicBBinding), aclRules(clusterA, aliceTopicBBinding.toFilter()));

    // 3. create bob topicA binding
    AclBinding bobTopicABinding = topicBinding("Bob", "TopicA", new Operation("Write"), PermissionType.ALLOW);
    authWriter.createAcls(clusterA, bobTopicABinding).toCompletableFuture().join();

    //verify the created bob topicA binding
    assertEquals(Collections.singleton(bobTopicABinding), aclRules(clusterA, bobTopicABinding.toFilter()));

    //get All created bindings
    Set<AclBinding> allBindings = new HashSet<>();
    allBindings.add(aliceTopicABinding);
    allBindings.add(aliceTopicBBinding);
    allBindings.add(bobTopicABinding);
    assertEquals(allBindings, aclRules(clusterA, AclBindingFilter.ANY));

    // Remove alice topicA Binding
    Collection<AclBinding> deletedBindings = authWriter.deleteAcls(clusterA, aliceTopicABinding.toFilter(),
        r -> true).toCompletableFuture().join();
    assertEquals(Collections.singleton(aliceTopicABinding), new HashSet<>(deletedBindings));

    //get All bindings
    allBindings.clear();
    allBindings.add(aliceTopicBBinding);
    allBindings.add(bobTopicABinding);
    assertEquals(allBindings, aclRules(clusterA, AclBindingFilter.ANY));

    // Remove bob topicA Binding
    deletedBindings = authWriter.deleteAcls(clusterA, bobTopicABinding.toFilter(),
        r -> true).toCompletableFuture().join();
    assertEquals(Collections.singleton(bobTopicABinding), new HashSet<>(deletedBindings));

    //get All bindings
    assertEquals(Collections.singleton(aliceTopicBBinding), aclRules(clusterA, AclBindingFilter.ANY));

    // delete remaining Bindings
    deletedBindings = authWriter.deleteAcls(clusterA, AclBindingFilter.ANY,
        r -> true).toCompletableFuture().join();
    assertEquals(Collections.singleton(aliceTopicBBinding), new HashSet<>(deletedBindings));

    assertTrue(authCache.aclBindings(clusterA, AclBindingFilter.ANY, r -> true).isEmpty());
  }

  private void createAuthStoreWithProduceFailure(RuntimeException exception, int failureIndex, AuthEntryType exceptionEntryType) {
    AtomicInteger count = new AtomicInteger();
    authStore = new MockAuthStore(rbacRoles, time, rootScope, numPartitions, storeNodeId) {
      @Override
      protected void onSend(ProducerRecord<AuthKey, AuthValue> record, ScheduledExecutorService executor) {
        if (exceptionEntryType == record.key().entryType() && count.incrementAndGet() == failureIndex) {
          authStore.prepareMasterWriter(storeNodeId);
          executor.schedule(() -> producer.errorNext(exception), 0, TimeUnit.MILLISECONDS);
        } else {
          super.onSend(record, executor);
        }
      }
    };
  }

  private void startAuthStore(MockAuthStore authStore,
                              MockLdapStore mockLdapStore,
                              int ldapRefreshIntervalMs) throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put("confluent.metadata.bootstrap.servers", "localhost:9092,localhost:9093");
    if (mockLdapStore != null) {
      configs.put("ldap." + Context.PROVIDER_URL, MockAuthStore.MOCK_LDAP_URL);
      configs.put(LdapConfig.REFRESH_INTERVAL_MS_PROP, String.valueOf(ldapRefreshIntervalMs));
      authStore.ldapStore = mockLdapStore;
    }
    authStore.configure(configs);
    authStore.startReader();
    authStore.startService(authStore.urls());
    authWriter = authStore.writer();
  }

  private Collection<ResourcePattern> rbacResources(KafkaPrincipal principal, String role, Scope scope) {
    RoleBindingValue assignment =
        (RoleBindingValue) authCache.get(new RoleBindingKey(principal, role, scope));
    return assignment == null ? null : assignment.resources();
  }

  private Collection<ResourcePattern> resources(String topic, String consumerGroup) {
    return Utils.mkSet(topicResource(topic), groupResource(consumerGroup));
  }

  private Collection<ResourcePatternFilter> resourceFilters(String topic, String consumerGroup) {
    return Utils.mkSet(topicResource(topic).toFilter(), groupResource(consumerGroup).toFilter());
  }

  private ResourcePattern topicResource(String topic) {
    return new ResourcePattern("Topic", topic, PatternType.LITERAL);
  }

  private ResourcePattern groupResource(String group) {
    return new ResourcePattern("Group", group, PatternType.LITERAL);
  }

  private void verifyFailure(CompletionStage<Void> stage, Class<? extends Exception> exceptionClass) throws Exception {
    try {
      stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertTrue("Unexpected exception " + cause, exceptionClass.isInstance(cause));
    }
  }

  private AclBinding topicBinding(String userName,
                                  String topic,
                                  Operation operation,
                                  PermissionType permissionType) {
    ResourcePattern resourcePattern = new ResourcePattern("Topic", topic, PatternType.LITERAL);
    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "username");
    AclRule accessRule = new AclRule(principal, permissionType, "", operation);
    return new AclBinding(ResourcePattern.to(resourcePattern), accessRule.toAccessControlEntry());
  }

  private Set<AclBinding> aclRules(Scope scope, AclBindingFilter filter) {
    return new HashSet<>(authCache.aclBindings(scope, filter, r -> true));
  }

  private void waitForUpdate(Supplier<Collection<?>> supplier, int expected) throws Exception {
    TestUtils.retryOnExceptionWithTimeout(() -> {
      Collection<?> list = supplier.get();
      assertEquals(expected, list == null ? 0 : list.size());
    });
  }
}
