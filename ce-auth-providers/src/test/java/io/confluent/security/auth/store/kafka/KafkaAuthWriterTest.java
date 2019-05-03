// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.AuthKey;
import io.confluent.security.auth.store.data.AuthValue;
import io.confluent.security.auth.store.data.RoleBindingKey;
import io.confluent.security.auth.store.data.RoleBindingValue;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourcePatternFilter;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.rbac.InvalidRoleBindingException;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.store.MetadataStoreStatus;
import io.confluent.security.store.NotMasterWriterException;
import io.confluent.security.test.utils.RbacTestUtils;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerRecord;
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
  private final KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
  private final KafkaPrincipal bob = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Bob");
  private final Scope clusterA = new Scope.Builder("testOrg").withKafkaCluster("clusterA").build();
  private final Scope clusterB = new Scope.Builder("testOrg").withKafkaCluster("clusterB").build();
  private final Scope anotherClusterA = new Scope.Builder("anotherOrg").withKafkaCluster("clusterA").build();
  private final Scope invalidScope = new Scope(Collections.emptyList(), Collections.singletonMap("", "invalid"));
  private final int storeNodeId = 1;

  private MockAuthStore authStore;
  private KafkaAuthWriter authWriter;
  private DefaultAuthCache authCache;

  @Before
  public void setUp() throws Exception {
    RbacRoles rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
    authStore = MockAuthStore.create(rbacRoles, time, Scope.intermediateScope("testOrg"), 2, storeNodeId);
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
    assertEquals(new URL("http://server2:8089"), authStore.masterWriterUrl("http"));
    assertEquals(new URL("https://server2:8090"), authStore.masterWriterUrl("https"));

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

  @Test(expected = IllegalArgumentException.class)
  public void testClusterScopeAddResources() throws Exception {
    authWriter.addResourceRoleBinding(bob, "Operator", clusterA, resources("topicA", "groupB"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testClusterScopeRemoveResources() throws Exception {
    authWriter.removeResourceRoleBinding(bob, "Operator", clusterA, resourceFilters("topicA", "groupB"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testClusterScopeSetResources() throws Exception {
    authWriter.replaceResourceRoleBinding(bob, "Operator", clusterA, resources("topicA", "groupB"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testResourceScopeBindingWithoutResources() throws Exception {
    authWriter.addClusterRoleBinding(alice, "Reader", clusterA);
  }

  @Test(expected = IllegalArgumentException.class)
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
    authStore.addNewGenerationStatusRecord(2);
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
}
