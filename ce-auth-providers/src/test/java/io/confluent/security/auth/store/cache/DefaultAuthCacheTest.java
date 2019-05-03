// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.security.auth.store.data.StatusKey;
import io.confluent.security.auth.store.data.StatusValue;
import io.confluent.security.auth.store.kafka.KafkaAuthStore;
import io.confluent.security.auth.store.kafka.MockAuthStore;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourcePatternFilter;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.RoleBinding;
import io.confluent.security.rbac.RoleBindingFilter;
import io.confluent.security.rbac.UserMetadata;
import io.confluent.security.store.MetadataStoreException;
import io.confluent.security.store.MetadataStoreStatus;
import io.confluent.security.test.utils.RbacTestUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DefaultAuthCacheTest {

  private final MockTime time = new MockTime();
  private final Scope clusterA = Scope.kafkaClusterScope("clusterA");
  private final ResourcePattern clusterResource = new ResourcePattern(new ResourceType("Cluster"), "kafka-cluster", PatternType.LITERAL);
  private RbacRoles rbacRoles;
  private KafkaAuthStore authStore;
  private DefaultAuthCache authCache;

  @Before
  public void setUp() throws Exception {
    rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
    this.authStore = MockAuthStore.create(rbacRoles, time, clusterA, 1, 1);
    authCache = authStore.authCache();
  }

  @After
  public void tearDown() {
    if (authStore != null)
      authStore.close();
  }

  @Test
  public void testClusterRoleBinding() throws Exception {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    RbacTestUtils.updateRoleBinding(authCache, alice, "ClusterAdmin", Scope.kafkaClusterScope("clusterA"), Collections.emptySet());
    assertEquals(1, authCache.rbacRules(clusterA).size());
    verifyPermissions(alice, clusterResource, "DescribeConfigs", "AlterConfigs");
    assertEquals(Collections.singleton(new RoleBinding(alice, "ClusterAdmin", Scope.kafkaClusterScope("clusterA"), null)),
        authCache.rbacRoleBindings(clusterA));
    assertEquals(Collections.emptySet(), authCache.rbacRoleBindings(Scope.kafkaClusterScope("clusterB")));

    RbacTestUtils.deleteRoleBinding(authCache, alice, "ClusterAdmin", Scope.kafkaClusterScope("clusterA"));
    assertTrue(authCache.rbacRules(clusterA).isEmpty());

    assertEquals(rbacRoles, authCache.rbacRoles());
  }

  @Test
  public void testResourceRoleBindingFilter() throws Exception {
    authStore.close();
    authStore = MockAuthStore.create(rbacRoles, time, Scope.ROOT_SCOPE, 1, 1);
    authCache = authStore.authCache();

    io.confluent.security.authorizer.ResourceType topicType = new io.confluent.security.authorizer.ResourceType("Topic");
    io.confluent.security.authorizer.ResourceType groupType = new io.confluent.security.authorizer.ResourceType("Group");
    ResourcePattern generalTopic = new ResourcePattern(topicType, "generalTopic", PatternType.LITERAL);
    ResourcePattern financeTopic = new ResourcePattern(topicType, "financeTopic", PatternType.LITERAL);
    ResourcePattern generalConsumerGroup = new ResourcePattern(groupType, "generalConsumerGroup", PatternType.LITERAL);
    ResourcePattern financeTopicPattern = new ResourcePattern(topicType, "finance", PatternType.PREFIXED);
    ResourcePattern financeGroupPattern = new ResourcePattern(groupType, "finance", PatternType.PREFIXED);

    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal bob = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Bob");
    RbacTestUtils.updateRoleBinding(authCache, alice, "Reader", Scope.kafkaClusterScope("financeCluster"),
        Utils.mkSet(financeTopicPattern, financeGroupPattern));
    RbacTestUtils.updateRoleBinding(authCache, alice, "Writer", Scope.kafkaClusterScope("financeCluster"),
        Utils.mkSet(financeTopic));
    RbacTestUtils.updateRoleBinding(authCache, alice, "Reader", Scope.kafkaClusterScope("generalCluster"),
        Utils.mkSet(generalTopic, generalConsumerGroup));
    RbacTestUtils.updateRoleBinding(authCache, bob, "Writer", Scope.kafkaClusterScope("generalCluster"),
        Collections.singleton(generalTopic));

    RoleBinding aliceFinanceWrite = new RoleBinding(alice, "Writer", Scope.kafkaClusterScope("financeCluster"),
        Utils.mkSet(financeTopic));
    RoleBinding bobGeneralWrite = new RoleBinding(bob, "Writer", Scope.kafkaClusterScope("generalCluster"),
        Utils.mkSet(generalTopic));
    assertEquals(Utils.mkSet(aliceFinanceWrite),
        authCache.rbacRoleBindings(new RoleBindingFilter(alice, "Writer", Scope.kafkaClusterScope("financeCluster"),
        new ResourcePatternFilter(topicType, financeTopic.name(), PatternType.LITERAL))));
    assertEquals(Utils.mkSet(aliceFinanceWrite, bobGeneralWrite),
        authCache.rbacRoleBindings(new RoleBindingFilter(null, "Writer", null, null)));
    assertEquals(Utils.mkSet(bobGeneralWrite),
        authCache.rbacRoleBindings(new RoleBindingFilter(bob, "Writer", null, null)));
    assertEquals(Utils.mkSet(bobGeneralWrite),
        authCache.rbacRoleBindings(new RoleBindingFilter(null, "Writer", Scope.kafkaClusterScope("generalCluster"), null)));
    assertEquals(Utils.mkSet(bobGeneralWrite),
        authCache.rbacRoleBindings(new RoleBindingFilter(bob, "Writer", Scope.kafkaClusterScope("generalCluster"),
            new ResourcePatternFilter(topicType, null, PatternType.LITERAL))));
    assertEquals(Utils.mkSet(bobGeneralWrite),
        authCache.rbacRoleBindings(new RoleBindingFilter(bob, "Writer", Scope.kafkaClusterScope("generalCluster"),
            new ResourcePatternFilter(topicType, generalTopic.name(), PatternType.ANY))));
    assertEquals(Utils.mkSet(bobGeneralWrite, aliceFinanceWrite),
        authCache.rbacRoleBindings(new RoleBindingFilter(null, "Writer", null,
            new ResourcePatternFilter(topicType, null, PatternType.ANY))));
    assertEquals(Utils.mkSet(bobGeneralWrite),
        authCache.rbacRoleBindings(new RoleBindingFilter(null, "Writer", null,
            new ResourcePatternFilter(ResourceType.ALL, "generalTopic", PatternType.MATCH))));
    assertEquals(Utils.mkSet(bobGeneralWrite, aliceFinanceWrite),
        authCache.rbacRoleBindings(new RoleBindingFilter(null, "Writer", null,
            new ResourcePatternFilter(null, null, null))));

    RoleBinding aliceFinanceTopic = new RoleBinding(alice, "Reader", Scope.kafkaClusterScope("financeCluster"),
        Utils.mkSet(financeTopicPattern));
    RoleBinding aliceFinanceRead = new RoleBinding(alice, "Reader", Scope.kafkaClusterScope("financeCluster"),
        Utils.mkSet(financeTopicPattern, financeGroupPattern));
    RoleBinding aliceGeneralTopic = new RoleBinding(alice, "Reader", Scope.kafkaClusterScope("generalCluster"),
        Utils.mkSet(generalTopic));
    RoleBinding aliceGeneralRead = new RoleBinding(alice, "Reader", Scope.kafkaClusterScope("generalCluster"),
        Utils.mkSet(generalTopic, generalConsumerGroup));
    assertEquals(Utils.mkSet(aliceFinanceRead),
        authCache.rbacRoleBindings(new RoleBindingFilter(null, "Reader", Scope.kafkaClusterScope("financeCluster"),
            new ResourcePatternFilter(null, null, PatternType.ANY))));
    assertEquals(Utils.mkSet(aliceFinanceRead, aliceGeneralRead),
        authCache.rbacRoleBindings(new RoleBindingFilter(alice, "Reader", null,
            new ResourcePatternFilter(null, null, PatternType.ANY))));
    assertEquals(Utils.mkSet(aliceFinanceRead),
        authCache.rbacRoleBindings(new RoleBindingFilter(alice, "Reader", null,
            new ResourcePatternFilter(null, null, PatternType.PREFIXED))));
    assertEquals(Utils.mkSet(aliceGeneralRead),
        authCache.rbacRoleBindings(new RoleBindingFilter(alice, "Reader", null,
            new ResourcePatternFilter(null, null, PatternType.LITERAL))));
    assertEquals(Utils.mkSet(aliceFinanceRead, aliceGeneralRead),
        authCache.rbacRoleBindings(new RoleBindingFilter(alice, "Reader", null,
            new ResourcePatternFilter(null, null, PatternType.MATCH))));
    assertEquals(Utils.mkSet(aliceFinanceTopic, aliceGeneralTopic),
        authCache.rbacRoleBindings(new RoleBindingFilter(alice, "Reader", null,
            new ResourcePatternFilter(topicType, null, PatternType.MATCH))));
    assertEquals(Utils.mkSet(aliceFinanceTopic),
        authCache.rbacRoleBindings(new RoleBindingFilter(alice, "Reader", null,
            new ResourcePatternFilter(topicType, "financeTopicA", PatternType.MATCH))));
    assertEquals(Utils.mkSet(aliceFinanceRead, aliceFinanceWrite),
        authCache.rbacRoleBindings(new RoleBindingFilter(alice, null, null,
            new ResourcePatternFilter(null, "financeTopic", PatternType.MATCH))));
    assertEquals(Utils.mkSet(aliceFinanceRead, aliceFinanceWrite),
        authCache.rbacRoleBindings(new RoleBindingFilter(null, null, null,
            new ResourcePatternFilter(null, "financeTopic", PatternType.MATCH))));
  }

  @Test
  public void testUserGroups() throws Exception {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    UserMetadata userMetadata = new UserMetadata(Collections.emptySet());
    assertEquals(Collections.emptySet(), authCache.groups(alice));
    RbacTestUtils.updateUser(authCache, alice, userMetadata.groups());
    assertEquals(Collections.emptySet(), authCache.groups(alice));
    assertEquals(userMetadata, authCache.userMetadata(alice));

    KafkaPrincipal developer = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "Developer");
    userMetadata = new UserMetadata(Collections.singleton(developer));
    RbacTestUtils.updateUser(authCache, alice, userMetadata.groups());
    assertEquals(Collections.singleton(developer), authCache.groups(alice));
    assertEquals(userMetadata, authCache.userMetadata(alice));

    KafkaPrincipal tester = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "Tester");
    userMetadata = new UserMetadata(Utils.mkSet(developer, tester));
    RbacTestUtils.updateUser(authCache, alice, userMetadata.groups());
    assertEquals(Utils.mkSet(developer, tester), authCache.groups(alice));
    assertEquals(userMetadata, authCache.userMetadata(alice));

    userMetadata = new UserMetadata(Collections.singleton(tester));
    RbacTestUtils.updateUser(authCache, alice, userMetadata.groups());
    assertEquals(Collections.singleton(tester), authCache.groups(alice));
    assertEquals(userMetadata, authCache.userMetadata(alice));

    RbacTestUtils.deleteUser(authCache, alice);
    assertEquals(Collections.emptySet(), authCache.groups(alice));
    assertNull(authCache.userMetadata(alice));
  }

  @Test
  public void testScopes() throws Exception {
    Scope clusterA = new Scope.Builder("org1").withKafkaCluster("clusterA").build();
    authStore.close();
    this.authStore = MockAuthStore.create(rbacRoles, time, Scope.intermediateScope("org1"), 1, 1);
    authCache = authStore.authCache();

    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> emptyGroups = Collections.emptySet();
    ResourcePattern topicA = new ResourcePattern("Topic", "topicA", PatternType.LITERAL);
    RbacTestUtils.updateRoleBinding(authCache, alice, "Reader", clusterA, Collections.singleton(topicA));
    assertEquals(1, authCache.rbacRules(clusterA).size());
    verifyPermissions(clusterA, alice, topicA, "Read", "Describe");

    Scope clusterB = new Scope.Builder("org1").withKafkaCluster("clusterB").build();
    RbacTestUtils.updateRoleBinding(authCache, alice, "ClusterAdmin", clusterB, Collections.emptySet());
    verifyPermissions(clusterB, alice, clusterResource, "AlterConfigs", "DescribeConfigs");
    verifyPermissions(clusterA, alice, clusterResource);
    verifyPermissions(clusterA, alice, topicA, "Read", "Describe");

    Scope clusterC = new Scope.Builder("org2").withKafkaCluster("clusterC").build();
    RbacTestUtils.updateRoleBinding(authCache, alice, "Writer", clusterC, Collections.singleton(topicA));
    try {
      authCache.rbacRules(clusterC, topicA, alice, emptyGroups);
      fail("Exception not thrown for unknown cluster");
    } catch (InvalidScopeException e) {
      // Expected exception
    }

    verifyPermissions(clusterB, alice, clusterResource, "AlterConfigs", "DescribeConfigs");
    verifyPermissions(clusterA, alice, clusterResource);
    verifyPermissions(clusterA, alice, topicA, "Read", "Describe");
  }

  @Test
  public void testStatusPropagation() throws Exception {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal developer = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "Developer");
    Collection<KafkaPrincipal> groups = Collections.singleton(developer);
    RbacTestUtils.updateUser(authCache, alice, groups);
    assertEquals(groups, authCache.groups(alice));

    assertEquals(MetadataStoreStatus.UNKNOWN, authCache.status(1));
    authCache.put(new StatusKey(1), new StatusValue(MetadataStoreStatus.INITIALIZING, 1, null));
    assertEquals(MetadataStoreStatus.INITIALIZING, authCache.status(1));
    assertEquals(MetadataStoreStatus.UNKNOWN, authCache.status(2));

    authCache.put(new StatusKey(2), new StatusValue(MetadataStoreStatus.INITIALIZED, 1, null));
    assertEquals(MetadataStoreStatus.INITIALIZED, authCache.status(2));
    assertEquals(groups, authCache.groups(alice));

    authCache.put(new StatusKey(2), new StatusValue(MetadataStoreStatus.FAILED, 1, null));
    verifyCacheFailed();

    authCache.put(new StatusKey(2), new StatusValue(MetadataStoreStatus.INITIALIZED, 1, null));
    assertEquals(groups, authCache.groups(alice));

    String error = "Test exception";
    authCache.put(new StatusKey(2), new StatusValue(MetadataStoreStatus.FAILED, 1, error));
    try {
      authCache.groups(alice);
      fail("Exception not thrown after error");
    } catch (MetadataStoreException e) {
      assertTrue("Unexpected exception " + e, e.getMessage().contains(error));
    }
    authCache.put(new StatusKey(1), new StatusValue(MetadataStoreStatus.FAILED, 1, error));
    verifyCacheFailed();
    authCache.put(new StatusKey(2), new StatusValue(MetadataStoreStatus.INITIALIZING, 1, null));
    verifyCacheFailed();
    authCache.put(new StatusKey(1), new StatusValue(MetadataStoreStatus.INITIALIZING, 1, null));
    assertEquals(groups, authCache.groups(alice));
  }

  private void verifyCacheFailed() {
    try {
      authCache.groups(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"));
      fail("Exception not thrown after error");
    } catch (MetadataStoreException e) {
      // Expected exception
    }
  }

  private void verifyPermissions(KafkaPrincipal principal,
                                 ResourcePattern resource,
                                 String... expectedOps) {
    verifyPermissions(clusterA, principal, resource, expectedOps);
  }

  private void verifyPermissions(Scope scope,
                                 KafkaPrincipal principal,
                                 ResourcePattern resource,
                                 String... expectedOps) {
    Set<String> actualOps = authCache.rbacRules(scope, resource, principal, Collections.emptySet())
        .stream()
        .filter(r -> r.principal().equals(principal))
        .map(r -> r.operation().name()).collect(Collectors.toSet());
    assertEquals(Utils.mkSet(expectedOps), actualOps);
  }
}
