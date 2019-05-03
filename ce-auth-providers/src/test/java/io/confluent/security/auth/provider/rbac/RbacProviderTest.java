// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.RoleBindingKey;
import io.confluent.security.auth.store.data.RoleBindingValue;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.rbac.RbacRoles;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RbacProviderTest {

  private final Scope clusterA = new Scope.Builder("testOrg").withKafkaCluster("clusterA").build();
  private final Scope clusterB = new Scope.Builder("testOrg").withKafkaCluster("clusterB").build();
  private final ResourcePattern clusterResource = new ResourcePattern(new ResourceType("Cluster"), "kafka-cluster", PatternType.LITERAL);
  private RbacProvider rbacProvider;
  private DefaultAuthCache authCache;
  private ResourcePattern topic = new ResourcePattern("Topic", "topicA", PatternType.LITERAL);

  @Before
  public void setUp() throws Exception {
    initializeRbacProvider(clusterA);
  }

  @After
  public void tearDown() {
    if (rbacProvider != null)
      rbacProvider.close();
  }

  @Test
  public void testSuperUserAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    updateRoleBinding(alice, "SuperUser", clusterA, null);
    assertTrue(rbacProvider.isSuperUser(alice, groups, clusterA));
    verifyRules(accessRules(alice, groups, clusterResource), "All");
    verifyRules(accessRules(alice, groups, topic), "All");

    // Delete non-existing role
    deleteRoleBinding(alice, "SuperUser", clusterB);
    assertTrue(rbacProvider.isSuperUser(alice, groups, clusterA));

    deleteRoleBinding(alice, "SuperUser", clusterA);
    assertFalse(rbacProvider.isSuperUser(alice, groups, clusterA));
  }

  @Test
  public void testSuperGroupAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);

    updateRoleBinding(admin, "SuperUser", clusterA, Collections.emptySet());
    assertTrue(rbacProvider.isSuperUser(alice, groups, clusterA));
    verifyRules(accessRules(alice, groups, clusterResource), "All");
    verifyRules(accessRules(alice, groups, topic), "All");

    assertFalse(rbacProvider.isSuperUser(alice, Collections.emptySet(), clusterA));

    deleteRoleBinding(admin, "SuperUser", clusterA);
    assertFalse(rbacProvider.isSuperUser(alice, groups, clusterA));

  }

  @Test
  public void testClusterScopeAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    updateRoleBinding(alice, "ClusterAdmin", clusterA, Collections.emptySet());
    verifyRules(accessRules(alice, groups, clusterResource),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic));

    updateRoleBinding(alice, "Operator", clusterA, Collections.emptySet());
    verifyRules(accessRules(alice, groups, clusterResource),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic), "DescribeConfigs", "AlterConfigs");
    updateRoleBinding(alice, "Operator", clusterA, Collections.singleton(topic));
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(alice, "ClusterAdmin", clusterA);
    verifyRules(accessRules(alice, groups, clusterResource));
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(alice, "Operator", clusterA);
    verifyRules(accessRules(alice, groups, clusterResource));
    verifyRules(accessRules(alice, groups, topic));
  }

  @Test
  public void testClusterScopeGroupAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);

    updateRoleBinding(admin, "ClusterAdmin", clusterA, Collections.emptySet());
    verifyRules(accessRules(alice, groups, clusterResource),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic));
    verifyRules(accessRules(alice, Collections.emptySet(), clusterResource));

    updateRoleBinding(admin, "Operator", clusterA, Collections.emptySet());
    verifyRules(accessRules(alice, groups, clusterResource),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic), "DescribeConfigs", "AlterConfigs");
    updateRoleBinding(admin, "Operator", clusterA, Collections.singleton(topic));
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    updateRoleBinding(alice, "Operator", clusterA, Collections.emptySet());
    verifyRules(accessRules(alice, groups, clusterResource),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(alice, "Operator", clusterA);
    verifyRules(accessRules(alice, groups, clusterResource),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(admin, "ClusterAdmin", clusterA);
    verifyRules(accessRules(alice, groups, clusterResource));
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(admin, "Operator", clusterA);
    verifyRules(accessRules(alice, groups, clusterResource));
    verifyRules(accessRules(alice, groups, topic));
  }

  @Test
  public void testLiteralResourceAccessRules() {
    verifyResourceAccessRules(new ResourcePattern("Topic", topic.name(), PatternType.LITERAL));
  }

  @Test
  public void testWildcardResourceAccessRules() {
    verifyResourceAccessRules(new ResourcePattern("Topic", "*", PatternType.LITERAL));
  }

  @Test
  public void testPrefixedResourceAccessRules() {
    verifyResourceAccessRules(new ResourcePattern("Topic", "top", PatternType.PREFIXED));
  }

  @Test
  public void testSingleCharPrefixedResourceAccessRules() {
    verifyResourceAccessRules(new ResourcePattern("Topic", "t", PatternType.PREFIXED));
  }

  @Test
  public void testFullNamePrefixedResourceAccessRules() {
    verifyResourceAccessRules(new ResourcePattern("Topic", "topic", PatternType.PREFIXED));
  }

  private void verifyResourceAccessRules(ResourcePattern roleResource) {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);
    Set<KafkaPrincipal> emptyGroups = Collections.emptySet();
    Set<ResourcePattern> resources = roleResource == null ?
        Collections.emptySet() : Collections.singleton(roleResource);

    updateRoleBinding(alice, "Reader", clusterA, resources);
    verifyRules(accessRules(alice, emptyGroups, clusterResource));
    verifyRules(accessRules(alice, emptyGroups, topic), "Read", "Describe");

    updateRoleBinding(admin, "Writer", clusterA, resources);
    verifyRules(accessRules(alice, groups, topic), "Read", "Describe", "Write");

    updateRoleBinding(alice, "Writer", clusterA, resources);
    verifyRules(accessRules(alice, groups, topic), "Read", "Describe", "Write");

    deleteRoleBinding(admin, "Writer", clusterA);
    verifyRules(accessRules(alice, groups, topic), "Read", "Describe", "Write");

    deleteRoleBinding(alice, "Reader", clusterA);
    verifyRules(accessRules(alice, groups, topic), "Describe", "Write");

    deleteRoleBinding(alice, "Writer", clusterA);
    verifyRules(accessRules(alice, groups, topic));
  }

  @Test
  public void testScopes() throws Exception {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    updateRoleBinding(alice, "Operator", clusterA, Collections.singleton(topic));
    verifyRules(accessRules(alice, groups, topic), "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, clusterResource));

    updateRoleBinding(alice, "ClusterAdmin", clusterB, Collections.emptySet());
    verifyRules(accessRules(alice, groups, clusterResource));
    verifyRules(accessRules(alice, groups, topic), "AlterConfigs", "DescribeConfigs");

  }

  @Test
  public void testProviderScope() throws Exception {
    initializeRbacProvider(Scope.intermediateScope("testOrg"));

    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    updateRoleBinding(alice, "Operator", clusterA, Collections.singleton(topic));
    verifyRules(rbacProvider.accessRules(alice, groups, clusterA, topic), "AlterConfigs", "DescribeConfigs");
    verifyRules(rbacProvider.accessRules(alice, groups, clusterA, clusterResource));

    updateRoleBinding(alice, "ClusterAdmin", clusterB, Collections.emptySet());
    verifyRules(rbacProvider.accessRules(alice, groups, clusterA, clusterResource));
    verifyRules(rbacProvider.accessRules(alice, groups, clusterA, topic), "AlterConfigs", "DescribeConfigs");
    verifyRules(rbacProvider.accessRules(alice, groups, clusterB, clusterResource), "AlterConfigs", "DescribeConfigs");
    verifyRules(rbacProvider.accessRules(alice, groups, clusterB, topic));

    try {
      Scope anotherScope = new Scope.Builder("anotherOrg").withKafkaCluster("clusterA").build();
      rbacProvider.accessRules(alice, groups, anotherScope, clusterResource);
      fail("Did not fail with invalid scope");
    } catch (InvalidScopeException e) {
      // Expected exception
    }
  }

  private void initializeRbacProvider(Scope scope) throws Exception {
    RbacRoles rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
    MockRbacProvider.MockAuthStore authStore = new MockRbacProvider.MockAuthStore(rbacRoles, scope);
    authCache = authStore.authCache();
    rbacProvider = new RbacProvider() {
      @Override
      public void configure(Map<String, ?> configs) {
        KafkaTestUtils.setFinalField(rbacProvider, RbacProvider.class, "authCache", authCache);
      }
    };
    rbacProvider.configureScope(scope);
    rbacProvider.configure(Collections.emptyMap());
  }

  private void updateRoleBinding(KafkaPrincipal principal, String role, Scope scope, Set<ResourcePattern> resources) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    RoleBindingValue value = new RoleBindingValue(resources == null ? Collections.emptySet() : resources);
    authCache.put(key, value);
  }

  private void deleteRoleBinding(KafkaPrincipal principal, String role, Scope scope) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    authCache.remove(key);
  }

  private Set<AccessRule> accessRules(KafkaPrincipal userPrincipal,
                                      Set<KafkaPrincipal> groupPrincipals,
                                      ResourcePattern resource) {
    return rbacProvider.accessRules(userPrincipal, groupPrincipals, clusterA, resource);
  }

  private void verifyRules(Set<AccessRule> rules, String... expectedOps) {
    Set<String> actualOps = rules.stream().map(r -> r.operation().name()).collect(Collectors.toSet());
    assertEquals(Utils.mkSet(expectedOps), actualOps);
  }
}

