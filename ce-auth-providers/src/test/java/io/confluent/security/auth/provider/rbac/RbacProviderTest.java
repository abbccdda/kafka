// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.Resource;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.RoleBindingKey;
import io.confluent.security.auth.store.data.RoleBindingValue;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.Scope;
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

  private final String clusterA = "testOrg/clusterA";
  private RbacProvider rbacProvider;
  private DefaultAuthCache authCache;
  private Resource topic = new Resource("Topic", "topicA", PatternType.LITERAL);

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

    updateRoleBinding(alice, "Super User", clusterA, null);
    assertTrue(rbacProvider.isSuperUser(alice, groups, clusterA));
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic));

    // Delete non-existing role
    deleteRoleBinding(alice, "Super User", "testOrg/clusterB");
    assertTrue(rbacProvider.isSuperUser(alice, groups, clusterA));

    deleteRoleBinding(alice, "Super User", clusterA);
    assertFalse(rbacProvider.isSuperUser(alice, groups, clusterA));
  }

  @Test
  public void testSuperGroupAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);

    updateRoleBinding(admin, "Super User", clusterA, Collections.emptySet());
    assertTrue(rbacProvider.isSuperUser(alice, groups, clusterA));
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic));

    assertFalse(rbacProvider.isSuperUser(alice, Collections.emptySet(), clusterA));

    deleteRoleBinding(admin, "Super User", clusterA);
    assertFalse(rbacProvider.isSuperUser(alice, groups, clusterA));

  }

  @Test
  public void testClusterScopeAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    updateRoleBinding(alice, "Cluster Admin", clusterA, Collections.emptySet());
    verifyRules(accessRules(alice, groups, Resource.CLUSTER),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic));

    updateRoleBinding(alice, "Operator", clusterA, Collections.emptySet());
    verifyRules(accessRules(alice, groups, Resource.CLUSTER),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic));
    updateRoleBinding(alice, "Operator", clusterA, Collections.singleton(topic));
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(alice, "Cluster Admin", clusterA);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(alice, "Operator", clusterA);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic));
  }

  @Test
  public void testClusterScopeGroupAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);

    updateRoleBinding(admin, "Cluster Admin", clusterA, Collections.emptySet());
    verifyRules(accessRules(alice, groups, Resource.CLUSTER),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic));
    verifyRules(accessRules(alice, Collections.emptySet(), Resource.CLUSTER));

    updateRoleBinding(admin, "Operator", clusterA, Collections.emptySet());
    verifyRules(accessRules(alice, groups, Resource.CLUSTER),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic));
    updateRoleBinding(admin, "Operator", clusterA, Collections.singleton(topic));
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    updateRoleBinding(alice, "Operator", clusterA, Collections.emptySet());
    verifyRules(accessRules(alice, groups, Resource.CLUSTER),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(alice, "Operator", clusterA);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(admin, "Cluster Admin", clusterA);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(admin, "Operator", clusterA);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic));
  }

  @Test
  public void testLiteralResourceAccessRules() {
    verifyResourceAccessRules(new Resource("Topic", topic.name(), PatternType.LITERAL));
  }

  @Test
  public void testWildcardResourceAccessRules() {
    verifyResourceAccessRules(new Resource("Topic", "*", PatternType.LITERAL));
  }

  @Test
  public void testPrefixedResourceAccessRules() {
    verifyResourceAccessRules(new Resource("Topic", "top", PatternType.PREFIXED));
  }

  @Test
  public void testSingleCharPrefixedResourceAccessRules() {
    verifyResourceAccessRules(new Resource("Topic", "t", PatternType.PREFIXED));
  }

  @Test
  public void testFullNamePrefixedResourceAccessRules() {
    verifyResourceAccessRules(new Resource("Topic", "topic", PatternType.PREFIXED));
  }

  private void verifyResourceAccessRules(Resource roleResource) {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);
    Set<KafkaPrincipal> emptyGroups = Collections.emptySet();
    Set<Resource> resources = roleResource == null ?
        Collections.emptySet() : Collections.singleton(roleResource);

    updateRoleBinding(alice, "Reader", clusterA, resources);
    verifyRules(accessRules(alice, emptyGroups, Resource.CLUSTER));
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
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));

    String clusterB = "testOrg/clusterB";
    updateRoleBinding(alice, "Cluster Admin", clusterB, Collections.emptySet());
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic), "AlterConfigs", "DescribeConfigs");

  }

  @Test
  public void testProviderScope() throws Exception {
    initializeRbacProvider("testOrg");

    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    updateRoleBinding(alice, "Operator", clusterA, Collections.singleton(topic));
    verifyRules(rbacProvider.accessRules(alice, groups, clusterA, topic), "AlterConfigs", "DescribeConfigs");
    verifyRules(rbacProvider.accessRules(alice, groups, clusterA, Resource.CLUSTER));

    String clusterB = "testOrg/clusterB";
    updateRoleBinding(alice, "Cluster Admin", clusterB, Collections.emptySet());
    verifyRules(rbacProvider.accessRules(alice, groups, clusterA, Resource.CLUSTER));
    verifyRules(rbacProvider.accessRules(alice, groups, clusterA, topic), "AlterConfigs", "DescribeConfigs");
    verifyRules(rbacProvider.accessRules(alice, groups, clusterB, Resource.CLUSTER), "AlterConfigs", "DescribeConfigs");
    verifyRules(rbacProvider.accessRules(alice, groups, clusterB, topic));

    try {
      rbacProvider.accessRules(alice, groups, "anotherOrg/clusterA", Resource.CLUSTER);
      fail("Did not fail with invalid scope");
    } catch (InvalidScopeException e) {
      // Expected exception
    }
  }

  private void initializeRbacProvider(String scope) throws Exception {
    RbacRoles rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
    MockRbacProvider.MockAuthStore authStore = new MockRbacProvider.MockAuthStore(rbacRoles, new Scope(scope));
    authCache = authStore.authCache();
    rbacProvider = new RbacProvider() {
      @Override
      public void configure(Map<String, ?> configs) {
        KafkaTestUtils.setFinalField(rbacProvider, RbacProvider.class, "authCache", authCache);
      }
    };
    Map<String, Object> configs = Collections.singletonMap(ConfluentAuthorizerConfig.SCOPE_PROP, scope);
    rbacProvider.configure(configs);
  }

  private void updateRoleBinding(KafkaPrincipal principal, String role, String scope, Set<Resource> resources) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    RoleBindingValue value = new RoleBindingValue(resources == null ? Collections.emptySet() : resources);
    authCache.put(key, value);
  }

  private void deleteRoleBinding(KafkaPrincipal principal, String role, String scope) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    authCache.remove(key);
  }

  private Set<AccessRule> accessRules(KafkaPrincipal userPrincipal,
                                      Set<KafkaPrincipal> groupPrincipals,
                                      Resource resource) {
    return rbacProvider.accessRules(userPrincipal, groupPrincipals, clusterA, resource);
  }

  private void verifyRules(Set<AccessRule> rules, String... expectedOps) {
    Set<String> actualOps = rules.stream().map(r -> r.operation().name()).collect(Collectors.toSet());
    assertEquals(Utils.mkSet(expectedOps), actualOps);
  }
}

