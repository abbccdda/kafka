// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.metadata.MetadataServiceConfig;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.RoleBindingKey;
import io.confluent.security.auth.store.data.RoleBindingValue;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.EmbeddedAuthorizer;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.rbac.RbacRoles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.ClusterResource;
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
    initializeRbacProvider("clusterA", clusterA, Collections.emptyMap());
  }

  @After
  public void tearDown() {
    if (rbacProvider != null)
      rbacProvider.close();
  }

  @Test
  public void testSystemAdminAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    updateRoleBinding(alice, "SystemAdmin", clusterA, null);
    assertFalse(rbacProvider.isSuperUser(alice, groups, clusterA));
    verifyRules(accessRules(alice, groups, clusterResource), "All");
    verifyRules(accessRules(alice, groups, topic), "All");

    // Delete non-existing role
    deleteRoleBinding(alice, "SystemAdmin", clusterB);
    assertFalse(rbacProvider.isSuperUser(alice, groups, clusterA));

    deleteRoleBinding(alice, "SystemAdmin", clusterA);
    assertFalse(rbacProvider.isSuperUser(alice, groups, clusterA));
  }

  @Test
  public void testSystemAdminGroupAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);

    updateRoleBinding(admin, "SystemAdmin", clusterA, Collections.emptySet());
    assertFalse(rbacProvider.isSuperUser(alice, groups, clusterA));
    verifyRules(accessRules(alice, groups, clusterResource), "All");
    verifyRules(accessRules(alice, groups, topic), "All");

    assertFalse(rbacProvider.isSuperUser(alice, Collections.emptySet(), clusterA));

    deleteRoleBinding(admin, "SystemAdmin", clusterA);
    assertFalse(rbacProvider.isSuperUser(alice, groups, clusterA));

  }

  @Test
  public void testRbacAuthorizerSuperUsers() throws Exception {
    KafkaPrincipal admin = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin");
    Scope metadataCluster = Scope.kafkaClusterScope("metadataCluster");
    Scope otherCluster = Scope.kafkaClusterScope("anotherCluster");
    Map<String, Object> configs = new HashMap<>();
    configs.put("super.users", admin.toString());
    configs.put(ConfluentAuthorizerConfig.METADATA_PROVIDER_PROP, "RBAC");
    configs.put(MetadataServiceConfig.METADATA_SERVER_LISTENERS_PROP, "http://127.0.0.1:8090");
    initializeRbacProvider("metadataCluster", Scope.ROOT_SCOPE, configs);
    EmbeddedAuthorizer authorizer = rbacProvider.createRbacAuthorizer();

    // Statically configured super users have access to security metadata in all clusters.
    // These users can also describe and alter access of any resource.
    // For the metadata service authorizer, these users are not granted access to any other resource.
    Operation alter = new Operation("Alter");
    Operation alterAccess = new Operation("AlterAccess");
    verifyAccess(authorizer, admin, metadataCluster, RbacProvider.SECURITY_METADATA, alter, AuthorizeResult.ALLOWED);
    verifyAccess(authorizer, admin, otherCluster, RbacProvider.SECURITY_METADATA, alter, AuthorizeResult.ALLOWED);
    verifyAccess(authorizer, admin, metadataCluster, topic.resourceType(), alter, AuthorizeResult.DENIED);
    verifyAccess(authorizer, admin, otherCluster, topic.resourceType(), alter, AuthorizeResult.DENIED);
    verifyAccess(authorizer, admin, metadataCluster, topic.resourceType(), alterAccess, AuthorizeResult.ALLOWED);
    verifyAccess(authorizer, admin, otherCluster, topic.resourceType(), alterAccess, AuthorizeResult.ALLOWED);

    // Super user roles have access to all resources within the role binding scope
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    updateRoleBinding(alice, "SystemAdmin", metadataCluster, Collections.emptySet());
    verifyAccess(authorizer, alice, metadataCluster, RbacProvider.SECURITY_METADATA, alter, AuthorizeResult.ALLOWED);
    verifyAccess(authorizer, alice, otherCluster, RbacProvider.SECURITY_METADATA, alter, AuthorizeResult.DENIED);
    verifyAccess(authorizer, alice, metadataCluster, topic.resourceType(), alter, AuthorizeResult.ALLOWED);
    verifyAccess(authorizer, alice, otherCluster, topic.resourceType(), alter, AuthorizeResult.DENIED);
    verifyAccess(authorizer, alice, metadataCluster, topic.resourceType(), alterAccess, AuthorizeResult.ALLOWED);
    verifyAccess(authorizer, alice, otherCluster, topic.resourceType(), alterAccess, AuthorizeResult.DENIED);
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
    initializeRbacProvider("clusterA", Scope.intermediateScope("testOrg"), Collections.emptyMap());

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

  private void initializeRbacProvider(String clusterId, Scope authStoreScope,  Map<String, ?> configs) throws Exception {
    RbacRoles rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
    MockRbacProvider.MockAuthStore authStore = new MockRbacProvider.MockAuthStore(rbacRoles, authStoreScope);
    authCache = authStore.authCache();
    rbacProvider = new RbacProvider() {
      @Override
      public void configure(Map<String, ?> configs) {
        super.configure(configs);
        KafkaTestUtils.setFinalField(rbacProvider, RbacProvider.class, "authCache", authCache);
      }
    };
    rbacProvider.onUpdate(new ClusterResource(clusterId));
    rbacProvider.configure(configs);
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

  private void verifyAccess(EmbeddedAuthorizer authorizer, KafkaPrincipal principal,
      Scope scope, ResourceType resourceType, Operation op, AuthorizeResult expectedResult) {
    Action action = new Action(scope, resourceType, "name", op);
    assertEquals(expectedResult,
        authorizer.authorize(principal, "localhost", Collections.singletonList(action)).get(0));
  }
}

