// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.security.auth.provider;


import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.provider.rbac.MockRbacProvider.MockAuthStore;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.RoleBindingKey;
import io.confluent.security.auth.store.data.RoleBindingValue;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.test.utils.RbacTestUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.server.authorizer.internals.ConfluentAuthorizerServerInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This test class exercises two broad things:
 *
 * - Are the role definitions for the cloud roles correct individually (test*AdminAccessRules).
 *   They check both positive and negative permissions on each of the items in our model hierarchy
 *
 * - Do the roles interact correctly (testInteraction*)
 *   We want to be sure that combinations of roles either laterally or hierarchically have
 *   the expected effects, particularly that they stay correct if some roles are deleted
 *
 *   These tests use a simple hierarchy:
 *
 *   - organization=123
 *       - environment=t55
 *           - lkc-a
 *           - lkc-b
 *       - environment=t66
 *           - lkc-c
 *   - organization=789
 *
 *   and they also test against "invalid" elements:
 *   - organization=789
 *       - environment=t55
 *           - lkc-b
 *
 *   In our model environment=t55 "actually" belongs to organization=123, but here we're
 *   simulating an error or attack in which the "wrong" scope path is supplied and verifying
 *   that erroneous access is not allowed
 */
public class CloudConfluentProviderTest {

  private final KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
  private final Set<KafkaPrincipal> groups = Collections.emptySet(); // Groups not supported in Cloud yet
  private final Scope clusterA = new Scope.Builder("organization=123", "environment=t55").withKafkaCluster("lkc-a").build();
  private final Scope clusterB = new Scope.Builder("organization=123", "environment=t55").withKafkaCluster("lkc-b").build();
  private final Scope clusterC = new Scope.Builder("organization=123", "environment=t66").withKafkaCluster("lkc-c").build();
  private final Scope environmentT55 = new Scope.Builder("organization=123", "environment=t55").build();
  private final Scope environmentT66 = new Scope.Builder("organization=123", "environment=t66").build();
  private final Scope organization123 = new Scope.Builder("organization=123").build();
  private final Scope organization789 = new Scope.Builder("organization=789").build();
  // environment t55 should *not* appear in both organizations 123 and 789. But we should not allow even if it does
  private final Scope wrongEnvironmentT55 = new Scope.Builder("organization=789", "environment=t55").build();
  private final Scope wrongClusterB = new Scope.Builder("organization=789", "environment=t55").withKafkaCluster("lkc-b").build();
  private final ResourcePattern clusterResource = new ResourcePattern(new ResourceType("Cluster"), "kafka-cluster", PatternType.LITERAL);
  private final ResourcePattern topic = new ResourcePattern("Topic", "topicA", PatternType.LITERAL);
  private final ResourcePattern user = new ResourcePattern("User", "789", PatternType.LITERAL);
  private final ResourcePattern envT55 = new ResourcePattern("Environment", "t55", PatternType.LITERAL);
  private final ResourcePattern envT66 = new ResourcePattern("Environment", "t66", PatternType.LITERAL);
  private final ResourcePattern org = new ResourcePattern("Organization", "123", PatternType.LITERAL);

  private ConfluentProvider rbacProvider;
  private DefaultAuthCache authCache;
  private Optional<ConfluentAdmin> aclClientOp;

  @Before
  public void setUp() throws Exception {
    initializeRbacProvider("clusterA", Scope.ROOT_SCOPE,
            Collections.singletonMap(KafkaConfig.AuthorizerClassNameProp(),
                    ConfluentConfigs.MULTITENANT_AUTHORIZER_CLASS_NAME));
  }

  @After
  public void tearDown() {
    if (rbacProvider != null)
      rbacProvider.close();
  }

  @Test
  public void testRoleBindingAdminAccessRules() {
    updateRoleBinding(alice, "CCloudRoleBindingAdmin", Scope.ROOT_SCOPE, null);

    verifyRules(alice, groups, organization123, org, "DescribeAccess", "AlterAccess");
    verifyRules(alice, groups, organization123, user);
    verifyRules(alice, groups, organization789, org, "DescribeAccess", "AlterAccess");

    verifyRules(alice, groups, environmentT55, envT55);

    verifyRules(alice, groups, clusterA, clusterResource);
    verifyRules(alice, groups, clusterA, topic);

    verifyRules(alice, groups, clusterB, clusterResource);
    verifyRules(alice, groups, clusterB, topic);

    verifyRules(alice, groups, clusterC, clusterResource);
    verifyRules(alice, groups, clusterC, topic);

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, clusterResource);

    deleteRoleBinding(alice, "CCloudRoleBindingAdmin", Scope.ROOT_SCOPE);
  }

  private void verifyOrgAdmin() {
    verifyRules(alice, groups, organization123, org, "Alter", "CreateEnvironment", "DescribeAccess", "AlterAccess");
    verifyRules(alice, groups, organization123, user, "All");
    verifyRules(alice, groups, organization789, org);

    verifyRules(alice, groups, environmentT55, envT55, "All");
    verifyRules(alice, groups, environmentT66, envT66, "All");

    verifyRules(alice, groups, clusterA, clusterResource, "All");
    verifyRules(alice, groups, clusterA, topic, "All");

    verifyRules(alice, groups, clusterB, clusterResource, "All");
    verifyRules(alice, groups, clusterB, topic, "All");

    verifyRules(alice, groups, clusterC, clusterResource, "All");
    verifyRules(alice, groups, clusterC, topic, "All");

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, clusterResource);
  }

  private void verifyNoPermissions() {
    verifyRules(alice, groups, organization123, org);
    verifyRules(alice, groups, organization123, user);
    verifyRules(alice, groups, organization789, org);

    verifyRules(alice, groups, environmentT55, envT55);
    verifyRules(alice, groups, environmentT66, envT66);

    verifyRules(alice, groups, clusterA, clusterResource);
    verifyRules(alice, groups, clusterA, topic);

    verifyRules(alice, groups, clusterB, clusterResource);
    verifyRules(alice, groups, clusterB, topic);

    verifyRules(alice, groups, clusterC, clusterResource);
    verifyRules(alice, groups, clusterC, topic);

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, clusterResource);
  }
  
  @Test
  public void testOrgAdminAccessRules() {
    updateRoleBinding(alice, "OrganizationAdmin", organization123, null);

    verifyOrgAdmin();

    deleteRoleBinding(alice, "OrganizationAdmin", organization123);

    verifyNoPermissions();
  }

  private void verifyEnvAdminT55() {
    verifyRules(alice, groups, organization123, org);
    verifyRules(alice, groups, organization123, user, "Invite", "Describe");
    verifyRules(alice, groups, organization789, org);

    verifyRules(alice, groups, environmentT55, envT55,
            "Alter", "Delete", "CreateKafkaCluster", "DescribeAccess", "AlterAccess");
    verifyRules(alice, groups, environmentT66, envT66);

    verifyRules(alice, groups, clusterA, clusterResource, "All");
    verifyRules(alice, groups, clusterA, topic, "All");

    verifyRules(alice, groups, clusterB, clusterResource, "All");
    verifyRules(alice, groups, clusterB, topic, "All");

    verifyRules(alice, groups, clusterC, clusterResource);
    verifyRules(alice, groups, clusterC, topic);

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, clusterResource);
  }

  @Test
  public void testEnvAdminAccessRules() {
    updateRoleBinding(alice, "EnvironmentAdmin", environmentT55, null);

    verifyEnvAdminT55();

    deleteRoleBinding(alice, "EnvironmentAdmin", environmentT55);

    verifyNoPermissions();
  }

  private void verifyCloudClusterAdminA() {
    verifyRules(alice, groups, organization123, org);
    verifyRules(alice, groups, organization123, user, "Invite", "Describe");
    verifyRules(alice, groups, organization789, org);

    verifyRules(alice, groups, environmentT55, envT55);
    verifyRules(alice, groups, environmentT55, envT66);

    verifyRules(alice, groups, clusterA, clusterResource, "All");
    verifyRules(alice, groups, clusterA, topic, "All");

    verifyRules(alice, groups, clusterB, clusterResource);
    verifyRules(alice, groups, clusterB, topic);

    verifyRules(alice, groups, clusterC, clusterResource);
    verifyRules(alice, groups, clusterC, topic);

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, clusterResource);
  }

  @Test
  public void testCloudClusterAdminAccessRules() {
    updateRoleBinding(alice, "CloudClusterAdmin", clusterA, null);

    verifyCloudClusterAdminA();

    deleteRoleBinding(alice, "CloudClusterAdmin", clusterA);

    verifyNoPermissions();
  }

  @Test
  public void testInteractionMultipleEnvironments() {
    updateRoleBinding(alice, "EnvironmentAdmin", environmentT55, null);
    updateRoleBinding(alice, "EnvironmentAdmin", environmentT66, null);

    verifyRules(alice, groups, organization123, user, "Invite", "Describe");
    verifyRules(alice, groups, organization789, org);

    verifyRules(alice, groups, environmentT55, envT55,
            "Alter", "Delete", "CreateKafkaCluster", "DescribeAccess", "AlterAccess");
    verifyRules(alice, groups, environmentT66, envT66,
            "Alter", "Delete", "CreateKafkaCluster", "DescribeAccess", "AlterAccess");

    verifyRules(alice, groups, clusterA, clusterResource, "All");
    verifyRules(alice, groups, clusterA, topic, "All");

    verifyRules(alice, groups, clusterB, clusterResource, "All");
    verifyRules(alice, groups, clusterB, topic, "All");

    verifyRules(alice, groups, clusterC, clusterResource, "All");
    verifyRules(alice, groups, clusterC, topic, "All");

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, clusterResource);

    deleteRoleBinding(alice, "EnvironmentAdmin", environmentT55);

    // still have our org permissions
    verifyRules(alice, groups, organization123, user, "Invite", "Describe");

    verifyRules(alice, groups, environmentT55, envT55);
    verifyRules(alice, groups, environmentT66, envT66,
            "Alter", "Delete", "CreateKafkaCluster", "DescribeAccess", "AlterAccess");

    verifyRules(alice, groups, clusterA, clusterResource);
    verifyRules(alice, groups, clusterA, topic);

    verifyRules(alice, groups, clusterB, clusterResource);
    verifyRules(alice, groups, clusterB, topic);

    verifyRules(alice, groups, clusterC, clusterResource, "All");
    verifyRules(alice, groups, clusterC, topic, "All");

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, clusterResource);

    deleteRoleBinding(alice, "EnvironmentAdmin", environmentT66);

    verifyNoPermissions();
  }

  @Test
  public void testInteractionDifferentLevels() {
    updateRoleBinding(alice, "OrganizationAdmin", organization123, null);
    updateRoleBinding(alice, "EnvironmentAdmin", environmentT55, null);
    updateRoleBinding(alice, "CloudClusterAdmin", clusterA, null);

    verifyOrgAdmin();

    deleteRoleBinding(alice, "OrganizationAdmin", organization123);

    verifyEnvAdminT55();

    deleteRoleBinding(alice, "EnvironmentAdmin", environmentT55);

    verifyCloudClusterAdminA();
  }

  @Test
  public void testInteractionDifferentLevelsDeleteInner() {
    updateRoleBinding(alice, "OrganizationAdmin", organization123, null);
    updateRoleBinding(alice, "EnvironmentAdmin", environmentT55, null);
    updateRoleBinding(alice, "CloudClusterAdmin", clusterA, null);

    verifyOrgAdmin();

    deleteRoleBinding(alice, "CloudClusterAdmin", clusterA);

    verifyOrgAdmin();

    deleteRoleBinding(alice, "EnvironmentAdmin", environmentT55);

    verifyOrgAdmin();
  }

  protected void initializeRbacProvider(String clusterId, Scope authStoreScope, Map<String, ?> configs) throws Exception {
    RbacRoles rbacRoles = RbacRoles.loadDefaultPolicy(true);
    MockAuthStore authStore = new MockAuthStore(rbacRoles, authStoreScope);
    authCache = authStore.authCache();
    List<Node> nodes = Collections.singletonList(new Node(1, "localhost", 9092));
    aclClientOp = Optional.of(new MockAdminClient(nodes, nodes.get(0)));
    rbacProvider = new ConfluentProvider() {
      @Override
      public void configure(Map<String, ?> configs) {
        super.configure(configs);
        KafkaTestUtils.setField(rbacProvider, ConfluentProvider.class, "authCache", authCache);
      }

      @Override
      protected ConfluentAdmin createMdsAdminClient(AuthorizerServerInfo serverInfo, Map<String, ?> clientConfigs) {
        return aclClientOp.get();
      }

      @Override
      protected AuthStore createAuthStore(Scope scope, ConfluentAuthorizerServerInfo serverInfo, Map<String, ?> configs) {
        return new MockAuthStore(RbacRoles.loadDefaultPolicy(isConfluentCloud()), scope);
      }
    };
    rbacProvider.onUpdate(new ClusterResource(clusterId));
    rbacProvider.configure(configs);
    rbacProvider.setKafkaMetrics(new Metrics());
  }

  protected void updateRoleBinding(KafkaPrincipal principal, String role, Scope scope, Set<ResourcePattern> resources) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    RoleBindingValue value = new RoleBindingValue(resources == null ? Collections.emptySet() : resources);
    authCache.put(key, value);
  }

  protected void deleteRoleBinding(KafkaPrincipal principal, String role, Scope scope) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    authCache.remove(key);
  }

  protected void verifyRules(KafkaPrincipal userPrincipal,
                             Set<KafkaPrincipal> groupPrincipals,
                             Scope scope,
                             ResourcePattern resource,
                             String... expectedOps) {
    RbacTestUtils.verifyPermissions(rbacProvider, userPrincipal, groupPrincipals, scope, resource, expectedOps);
  }

}
