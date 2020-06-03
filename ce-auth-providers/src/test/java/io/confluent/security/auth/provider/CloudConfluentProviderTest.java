// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider;

import static org.junit.Assert.assertFalse;

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

public class CloudConfluentProviderTest {

  private final Scope clusterA = new Scope.Builder("organization=123", "environment=t55").withKafkaCluster("lkc-a").build();
  private final Scope clusterB = new Scope.Builder("organization=123", "environment=t55").withKafkaCluster("lkc-b").build();
  private final ResourcePattern clusterResource = new ResourcePattern(new ResourceType("Cluster"), "kafka-cluster", PatternType.LITERAL);
  protected ConfluentProvider rbacProvider;
  protected DefaultAuthCache authCache;
  protected Optional<ConfluentAdmin> aclClientOp;
  private ResourcePattern topic = new ResourcePattern("Topic", "topicA", PatternType.LITERAL);

  @Before
  public void setUp() throws Exception {
    initializeRbacProvider("clusterA", clusterA,
            Collections.singletonMap(KafkaConfig.AuthorizerClassNameProp(),
                    ConfluentConfigs.MULTITENANT_AUTHORIZER_CLASS_NAME));
  }

  @After
  public void tearDown() {
    if (rbacProvider != null)
      rbacProvider.close();
  }

  @Test
  public void testOrgAdminAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    updateRoleBinding(alice, "OrgAdmin", clusterA, null);
    assertFalse(rbacProvider.isSuperUser(alice, clusterA));
    verifyRules(alice, groups, clusterA, clusterResource, "All");
    verifyRules(alice, groups, clusterA, topic, "All");

    // Delete non-existing role
    deleteRoleBinding(alice, "OrgAdmin", clusterB);
    assertFalse(rbacProvider.isSuperUser(alice, clusterA));

    deleteRoleBinding(alice, "OrgAdmin", clusterA);
    assertFalse(rbacProvider.isSuperUser(alice, clusterA));
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
