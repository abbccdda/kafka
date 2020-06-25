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
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.server.authorizer.internals.ConfluentAuthorizerServerInfo;

public abstract class CloudBaseProviderTest {

    protected final KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    protected final Set<KafkaPrincipal> groups = Collections.emptySet(); // Groups not supported in Cloud yet
    protected final Scope clusterA = new Scope.Builder("organization=123", "environment=t55", "cloud-cluster=lkc-a").build();
    protected final Scope kafkaClusterA = new Scope.Builder("organization=123", "environment=t55", "cloud-cluster=lkc-a").withKafkaCluster("lkc-a").build();
    protected final Scope clusterB = new Scope.Builder("organization=123", "environment=t55", "cloud-cluster=lkc-b").build();
    protected final Scope kafkaClusterB = new Scope.Builder("organization=123", "environment=t55", "cloud-cluster=lkc-b").withKafkaCluster("lkc-b").build();
    protected final Scope clusterC = new Scope.Builder("organization=123", "environment=t66", "cloud-cluster=lkc-c").build();
    protected final Scope kafkaClusterC = new Scope.Builder("organization=123", "environment=t66", "cloud-cluster=lkc-c").withKafkaCluster("lkc-c").build();
    protected final Scope environmentT55 = new Scope.Builder("organization=123", "environment=t55").build();
    protected final Scope environmentT66 = new Scope.Builder("organization=123", "environment=t66").build();
    protected final Scope organization123 = new Scope.Builder("organization=123").build();
    protected final Scope organization789 = new Scope.Builder("organization=789").build();
    // environment t55 should *not* appear in both organizations 123 and 789. But we should not allow even if it does
    protected final Scope wrongEnvironmentT55 = new Scope.Builder("organization=789", "environment=t55").build();
    protected final Scope wrongClusterB = new Scope.Builder("organization=789", "environment=t55", "cloud-cluster=lkc-b").build();
    protected final ResourcePattern cloudClusterResource = new ResourcePattern(new ResourceType("CloudCluster"), "cloud-cluster", PatternType.LITERAL);
    protected final ResourcePattern clusterResource = new ResourcePattern(new ResourceType("Cluster"), "kafka-cluster", PatternType.LITERAL);
    protected final ResourcePattern topic = new ResourcePattern("Topic", "topicA", PatternType.LITERAL);
    protected final ResourcePattern topicB = new ResourcePattern("Topic", "topicB", PatternType.LITERAL);
    protected final ResourcePattern user = new ResourcePattern("User", "789", PatternType.LITERAL);
    protected final ResourcePattern envT55 = new ResourcePattern("Environment", "t55", PatternType.LITERAL);
    protected final ResourcePattern envT66 = new ResourcePattern("Environment", "t66", PatternType.LITERAL);
    protected final ResourcePattern org = new ResourcePattern("Organization", "123", PatternType.LITERAL);

    protected ConfluentProvider rbacProvider;
    protected DefaultAuthCache authCache;
    protected Optional<ConfluentAdmin> aclClientOp;

    protected void initializeRbacProvider(String clusterId, Scope authStoreScope, Map<String, ?> configs, RbacRoles rbacRoles) throws Exception {
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