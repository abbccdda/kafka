// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.test.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import io.confluent.security.auth.metadata.AuthCache;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.AclBindingKey;
import io.confluent.security.auth.store.data.AclBindingValue;
import io.confluent.security.auth.store.data.RoleBindingKey;
import io.confluent.security.auth.store.data.RoleBindingValue;
import io.confluent.security.auth.store.data.UserKey;
import io.confluent.security.auth.store.data.UserValue;
import io.confluent.security.auth.store.kafka.KafkaAuthStore;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.PermissionType;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.acl.AclRule;
import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.AuthorizeRule;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import kafka.metrics.KafkaYammerMetrics;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

public class RbacTestUtils {

  public static void updateRoleBinding(DefaultAuthCache authCache,
                                       KafkaPrincipal principal,
                                       String role,
                                       Scope scope,
                                       Set<ResourcePattern> resources) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    RoleBindingValue value = new RoleBindingValue(resources == null ? Collections.emptySet() : resources);
    authCache.put(key, value);
  }

  public static void deleteRoleBinding(DefaultAuthCache authCache,
                                       KafkaPrincipal principal,
                                       String role,
                                       Scope scope) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    authCache.remove(key);
  }

  public static void updateAclBinding(DefaultAuthCache authCache,
                                      ResourcePattern resourcePattern,
                                      Scope scope,
                                      Set<AclRule> accessRules) {
    AclBindingKey key = new AclBindingKey(resourcePattern, scope);
    AclBindingValue value = new AclBindingValue(accessRules == null ? Collections.emptySet() : accessRules);
    authCache.put(key, value);
  }

  public static void deleteAclBinding(DefaultAuthCache authCache,
                                      ResourcePattern resourcePattern,
                                      Scope scope) {
    AclBindingKey key = new AclBindingKey(resourcePattern, scope);
    authCache.remove(key);
  }

  public static void updateUser(DefaultAuthCache authCache, KafkaPrincipal user, Collection<KafkaPrincipal> groups) {
    authCache.put(new UserKey(user), new UserValue(groups));
  }

  public static void deleteUser(DefaultAuthCache authCache, KafkaPrincipal user) {
    authCache.remove(new UserKey(user));
  }

  public static Cluster mockCluster(int numNodes) {
    Node[] nodes = new Node[numNodes];
    for (int i = 0; i < numNodes; i++)
      nodes[i] = new Node(i, "host" + i, 9092);
    Node[] replicas = numNodes == 1 ? new Node[]{nodes[0]} : new Node[]{nodes[0], nodes[1]};
    PartitionInfo partitionInfo = new PartitionInfo(KafkaAuthStore.AUTH_TOPIC, 0,
        nodes[0], replicas, replicas);
    return new Cluster("cluster", Utils.mkSet(nodes),
        Collections.singleton(partitionInfo), Collections.emptySet(), Collections.emptySet());
  }

  public static <K, V> MockConsumer<K, V> mockConsumer(Cluster cluster, int numAuthTopicPartitions) {
    Node node = cluster.nodes().get(0);
    List<PartitionInfo> partitionInfos = new ArrayList<>(numAuthTopicPartitions);
    Map<TopicPartition, Long> offsets = new HashMap<>(numAuthTopicPartitions);

    for (int i = 0; i < numAuthTopicPartitions; i++) {
      partitionInfos.add(new PartitionInfo(KafkaAuthStore.AUTH_TOPIC,
          i,
          node,
          new Node[]{node},
          new Node[]{node}));
      offsets.put(new TopicPartition(KafkaAuthStore.AUTH_TOPIC, i), 0L);
    }

    MockConsumer<K, V> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.updatePartitions(KafkaAuthStore.AUTH_TOPIC, partitionInfos);
    consumer.updateBeginningOffsets(offsets);
    consumer.updateEndOffsets(offsets);
    return consumer;
  }

  public static void verifyMetric(String name, String type, long min, long max) {
    Set<Metric> metrics = metrics(name, type);
    assertFalse(metrics.isEmpty());
    long value = metrics.stream().mapToLong(RbacTestUtils::metricValue).sum();
    assertTrue("Unexpected value: " + value, value >= min && value <= max);
  }

  private static Set<Metric> metrics(String name, String type) {
    return KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
        .filter(e -> e.getKey().getName().equals(name) && e.getKey().getType().equals(type))
        .map(Map.Entry::getValue)
        .collect(Collectors.toSet());
  }

  @SuppressWarnings("unchecked")
  private static long metricValue(Metric metric) {
    if (metric instanceof Gauge) {
      return ((Gauge<Number>) metric).value().longValue();
    } else if (metric instanceof Meter) {
      return ((Meter) metric).count();
    } else
      throw new IllegalArgumentException("Metric value could not be computed for " + metric);
  }

  public static void verifyMetadataStoreMetrics() throws Exception {
    RbacTestUtils.verifyMetric("reader-failure-start-seconds-ago", "KafkaAuthStore", 0, 0);
    RbacTestUtils.verifyMetric("remote-failure-start-seconds-ago", "KafkaAuthStore", 0, 0);
    RbacTestUtils.verifyMetric("writer-failure-start-seconds-ago", "KafkaAuthStore", 0, 0);
    RbacTestUtils.verifyMetric("active-writer-count", "KafkaAuthStore", 0, 1);
    TestUtils.waitForCondition(() -> {
      Set<Metric> metrics = metrics("active-writer-count", "KafkaAuthStore");
      return metrics.stream().mapToLong(RbacTestUtils::metricValue).sum() == 1;
    }, "Writer not elected within timeout");
  }

  public static void verifyPermissions(AccessRuleProvider provider,
                                       KafkaPrincipal userPrincipal,
                                       Set<KafkaPrincipal> groupPrincipals,
                                       Scope scope,
                                       ResourcePattern resource,
                                       String... expectedOps) {
    verifyPermissions(new ProviderRuleMatcher(provider), userPrincipal, groupPrincipals, scope, resource, expectedOps);
  }

  public static void verifyPermissions(AuthCache authCache,
                                       KafkaPrincipal userPrincipal,
                                       Set<KafkaPrincipal> groupPrincipals,
                                       Scope scope,
                                       ResourcePattern resource,
                                       String... expectedOps) {
    verifyPermissions(new AuthCacheRuleMatcher(authCache), userPrincipal, groupPrincipals, scope, resource, expectedOps);
  }

  private static void verifyPermissions(RuleMatcher ruleMatcher,
                                        KafkaPrincipal userPrincipal,
                                        Set<KafkaPrincipal> groupPrincipals,
                                        Scope scope,
                                        ResourcePattern resource,
                                        String... expectedOps) {
    List<String> allOps = Arrays.asList("Create", "Delete", "Read", "Write", "Describe", "DescribeConfigs", "AlterConfigs");
    Set<String> allowedOps = Utils.mkSet(expectedOps);

    for (String op: allOps) {
      Action action = new Action(scope, resource, new Operation(op));
      AuthorizeRule rule = ruleMatcher.findRule(userPrincipal, groupPrincipals, action);
      assertFalse("Deny rule not expected for " + op, rule.deny());
      Operation operation = new Operation(op);
      if (allowedOps.contains("All")) {
        assertTrue("Expected allow for " + op, rule.allowRule().isPresent());
      } else if (allowedOps.stream().anyMatch(allowedOp -> new Operation(allowedOp).matches(operation, PermissionType.ALLOW))) {
        assertTrue("Expected allow for " + op, rule.allowRule().isPresent());
        assertTrue("Unexpected rule for " + op, allowedOps.contains(rule.allowRule().get().operation().name()));
      } else
        assertFalse("Unexpected allow for " + op, rule.allowRule().isPresent());
    }
  }

  private interface RuleMatcher {
    AuthorizeRule findRule(KafkaPrincipal userPrincipal,
                           Set<KafkaPrincipal> groupPrincipals,
                           Action action);
  }

  private static final class ProviderRuleMatcher implements  RuleMatcher {

    private final AccessRuleProvider provider;
    ProviderRuleMatcher(AccessRuleProvider provider) {
      this.provider = provider;
    }

    @Override
    public AuthorizeRule findRule(KafkaPrincipal userPrincipal,
                                  Set<KafkaPrincipal> groupPrincipals,
                                  Action action) {
      return provider.findRule(userPrincipal, groupPrincipals, "", action);
    }
  }

  private static final class AuthCacheRuleMatcher implements  RuleMatcher {

    private final AuthCache authCache;
    AuthCacheRuleMatcher(AuthCache authCache) {
      this.authCache = authCache;
    }

    @Override
    public AuthorizeRule findRule(KafkaPrincipal userPrincipal,
                                  Set<KafkaPrincipal> groupPrincipals,
                                  Action action) {
      return authCache.findRule(userPrincipal, groupPrincipals, "", action);
    }
  }
}
