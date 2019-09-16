// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class TestAccessRuleProvider implements AccessRuleProvider {

  private static final String SCOPE = "test";

  static RuntimeException exception;
  static CompletionStage<Void> startFuture = CompletableFuture.completedFuture(null);
  static boolean usesMetadataFromThisKafkaCluster = false;
  static Set<KafkaPrincipal> superUsers = new HashSet<>();
  static Map<ResourcePattern, Set<AccessRule>> accessRules = new HashMap<>();

  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public CompletionStage<Void> start(Map<String, ?> interBrokerListenerConfigs) {
    return startFuture;
  }


  @Override
  public boolean isSuperUser(KafkaPrincipal principal,
                             Scope scope) {
    validate(scope);
    return superUsers.contains(principal);
  }

  @Override
  public Set<AccessRule> accessRules(KafkaPrincipal sessionPrincipal,
                                     Set<KafkaPrincipal> groupPrincipals,
                                     Scope scope,
                                     ResourcePattern resource) {

    validate(scope);
    Set<KafkaPrincipal> principals = new HashSet<>(groupPrincipals.size() + 1);
    principals.add(sessionPrincipal);
    principals.addAll(groupPrincipals);
    return accessRules.getOrDefault(resource, Collections.emptySet()).stream()
        .filter(rule -> principals.contains(rule.principal()))
        .collect(Collectors.toSet());
  }

  @Override
  public boolean mayDeny() {
    return false;
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    return usesMetadataFromThisKafkaCluster;
  }

  @Override
  public String providerName() {
    return "TEST";
  }

  @Override
  public void close() {
  }

  private void validate(Scope scope) {
    if (exception != null)
      throw exception;
    if (!scope.clusters().values().iterator().next().startsWith(SCOPE))
      throw new InvalidScopeException("Unknown scope " + scope);
  }

  static void reset() {
    exception = null;
    startFuture = CompletableFuture.completedFuture(null);
    usesMetadataFromThisKafkaCluster = false;
    accessRules.clear();
    superUsers.clear();
  }
}
