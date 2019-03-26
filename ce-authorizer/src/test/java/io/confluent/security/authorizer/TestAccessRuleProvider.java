// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class TestAccessRuleProvider implements AccessRuleProvider {

  private static final String SCOPE = "test";

  static RuntimeException exception;
  static Set<KafkaPrincipal> superUsers = new HashSet<>();
  static Map<Resource, Set<AccessRule>> accessRules = new HashMap<>();

  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public boolean isSuperUser(KafkaPrincipal sessionPrincipal,
                             Set<KafkaPrincipal> groupPrincipals,
                             String scope) {
    validate(scope);
    return superUsers.contains(sessionPrincipal) || groupPrincipals.stream().anyMatch(superUsers::contains);
  }

  @Override
  public Set<AccessRule> accessRules(KafkaPrincipal sessionPrincipal,
                                     Set<KafkaPrincipal> groupPrincipals,
                                     String scope,
                                     Resource resource) {

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
    return false;
  }

  @Override
  public String providerName() {
    return "TEST";
  }

  @Override
  public void close() {
  }

  private void validate(String scope) {
    if (exception != null)
      throw exception;
    if (!scope.startsWith(SCOPE))
      throw new InvalidScopeException("Unknown scope " + scope);
  }

  static void reset() {
    exception = null;
    accessRules.clear();
    superUsers.clear();
  }
}
