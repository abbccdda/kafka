// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.Auditable;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.authorizer.provider.AuthorizeRule;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.internals.ConfluentAuthorizerServerInfo;
import org.apache.kafka.server.audit.AuditLogProvider;

public class TestAccessRuleProvider implements AccessRuleProvider, Auditable {

  private static final String SCOPE = "test";

  static RuntimeException exception;
  static CompletionStage<Void> startFuture = CompletableFuture.completedFuture(null);
  static boolean usesMetadataFromThisKafkaCluster = false;
  static Set<KafkaPrincipal> superUsers = new HashSet<>();
  static Map<ResourcePattern, Set<AccessRule>> accessRules = new HashMap<>();
  static AuditLogProvider auditLogProvider;

  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public CompletionStage<Void> start(ConfluentAuthorizerServerInfo serverInfo, Map<String, ?> interBrokerListenerConfigs) {
    return startFuture;
  }


  @Override
  public boolean isSuperUser(KafkaPrincipal principal,
                             Scope scope) {
    validate(scope);
    return superUsers.contains(principal);
  }

  @Override
  public AuthorizeRule findRule(KafkaPrincipal sessionPrincipal,
                                Set<KafkaPrincipal> groupPrincipals,
                                String host,
                                Action action) {
    validate(action.scope());
    Set<AccessRule> rules = accessRules.getOrDefault(action.resourcePattern(), Collections.emptySet());
    Operation op = action.operation();
    Set<KafkaPrincipal> principals = AccessRule.matchingPrincipals(sessionPrincipal, groupPrincipals,
        AccessRule.WILDCARD_USER_PRINCIPAL, AccessRule.WILDCARD_GROUP_PRINCIPAL);
    AuthorizeRule authorizeRule = new AuthorizeRule();
    rules.stream().filter(rule -> rule.matches(principals, host, op, PermissionType.DENY))
        .findAny().ifPresent(authorizeRule::addRuleIfNotExist);
    rules.stream().filter(rule -> rule.matches(principals, host, op, PermissionType.ALLOW))
        .findAny().ifPresent(authorizeRule::addRuleIfNotExist);
    authorizeRule.noResourceAcls(rules.isEmpty());
    return authorizeRule;
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

  @Override
  public void auditLogProvider(AuditLogProvider auditLogProvider) {
    TestAccessRuleProvider.auditLogProvider = auditLogProvider;
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
    auditLogProvider = null;
  }
}
