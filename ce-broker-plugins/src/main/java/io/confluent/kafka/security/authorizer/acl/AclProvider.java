// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer.acl;

import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import kafka.security.authorizer.AclAuthorizer;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class AclProvider extends AclAuthorizer implements AccessRuleProvider {

  private static final Logger log = LoggerFactory.getLogger("kafka.authorizer.logger");

  @Override
  public String providerName() {
    return AccessRuleProviders.ZK_ACL.name();
  }

  @Override
  public boolean isSuperUser(KafkaPrincipal principal,
                             Scope scope) {
    // `super.users` config is checked by the authorizer before checking with providers
    return false;
  }

  @Override
  public boolean mayDeny() {
    return true;
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    return false;
  }

  @Override
  public Set<AccessRule> accessRules(KafkaPrincipal sessionPrincipal,
                                     Set<KafkaPrincipal> groupPrincipals,
                                     Scope scope,
                                     ResourcePattern resource) {
    ResourceType resourceType = SecurityUtils.resourceType(resource.resourceType().name());
    KafkaPrincipal userPrincipal = userPrincipal(sessionPrincipal);
    return matchingAcls(resourceType, resource.name())
        .filterAndTransform(p -> userOrGroupAcl(p, userPrincipal, groupPrincipals), AclMapper::accessRule);
  }

  @Override
  public Map<Endpoint, CompletableFuture<Void>> start(AuthorizerServerInfo serverInfo) {
    return Collections.emptyMap();
  }

  @Override
  public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext,
                                             List<Action> actions) {
    throw new IllegalStateException("This provider should be used for authorization only using the AccessRuleProvider interface");
  }

  private KafkaPrincipal userPrincipal(KafkaPrincipal sessionPrincipal) {
    // Always use KafkaPrincipal instance for comparisons since super.users and ACLs are
    // instantiated as KafkaPrincipal
    return sessionPrincipal.getClass() != KafkaPrincipal.class
        ? new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName())
        : sessionPrincipal;
  }

  private boolean userOrGroupAcl(KafkaPrincipal aclPrincipal,
                                 KafkaPrincipal userPrincipal,
                                 Set<KafkaPrincipal> groupPrincipals) {
    return aclPrincipal.equals(userPrincipal) ||
        aclPrincipal.equals(AccessRule.WILDCARD_USER_PRINCIPAL) ||
        groupPrincipals.contains(aclPrincipal) ||
        (!groupPrincipals.isEmpty() && aclPrincipal.equals(AccessRule.WILDCARD_GROUP_PRINCIPAL));
  }
}
