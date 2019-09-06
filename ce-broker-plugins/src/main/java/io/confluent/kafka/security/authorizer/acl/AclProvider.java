// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer.acl;

import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import kafka.security.auth.ResourceType;
import kafka.security.authorizer.AclAuthorizer;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

public class AclProvider extends AclAuthorizer implements AccessRuleProvider {

  private static final Logger log = LoggerFactory.getLogger("kafka.authorizer.logger");

  @Override
  public String providerName() {
    return AccessRuleProviders.ACL.name();
  }

  @Override
  public boolean isSuperUser(KafkaPrincipal sessionPrincipal,
                             Set<KafkaPrincipal> groupPrincipals,
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
    ResourceType resourceType = AclMapper.kafkaResourceType(resource.resourceType());
    KafkaPrincipal userPrincipal = userPrincipal(sessionPrincipal);
    return JavaConversions.setAsJavaSet(matchingAcls(resourceType, resource.name())).stream()
        .map(AclMapper::accessRule)
        .filter(acl -> userOrGroupAcl(acl, userPrincipal, groupPrincipals))
        .collect(Collectors.toSet());
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


  @Override
  public boolean needsLicense() {
    return false;
  }

  private KafkaPrincipal userPrincipal(KafkaPrincipal sessionPrincipal) {
    // Always use KafkaPrincipal instance for comparisons since super.users and ACLs are
    // instantiated as KafkaPrincipal
    return sessionPrincipal.getClass() != KafkaPrincipal.class
        ? new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName())
        : sessionPrincipal;
  }

  private boolean userOrGroupAcl(AccessRule rule,
                                 KafkaPrincipal userPrincipal,
                                 Set<KafkaPrincipal> groupPrincipals) {
    KafkaPrincipal aclPrincipal = rule.principal();
    return aclPrincipal.equals(userPrincipal) ||
        aclPrincipal.equals(AccessRule.WILDCARD_USER_PRINCIPAL) ||
        groupPrincipals.contains(aclPrincipal) ||
        (!groupPrincipals.isEmpty() && aclPrincipal.equals(AccessRule.WILDCARD_GROUP_PRINCIPAL));
  }
}
