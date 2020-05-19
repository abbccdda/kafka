// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer.acl;

import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.PermissionType;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.security.authorizer.provider.AuthorizeRule;
import kafka.security.authorizer.AclAuthorizer;
import kafka.security.authorizer.AclEntry;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class AclProvider extends AclAuthorizer implements AccessRuleProvider {

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
  public AuthorizeRule findRule(KafkaPrincipal sessionPrincipal,
                                Set<KafkaPrincipal> groupPrincipals,
                                String host,
                                io.confluent.security.authorizer.Action action) {
    return findRule(sessionPrincipal, groupPrincipals,
        AccessRule.WILDCARD_USER_PRINCIPAL, AccessRule.WILDCARD_GROUP_PRINCIPAL,
        host, action);

  }

  protected AuthorizeRule findRule(KafkaPrincipal sessionPrincipal,
                                   Set<KafkaPrincipal> groupPrincipals,
                                   KafkaPrincipal wildcardUserPrincipal,
                                   KafkaPrincipal wilcardGroupPrincipal,
                                   String host,
                                   io.confluent.security.authorizer.Action action) {
    ResourcePattern resource = action.resourcePattern();
    ResourceType resourceType = SecurityUtils.resourceType(resource.resourceType().name());
    KafkaPrincipal userPrincipal = userPrincipal(sessionPrincipal);
    AclSeqs acls = matchingAcls(resourceType, resource.name());

    Set<KafkaPrincipal> matchingPrincipals = AccessRule.matchingPrincipals(userPrincipal,
        groupPrincipals, wildcardUserPrincipal, wilcardGroupPrincipal);
    AuthorizeRule authorizeRule = new AuthorizeRule();
    authorizeRule.noResourceAcls(acls.isEmpty());
    acls.find(aclEntry -> updateMatchingAcl(aclEntry, matchingPrincipals, host, action.operation(), authorizeRule));
    return authorizeRule;
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

  /**
   * Return `true` if a matching `DENY` rule has been found and `false` otherwise. We do not
   * return `true` when a matching `ALLOW` is found as a `DENY` rule may take precedence (this
   * could be improved if we used separate data structures for `ALLOW` and `DENY`).
   */
  private boolean updateMatchingAcl(AclEntry aclEntry,
                                    Set<KafkaPrincipal> matchingPrincipals,
                                    String host,
                                    Operation requestedOperation,
                                    AuthorizeRule authorizeRule) {
    Operation aclOperation = AclMapper.operation(aclEntry.operation());
    PermissionType aclPermissionType = AclMapper.permissionType(aclEntry.permissionType());
    KafkaPrincipal aclPrincipal = aclEntry.kafkaPrincipal();
    String aclHost = aclEntry.host();
    if (AccessRule.matches(aclPrincipal, aclHost, aclOperation, aclPermissionType, matchingPrincipals,
        host, requestedOperation, PermissionType.DENY)) {
      authorizeRule.addRuleIfNotExist(AclMapper.accessRule(aclEntry));
      return true;
    } else if (!authorizeRule.allowRule().isPresent() && AccessRule.matches(aclPrincipal, aclHost,
        aclOperation, aclPermissionType, matchingPrincipals, host, requestedOperation, PermissionType.ALLOW)) {
      authorizeRule.addRuleIfNotExist(AclMapper.accessRule(aclEntry));
    }
    return false;
  }
}
