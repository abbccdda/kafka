// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer.acl;

import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.provider.AccessRuleProvider;
import io.confluent.kafka.security.authorizer.AccessRule;
import java.util.Set;
import java.util.stream.Collectors;
import kafka.network.RequestChannel;
import kafka.security.auth.Operation;
import kafka.security.auth.ResourceType;
import kafka.security.auth.SimpleAclAuthorizer;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

public class AclProvider extends SimpleAclAuthorizer implements AccessRuleProvider {

  private static final Logger log = LoggerFactory.getLogger("kafka.authorizer.logger");

  @Override
  public String providerName() {
    return AccessRuleProviders.ACL.name();
  }

  @Override
  public boolean isSuperUser(KafkaPrincipal sessionPrincipal,
                             Set<KafkaPrincipal> groupPrincipals,
                             String scope) {
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
                                     String scope,
                                     Resource resource) {
    ResourceType resourceType = AclMapper.kafkaResourceType(resource.resourceType());
    KafkaPrincipal userPrincipal = userPrincipal(sessionPrincipal);
    return JavaConversions.setAsJavaSet(getMatchingAcls(resourceType, resource.name())).stream()
        .map(AclMapper::accessRule)
        .filter(acl -> userOrGroupAcl(acl, userPrincipal, groupPrincipals))
        .collect(Collectors.toSet());
  }

  @Override
  public boolean authorize(RequestChannel.Session session,
                           Operation operation,
                           kafka.security.auth.Resource resource) {
    throw new IllegalStateException("This provider should be used for authorization only using the AccessRuleProvider interface");
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
