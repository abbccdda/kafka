// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.ldap.authorizer;


import io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import java.util.HashMap;
import java.util.Map;

public class LdapAuthorizer extends ConfluentServerAuthorizer {

  @Override
  public void configure(Map<String, ?> configs) {
    Map<String, Object>  authorizerConfigs = new HashMap<>(configs);
    authorizerConfigs.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP,
        AccessRuleProviders.ACL.name());
    super.configure(authorizerConfigs);
  }
}
