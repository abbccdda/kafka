// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.ldap.authorizer;


import io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.GroupProviders;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.utils.Time;


public class LdapAuthorizer extends ConfluentServerAuthorizer {
  public static final String LICENSE_PROP = "ldap.authorizer.license";
  private static final String METRIC_GROUP = "kafka.ldap.plugins";

  public LdapAuthorizer() {
    this(Time.SYSTEM);
  }

  public LdapAuthorizer(Time time) {
    super(time);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    Map<String, Object>  authorizerConfigs = new HashMap<>(configs);
    authorizerConfigs.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP,
        AccessRuleProviders.ACL.name());
    authorizerConfigs.put(ConfluentAuthorizerConfig.GROUP_PROVIDER_PROP,
        GroupProviders.LDAP.name());
    super.configure(authorizerConfigs);
  }

  @Override
  public String licensePropName() {
    return LICENSE_PROP;
  }

  @Override
  public String licenseStatusMetricGroup() {
    return METRIC_GROUP;
  }
}
