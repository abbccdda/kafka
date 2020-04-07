// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.AuthorizeRule;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class SuperUserProvider implements AccessRuleProvider {

  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public boolean isSuperUser(KafkaPrincipal principal, Scope scope) {
    return true;
  }

  @Override
  public AuthorizeRule findRule(KafkaPrincipal sessionPrincipal,
                                Set<KafkaPrincipal> groupPrincipals,
                                String host,
                                Action action) {
    return new AuthorizeRule();
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
    return "SUPER_USERS";
  }

  @Override
  public void close() throws IOException {
  }
}
