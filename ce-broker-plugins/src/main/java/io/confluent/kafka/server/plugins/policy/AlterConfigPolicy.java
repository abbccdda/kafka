// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.policy;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import java.util.Map;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlterConfigPolicy implements org.apache.kafka.server.policy.AlterConfigPolicy {
  private static final Logger log = LoggerFactory.getLogger(AlterConfigPolicy.class);

  private TopicPolicyConfig policyConfig;

  @Override
  public void configure(Map<String, ?> cfgMap) {
    this.policyConfig = new TopicPolicyConfig(cfgMap);
  }

  /**
   * We don't allow any config updates using the external listener where all principals are MultiTenantPrincipals.
   * On internal listeners with regular non-tenant principals, config updates are permitted, for example to update
   * SSL keystores without broker restart.
   */
  @Override
  public void validate(RequestMetadata reqMetadata) throws PolicyViolationException {
    KafkaPrincipal principal = reqMetadata.principal();

    if (principal == null) {
      log.error("Request principal not provided to validate alter policy");
      throw new IllegalStateException("Request principal not provided to validate alter policy");
    } else if (principal instanceof MultiTenantPrincipal) {
      switch (reqMetadata.resource().type()) {
        case TOPIC:
          log.trace("Validating request to update configs using principal {}", principal);
          policyConfig.validateTopicConfigs(reqMetadata.configs());
          break;

        default:
          log.trace("Not allowing update of {} configs using principal {}", reqMetadata.resource().type(), principal);
          throw new PolicyViolationException("Altering resources of type " + reqMetadata.resource().type() + " is not permitted");
      }
    } else {
      log.info("Allowing update of configs using principal {}", principal);
    }
  }

  @Override
  public void close() throws Exception {}

}
