// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.policy;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import java.util.Map;

import org.apache.kafka.common.config.ConfigResource;
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

  @Override
  public void validate(RequestMetadata reqMetadata) throws PolicyViolationException {
    if (reqMetadata.resource().type().equals(ConfigResource.Type.TOPIC)) {
      validateTopicRequest(reqMetadata);
    } else if (reqMetadata.resource().type().equals(ConfigResource.Type.BROKER)) {
      validateBrokerRequest(reqMetadata);
    } else {
      throw new PolicyViolationException(String.format(
              "Altering resources of type '%s' is not permitted",
              reqMetadata.resource().toString()));
    }
  }

  private void validateTopicRequest(RequestMetadata reqMetadata) throws PolicyViolationException {
    this.policyConfig.validateTopicConfigs(reqMetadata.configs());
  }

  /**
   * We don't allow any broker config updates using the external listener where all principals
   * are MultiTenantPrincipals. On internal listeners with regular non-tenant principals, broker
   * config updates are permitted, for example to update SSL keystores without broker restart.
   */
  private void validateBrokerRequest(RequestMetadata reqMetadata) throws PolicyViolationException {
    KafkaPrincipal principal = reqMetadata.principal();
    if (principal == null) {
      log.error("Request principal not provided to validate alter policy");
      throw new IllegalStateException("Request principal not provided to validate alter policy");
    } else if (principal instanceof MultiTenantPrincipal) {
      log.trace("Not allowing update of broker config using principal {}", principal);
      throw new PolicyViolationException("Altering broker configs is not permitted");
    } else {
      log.info("Allowing update of broker config using principal {}", principal);
    }
  }

  @Override
  public void close() throws Exception {}

}
