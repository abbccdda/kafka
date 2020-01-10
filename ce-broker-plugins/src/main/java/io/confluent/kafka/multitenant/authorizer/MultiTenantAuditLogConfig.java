/*
 * Copyright [2020 - 2020] Confluent Inc.
 */
package io.confluent.kafka.multitenant.authorizer;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class MultiTenantAuditLogConfig extends AbstractConfig {

  // We can't refer to the AuditLogConfig and EventLog config to build this for dependency reasons
  public static final String MULTI_TENANT_AUDIT_LOGGER_ENABLE_CONFIG = "confluent.security.event.logger.multitenant.enable";
  public static final String DEFAULT_MULTI_TENANT_AUDIT_LOGGER_ENABLE_CONFIG = "false";
  public static final String MULTI_TENANT_AUDIT_LOGGER_ENABLE_CONFIG_DOC = "Should the multi tenant event logger be enabled.";

  private static final ConfigDef CONFIG;

  static {
    CONFIG = new ConfigDef()
        .define(
            MULTI_TENANT_AUDIT_LOGGER_ENABLE_CONFIG,
            ConfigDef.Type.BOOLEAN,
            DEFAULT_MULTI_TENANT_AUDIT_LOGGER_ENABLE_CONFIG,
            ConfigDef.Importance.HIGH,
            MULTI_TENANT_AUDIT_LOGGER_ENABLE_CONFIG_DOC
        );
  }

  public MultiTenantAuditLogConfig(Map<String, ?> configs) {
    super(CONFIG, configs);
  }
}
