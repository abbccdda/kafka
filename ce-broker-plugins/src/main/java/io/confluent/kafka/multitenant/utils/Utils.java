// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.utils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  public static String requireNonEmpty(String value, String argName) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException(argName + " must not be empty or null");
    }
    return value;
  }

  public static AdminClient createAdminClient(String endpoint) {
    Properties adminClientProps = new Properties();
    try {
        LOG.info("Using bootstrap server {}", endpoint);
        adminClientProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, endpoint);
        return AdminClient.create(adminClientProps);
    } catch (Exception e) {
        LOG.error("Failed to create admin client for endpoint {} ", endpoint, e);
        return null;
    }
  }
}
