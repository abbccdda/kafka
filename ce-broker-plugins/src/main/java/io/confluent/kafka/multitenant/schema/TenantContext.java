// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.schema;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.utils.Utils;

public class TenantContext implements TransformContext {
  public static final String DELIMITER = "_";

  public enum ValueType { TOPIC, GROUP, TRANSACTIONAL_ID }

  public final MultiTenantPrincipal principal;
  private final String prefix;
  protected final int prefixSizeInBytes;

  public static boolean isTenantPrefixed(String prefixedName) {
    return prefixedName != null && prefixedName.contains(DELIMITER);
  }

  public static String extractTenantPrefix(String prefixedName) {
    if (!isTenantPrefixed(prefixedName)) {
      throw new IllegalArgumentException("Name is not tenant-prefixed: " + prefixedName);
    }
    return prefixedName.substring(0, prefixedName.indexOf(DELIMITER) + 1);
  }

  public static String extractTenant(String prefixedName) {
    if (!isTenantPrefixed(prefixedName)) {
      throw new IllegalArgumentException("Name is not tenant-prefixed: " + prefixedName);
    }
    return prefixedName.substring(0, prefixedName.indexOf(DELIMITER));
  }

  public TenantContext(MultiTenantPrincipal principal) {
    String tenantName = principal.tenantMetadata().tenantName;
    ensureValidTenantName(tenantName);

    this.principal = principal;
    this.prefix = tenantName + DELIMITER;
    this.prefixSizeInBytes = Utils.utf8Length(prefix);
  }

  private static void ensureValidTenantName(String tenantName) {
    if (tenantName.contains(DELIMITER)) {
      throw new IllegalArgumentException("Tenant name includes illegal `" + DELIMITER
              + "` character: " + tenantName);
    }

    try {
      Topic.validate(tenantName);
    } catch (InvalidTopicException e) {
      throw new IllegalArgumentException("Tenant name " + tenantName
          + " must be a valid topic prefix");
    }
  }

  public String addTenantPrefix(String value) {
    return prefix + value;
  }

  public String prefix() {
    return prefix;
  }

  public int prefixSizeInBytes() {
    return prefixSizeInBytes;
  }

  public TopicPartition removeTenantPrefix(TopicPartition tp) {
    return new TopicPartition(removeTenantPrefix(tp.topic()), tp.partition());
  }

  public String removeTenantPrefix(String value) {
    return value.substring(prefix.length());
  }

  public static boolean hasTenantPrefix(String prefix, String prefixedName) {
    return isTenantPrefixed(prefixedName) && prefixedName.startsWith(prefix);
  }

  public boolean hasTenantPrefix(String value) {
    return hasTenantPrefix(prefix, value);
  }

  public String removeAllTenantPrefixes(String message) {
    return message.replace(prefix, "");
  }

  public int sizeOfRemovedPrefixes(String message) {
    return message.length() - removeAllTenantPrefixes(message).length();
  }

  public String prefixedWildcard() {
    return prefix + "*";
  }

}
