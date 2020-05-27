/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.TopicPartition;

public class TenantHelpers {
    public static final Pattern TENANT_PREFIX_REGEX = Pattern.compile("^(lkc-[0-9a-z]+_).+");

    public static boolean isTenantPrefixed(String name) {
        return TENANT_PREFIX_REGEX.matcher(name).matches();
    }

    public static boolean isTenantPrefixed(TopicPartition topicPartition) {
        return isTenantPrefixed(topicPartition.topic());
    }

    public static String extractTenantPrefix(String name) {
        Matcher matcher = TENANT_PREFIX_REGEX.matcher(name);
        if (matcher.matches())
            return matcher.group(1);
        else
            throw new IllegalArgumentException("Name is not tenant-prefixed: " + name);
    }

    public static TopicPartition prefixWithTenant(String prefix, TopicPartition tp) {
        if (tp.topic().startsWith(prefix))
            return tp;
        else
            return new TopicPartition(prefix + tp.topic(), tp.partition());
    }
}
