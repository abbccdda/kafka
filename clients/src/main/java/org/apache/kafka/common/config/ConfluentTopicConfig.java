/*
 Copyright 2018 Confluent Inc.
 */

package org.apache.kafka.common.config;

/**
 * Keys that can be used to configure a topic for Confluent Platform Kafka. These keys are useful when creating or reconfiguring a
 * topic using the AdminClient.
 *
 */
// This is a public API, so we should not remove or alter keys without a discussion and a deprecation period.
public class ConfluentTopicConfig {
    private static final String CONFLUENT_PREFIX = "confluent.";

    public static final String TIER_ENABLE_CONFIG = CONFLUENT_PREFIX + "tier.enable";
    public static final String TIER_ENABLE_DOC = "True if this topic has tiered storage enabled.";

    public static final String TIER_LOCAL_HOTSET_BYTES_CONFIG = CONFLUENT_PREFIX + "tier.local.hotset.bytes";
    public static final String TIER_LOCAL_HOTSET_BYTES_DOC = "For a topic with tiering enabled, this configuration " +
            "controls the maximum size a partition (which consists of log segments) can grow to on broker-local storage " +
            "before we will discard old log segments to free up space. Log segments retained on broker-local storage is " +
            "referred as the \"hotset\". Segments discarded from local store could continue to exist in tiered storage " +
            "and remain available for fetches depending on retention configurations. By default there is no size limit " +
            "only a time limit. Since this limit is enforced at the partition level, multiply it by the number of " +
            "partitions to compute the topic hotset in bytes.";

    public static final String TIER_LOCAL_HOTSET_MS_CONFIG = CONFLUENT_PREFIX + "tier.local.hotset.ms";
    public static final String TIER_LOCAL_HOTSET_MS_DOC = "For a topic with tiering enabled, this configuration " +
            "controls the maximum time we will retain a log segment on broker-local storage before we will discard it to " +
            "free up space. Segments discarded from local store could continue to exist in tiered storage and remain " +
            "available for fetches depending on retention configurations. If set to -1, no time limit is applied.";
}
