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
    public static final String CONFLUENT_PREFIX = "confluent.";

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

    public static final String APPEND_RECORD_INTERCEPTOR_CLASSES_CONFIG = CONFLUENT_PREFIX + "append.record.interceptor.classes";
    public static final String APPEND_RECORD_INTERCEPTOR_CLASSES_CONFIG_DOC = "A list of classes to use as interceptors. " +
        "Implementing the <code>RecordInterceptor</code> interface allows you to intercept and possibly reject " +
        "the records before they are appended from the produce request to the log at the Kafka broker. " +
        "By default, there are no interceptors.";

    private static final String SCHEMA_VALIDATION = "schema.validation";

    public static final String SCHEMA_VALIDATION_CONFIG = CONFLUENT_PREFIX + SCHEMA_VALIDATION;
    public static final String SCHEMA_VALIDATION_DOC = "True if schema validation is enabled for this topic.";

    public static final String TOPIC_PLACEMENT_CONSTRAINTS_CONFIG = CONFLUENT_PREFIX + "placement.constraints";
    public static final String TOPIC_PLACEMENT_CONSTRAINTS_DOC = "This configuration is a JSON object that controls the set of " +
        "brokers (replicas) which will always be allowed to join the ISR. And the set of brokers (observers) which are not " +
        "allowed to join the ISR.";
}
