/*
 Copyright 2018 Confluent Inc.
 */

package org.apache.kafka.common.config;

/**
 * Keys that can be used to configure a topic for Confluent Platform Kafka. These keys are useful when creating or reconfiguring a
 * topic using the AdminClient.
 */
// This is a public API, so we should not remove or alter keys without a discussion and a deprecation period.
public class ConfluentTopicConfig {
    public static final String CONFLUENT_PREFIX = "confluent.";

    public static final String TIER_ENABLE_CONFIG = CONFLUENT_PREFIX + "tier.enable";
    public static final String TIER_ENABLE_DOC = "Allow tiering for topic(s). This enables tiering and fetching of data " +
            "to and from the configured remote storage.";

    public static final String TIER_LOCAL_HOTSET_BYTES_CONFIG = CONFLUENT_PREFIX + "tier.local.hotset.bytes";
    public static final String TIER_LOCAL_HOTSET_BYTES_DOC = "When tiering is enabled, this configuration " +
            "controls the maximum size a partition (which consists of log segments) can grow to on broker-local storage " +
            "before we will discard old log segments to free up space. Log segments retained on broker-local storage is " +
            "referred as the \"hotset\". Segments discarded from local store could continue to exist in tiered storage " +
            "and remain available for fetches depending on retention configurations. By default there is no size limit " +
            "only a time limit. Since this limit is enforced at the partition level, multiply it by the number of " +
            "partitions to compute the topic hotset in bytes.";

    public static final String TIER_LOCAL_HOTSET_MS_CONFIG = CONFLUENT_PREFIX + "tier.local.hotset.ms";
    public static final String TIER_LOCAL_HOTSET_MS_DOC = "When tiering is enabled, this configuration " +
            "controls the maximum time we will retain a log segment on broker-local storage before we will discard it to " +
            "free up space. Segments discarded from local store could continue to exist in tiered storage and remain " +
            "available for fetches depending on retention configurations. If set to -1, no time limit is applied.";

    public static final String TIER_SEGMENT_HOTSET_ROLL_MIN_BYTES_CONFIG =
            CONFLUENT_PREFIX + "tier.segment.hotset.roll.min.bytes";
    public static final String TIER_SEGMENT_HOTSET_ROLL_MIN_BYTES_DOC = "When tiering is enabled, this configuration " +
            "allows a segment roll to be forced if the active segment is larger than the configured bytes and if all " +
            "records in the segment are ready for eviction from the hotset. Rolling the segment ensures that it can be " +
            "tiered and the segment can then be deleted from the hotset. A minimum size is enforced to ensure efficient " +
            "tiering and consumption.";

    public static final String PREFER_TIER_FETCH_MS_CONFIG = CONFLUENT_PREFIX + "prefer.tier.fetch.ms";
    public static final String PREFER_TIER_FETCH_MS_DOC = "For a topic with tiering enabled, this configuration sets " +
            "preference for data to be fetched from tiered storage, even if it is available on broker-local storage " +
            "through the configured hotset retention. Data will be preferentially fetched from tiered storage if present " +
            "when the configured amount of time has elapsed since data was appended to the log.";

    public static final String APPEND_RECORD_INTERCEPTOR_CLASSES_CONFIG = CONFLUENT_PREFIX + "append.record.interceptor.classes";
    public static final String APPEND_RECORD_INTERCEPTOR_CLASSES_CONFIG_DOC = "A list of classes to use as interceptors. " +
        "Implementing the <code>RecordInterceptor</code> interface allows you to intercept and possibly reject " +
        "the records before they are appended from the produce request to the log at the Kafka broker. " +
        "By default, there are no interceptors.";

    private static final String SCHEMA_VALIDATION = "schema.validation";

    public static final String KEY_SCHEMA_VALIDATION_CONFIG = CONFLUENT_PREFIX + "key." + SCHEMA_VALIDATION;
    public static final String KEY_SCHEMA_VALIDATION_DOC = "True if schema validation at record key is enabled for this topic.";

    public static final String VALUE_SCHEMA_VALIDATION_CONFIG = CONFLUENT_PREFIX + "value." + SCHEMA_VALIDATION;
    public static final String VALUE_SCHEMA_VALIDATION_DOC = "True if schema validation at record value is enabled for this topic.";

    private static final String KEY_SUBJECT_NAME_STRATEGY = "key.subject.name.strategy";
    private static final String VALUE_SUBJECT_NAME_STRATEGY = "value.subject.name.strategy";

    // default TopicNameStrategy.class cannot be defined here, but should be in the plugin that can
    // depend on schema.registry
    public static final String KEY_SUBJECT_NAME_STRATEGY_CONFIG = CONFLUENT_PREFIX + KEY_SUBJECT_NAME_STRATEGY;
    public static final String KEY_SUBJECT_NAME_STRATEGY_DOC =
            "Determines how to construct the subject name under which the key schema is registered "
                    + "with the schema registry. By default, TopicNameStrategy is used";


    public static final String VALUE_SUBJECT_NAME_STRATEGY_CONFIG = CONFLUENT_PREFIX + VALUE_SUBJECT_NAME_STRATEGY;
    public static final String VALUE_SUBJECT_NAME_STRATEGY_DOC =
            "Determines how to construct the subject name under which the value schema is registered "
                    + "with the schema registry. By default, TopicNameStrategy is used";

    private static final String LINE_SEPARATOR = System.lineSeparator();
    public static final String TOPIC_PLACEMENT_CONSTRAINTS_CONFIG = CONFLUENT_PREFIX + "placement.constraints";
    public static final String TOPIC_PLACEMENT_CONSTRAINTS_DOC = "This configuration is a JSON object that controls the set of " +
        "brokers (replicas) which will always be allowed to join the ISR. And the set of brokers (observers) which are not " +
        "allowed to join the ISR. The format of JSON is:" + LINE_SEPARATOR +
            "{" + LINE_SEPARATOR +
            "  \"version\": 1," + LINE_SEPARATOR +
            "  \"replicas\": [" + LINE_SEPARATOR +
            "    {" + LINE_SEPARATOR +
            "      \"count\": 2," + LINE_SEPARATOR +
            "      \"constraints\": {\"rack\": \"east-1\"}" + LINE_SEPARATOR +
            "    }," + LINE_SEPARATOR +
            "    {" + LINE_SEPARATOR +
            "      \"count\": 1," + LINE_SEPARATOR +
            "      \"constraints\": {\"rack\": \"east-2\"}" + LINE_SEPARATOR +
            "    }" + LINE_SEPARATOR +
            "  ]," + LINE_SEPARATOR +
            "  \"observers\":[" + LINE_SEPARATOR +
            "    {" + LINE_SEPARATOR +
            "      \"count\": 1," + LINE_SEPARATOR +
            "      \"constraints\": {\"rack\": \"west-1\"}" + LINE_SEPARATOR +
            "    }" + LINE_SEPARATOR +
            "  ]" + LINE_SEPARATOR +
            "}";

    public static final String SEGMENT_SPECULATIVE_PREFETCH_ENABLE_CONFIG = CONFLUENT_PREFIX + "segment.speculative.prefetch.enable";
    public static final String SEGMENT_SPECULATIVE_PREFETCH_ENABLE_DOC = "If <code>true</code>, reads to log " +
        "segments may be prefetched from disk if they're predicted to not be resident in memory. This can reduce " +
        "latency and mitigate pipeline stalls when fetching from older log segments, at the expense of potentially " +
        "increased memory usage, which may have adverse affects on other cached data. If <code>false</code>, no " +
        "explicit prefetching is performed.";
}
