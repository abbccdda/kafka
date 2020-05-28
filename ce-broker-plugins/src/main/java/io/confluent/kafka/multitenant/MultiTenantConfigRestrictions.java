package io.confluent.kafka.multitenant;

import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.ConfluentTopicConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.utils.Utils;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MultiTenantConfigRestrictions {

  public static final Set<String> UPDATABLE_BROKER_CONFIGS = Utils.mkSet(
    // cluster/broker configs
    KafkaConfig.SslCipherSuitesProp(),
    KafkaConfig.AutoCreateTopicsEnableProp(),
    // used when the create topics request does not specify the number of partitions
    KafkaConfig.NumPartitionsProp(),
    // topic config defaults - start with the most useful, consider exposing others later
    KafkaConfig.LogRetentionTimeMillisProp()
  );

  public static final Set<String> VISIBLE_BROKER_CONFIGS = Stream.concat(
    UPDATABLE_BROKER_CONFIGS.stream(),
    Stream.of(
      ConfluentConfigs.SCHEMA_REGISTRY_URL_CONFIG,
      // C3 uses the following to pick a default value in the create topics page
      KafkaConfig.DefaultReplicationFactorProp(),
      // topic config defaults
      KafkaConfig.LogCleanupPolicyProp(),
      KafkaConfig.MessageMaxBytesProp(),
      KafkaConfig.LogMessageTimestampDifferenceMaxMsProp(),
      KafkaConfig.LogMessageTimestampTypeProp(),
      KafkaConfig.LogCleanerMinCompactionLagMsProp(),
      KafkaConfig.LogRetentionBytesProp(),
      KafkaConfig.LogCleanerDeleteRetentionMsProp(),
      KafkaConfig.LogSegmentBytesProp()
    )
  ).collect(Collectors.toSet());

  // Topic configs that are modifiable by cloud users
  public static final Set<String> UPDATABLE_TOPIC_CONFIGS = Utils.mkSet(
    TopicConfig.CLEANUP_POLICY_CONFIG,
    TopicConfig.MAX_MESSAGE_BYTES_CONFIG,
    TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG,
    TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG,
    TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG,
    TopicConfig.RETENTION_BYTES_CONFIG,
    TopicConfig.RETENTION_MS_CONFIG,
    TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
    TopicConfig.DELETE_RETENTION_MS_CONFIG,
    TopicConfig.SEGMENT_BYTES_CONFIG,
    TopicConfig.SEGMENT_MS_CONFIG
  );

  public static boolean visibleTopicConfig(String configName) {
    // hide all Confluent-specific topic configs in CCloud
    return !configName.startsWith(ConfluentTopicConfig.CONFLUENT_PREFIX);
  }
}
