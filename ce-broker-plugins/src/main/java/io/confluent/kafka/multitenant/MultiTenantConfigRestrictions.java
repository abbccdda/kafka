package io.confluent.kafka.multitenant;

import org.apache.kafka.common.utils.Utils;

import java.util.Set;

public class MultiTenantConfigRestrictions {

  public static final Set<String> VISIBLE_BROKER_CONFIGS = Utils.mkSet(
      // topic config defaults
      "log.cleanup.policy",
      "message.max.bytes",
      "log.message.timestamp.difference.max.ms",
      "log.message.timestamp.type",
      "log.cleaner.min.compaction.lag.ms",
      "log.retention.bytes",
      "log.retention.ms",
      "log.cleaner.delete.retention.ms",
      "log.segment.bytes",
      "default.replication.factor",
      "num.partitions",
      "confluent.schema.registry.url"
  );

  // Topic configs that are modifiable by cloud users
  public static final Set<String> UPDATABLE_TOPIC_CONFIGS = Utils.mkSet(
      "cleanup.policy",
      "max.message.bytes",
      "message.timestamp.difference.max.ms",
      "message.timestamp.type",
      "min.compaction.lag.ms",
      "retention.bytes",
      "retention.ms",
      "min.insync.replicas",
      "delete.retention.ms",
      "segment.bytes",
      "segment.ms"
  );

  public static boolean visibleTopicConfig(String configName) {
    return !configName.startsWith("confluent.tier") &&
        // we will not allow users to turn on / off schema validation in CC for now
        !configName.startsWith("confluent.key.schema.validation") &&
        !configName.startsWith("confluent.value.schema.validation") &&
        !configName.equals("confluent.placement.constraints");
  }
}
