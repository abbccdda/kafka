/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import java.util.Map;
import java.util.Properties;
import kafka.server.ConfigType;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import scala.collection.JavaConversions;


/**
 * The Kafka topic config provider implementation for SBK.
 *
 */
public class KafkaTopicConfigProvider implements TopicConfigProvider {
  public static final String ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_GROUP = "KafkaTopicConfigProvider";
  public static final String ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_TYPE = "GetAllActiveTopicConfigs";
  private String _connectString;
  private boolean _zkSecurityEnabled;

  @Override
  public Properties topicConfigs(String topic) {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(_connectString,
                                                                              ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_GROUP,
                                                                              ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_TYPE,
                                                                              _zkSecurityEnabled);
    try {
      AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);
      return adminZkClient.fetchEntityConfig(ConfigType.Topic(), topic);
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Override
  public Map<String, Properties> allTopicConfigs() {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(_connectString,
                                                                              ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_GROUP,
                                                                              ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_TYPE,
                                                                              _zkSecurityEnabled);
    try {
      AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);
      return JavaConversions.mapAsJavaMap(adminZkClient.getAllTopicConfigs());
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _connectString = (String) configs.get(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG);
    _zkSecurityEnabled = (Boolean) configs.get(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
  }

  @Override
  public void close() {
    // nothing to do.
  }
}
