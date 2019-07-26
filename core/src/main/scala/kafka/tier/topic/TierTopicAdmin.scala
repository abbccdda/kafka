/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.topic

import java.util.Properties

import kafka.utils.Logging
import kafka.zk.AdminZkClient
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.TopicExistsException

object TierTopicAdmin extends Logging {
  /**
    * Check if topic exists. Create a new topic with the given configurations if it does not.
    *
    * @param adminZkClient admin zk client to use
    * @param topicName topic to create
    * @param configuredNumPartitions configured number of partitions
    * @param configuredReplicationFactor configured replication factor
    * @return Number of partitions in the created topic. Note that this may differ from the configured value if the topic
    *         already exists.
    * @throws Exception Caller is expected to handle any exceptions from the underlying zk client
    */
  def ensureTopic(adminZkClient: AdminZkClient,
                  topicName: String,
                  configuredNumPartitions: Int,
                  configuredReplicationFactor: Short): Int = {
    try {
      // try to create the topic
      adminZkClient.createTopic(topicName, configuredNumPartitions, configuredReplicationFactor, TierTopicAdmin.topicConfig)
      info(s"Created topic $topicName with $configuredNumPartitions partitions")
      configuredNumPartitions
    } catch {
      case _: TopicExistsException =>
        // topic already exists; query the number of partitions
        val numPartitions = adminZkClient.numPartitions(Set(topicName)).get(topicName).get

        if (numPartitions != configuredNumPartitions)
          warn(s"Topic $topicName already exists. Mismatch between existing partition count $numPartitions " +
            s"and configured partition count $configuredNumPartitions.")
        else
          info(s"Topic $topicName exists with $numPartitions partitions")

        numPartitions
    }
  }

  // visible for tests
  private[topic] def topicConfig: Properties = {
    val properties = new Properties()
    properties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
    properties.put(TopicConfig.RETENTION_MS_CONFIG, "-1")
    properties.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
    properties
  }
}

