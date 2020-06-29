/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol;

import kafka.log.LogConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Utility class to deal with internal topics needed by cruise control/databalancer.
 */
public final class SbkTopicUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SbkTopicUtils.class);

    /**
     * Checks for existence of a topic and creates it if its not already present.
     *
     * @return true if the topic exists and false if the topic had to be created.
     */
    public static boolean checkTopicPropertiesMaybeCreate(SbkTopicConfig topicConfig,
                                                          Map<String, ?> config) {
        boolean topicExists = false;
        ConfluentAdmin adminClient = KafkaCruiseControlUtils.createAdmin(KafkaCruiseControlUtils.filterAdminClientConfigs(config));
        try {
            int numberOfBrokersInCluster = adminClient.describeCluster().nodes().get().size();

            int replicationFactor = topicConfig.replicationFactor;
            if (numberOfBrokersInCluster < replicationFactor) {
                LOG.warn("Kafka cluster has {} brokers but the requested replication factor is {} for topic {}.",
                        numberOfBrokersInCluster, replicationFactor, topicConfig.topic);
            } else {
                Set<String> topics = adminClient.listTopics().names().get();
                topicExists = ensureTopicCreated(adminClient, topics,
                        topicConfig.topic, topicConfig.cleanupPolicy, topicConfig.minRetentionTimeMs,
                        replicationFactor, topicConfig.partitionCount);
            }
        } catch (InterruptedException | ExecutionException ex) {
            LOG.error("Error while checking topic {} exsistence", topicConfig.topic, ex);
        } finally {
            KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
        }
        return topicExists;
    }

    /**
     * Check that topic "topic" exists and has the correct configuration. If the topic does not exist, it
     * will be created. Return value indicates if the topic did exist (but NOT that the topic configuration
     * has been updated to match the desired configuration).
     *
     * @param adminClient -- for topic describe operations
     * @param allTopics -- List of topics known to the system
     * @param topic -- topic whose existence and configuration is under question
     * @param cleanupPolicy -- cleanup policy of the topic: `delete` or `compact`
     * @param retentionMs -- desired retention period for the topic
     * @param replicationFactor -- desired replication factor for the topic
     * @param partitionCount -- desired partition count for the topic
     * @return true if the topic exists and false if the topic had to be created.
     */
    private static boolean ensureTopicCreated(ConfluentAdmin adminClient,
                                              Set<String> allTopics,
                                              String topic,
                                              String cleanupPolicy,
                                              long retentionMs,
                                              int replicationFactor,
                                              int partitionCount) {
        Map<String, String> props = new HashMap<>(2);
        props.put(LogConfig.RetentionMsProp(), Long.toString(retentionMs));
        props.put(LogConfig.CleanupPolicyProp(), cleanupPolicy);
        if (!allTopics.contains(topic)) {
            LOG.info("DataBalancer: Creating topic {} ", topic);
            NewTopic newTopic = new NewTopic(topic, partitionCount, (short) replicationFactor).configs(props);
            try {
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e.getMessage());
            }
            return false;
        } else {
            try {
                LOG.info("DataBalancer: Adjusting topic {} configuration", topic);
                ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                List<AlterConfigOp> alterConfigOps = props.entrySet()
                        .stream()
                        .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                        .collect(Collectors.toList());
                Map<ConfigResource, Collection<AlterConfigOp>> configs = Collections.singletonMap(topicResource, alterConfigOps);
                TopicDescription topicDescription;
                try {
                    adminClient.incrementalAlterConfigs(configs).all().get();
                    topicDescription = adminClient.describeTopics(Collections.singleton(topic)).values().get(topic).get();
                    maybeIncreaseTopicPartitionCount(adminClient, topic, topicDescription, partitionCount);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }  catch (RuntimeException re) {
                LOG.error("Skip updating configuration of topic " +  topic + " due to exception.", re);
            }
            return true;
        }
    }

    /**
     * Add new partitions to the Kafka topic if the topic has fewer partitions than {@code partitionCount}.
     *
     * @param adminClient AdminZkClient class to use to add new partitions.
     * @param topic The topic to apply the change.
     * @param topicDescription The description of the topic ot apply the change.
     * @param partitionCount The target partition count of the topic.
     */
    static void maybeIncreaseTopicPartitionCount(ConfluentAdmin adminClient,
                                                 String topic,
                                                 TopicDescription topicDescription,
                                                 int partitionCount) throws ExecutionException, InterruptedException {
        if (partitionCount > topicDescription.partitions().size()) {
            LOG.info("DataBalancer: Adjusting topic {} partition count to {}", topic, partitionCount);

            Map<String, NewPartitions> newPartitions = Collections.singletonMap(topic, NewPartitions.increaseTo(partitionCount));
            adminClient.createPartitions(newPartitions).all().get();
            LOG.info("Kafka topic " + topic + " now has " + partitionCount + " partitions.");
        }
    }

    public static final class SbkTopicConfig {

        public final long minRetentionTimeMs;
        public final short replicationFactor;
        public final String topic;
        public final int partitionCount;
        public final String cleanupPolicy;

        public SbkTopicConfig(long minRetentionTimeMs,
                              short replicationFactor,
                              String topic,
                              int partitionCount,
                              String cleanupPolicy) {
            this.minRetentionTimeMs = minRetentionTimeMs;
            this.replicationFactor = replicationFactor;
            this.topic = topic;
            this.partitionCount = partitionCount;
            this.cleanupPolicy = cleanupPolicy;
        }
    }

    public static final class SbkTopicConfigBuilder {

        private long minRetentionTimeMs;
        private short replicationFactor;
        private String topic;
        private int partitionCount;
        private String cleanupPolicy;

        public SbkTopicConfig build() {
            return new SbkTopicConfig(minRetentionTimeMs,
                    replicationFactor,
                    topic,
                    partitionCount,
                    cleanupPolicy);
        }

        public SbkTopicConfigBuilder setMinRetentionTimeMs(long minRetentionTimeMs) {
            this.minRetentionTimeMs = minRetentionTimeMs;
            return this;
        }

        public SbkTopicConfigBuilder setReplicationFactor(Map<String, ?> config,
                                                          String replicationFactorConfig,
                                                          short defaultReplicationFactor) {
            String replicationFactor = (String) config.get(replicationFactorConfig);
            this.replicationFactor = replicationFactor == null || replicationFactor.isEmpty()
                    ? defaultReplicationFactor
                    : Short.parseShort(replicationFactor);
            return this;
        }

        public SbkTopicConfigBuilder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public SbkTopicConfigBuilder setPartitionCount(Map<String, ?> config,
                                                       String partitionCountConfig,
                                                       int defaultPartitionCount) {
            String partitionCount = (String) config.get(partitionCountConfig);
            this.partitionCount = partitionCount == null || partitionCount.isEmpty()
                    ? defaultPartitionCount
                    : Integer.parseInt(partitionCount);
            return this;
        }

        public SbkTopicConfigBuilder setPartitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return this;
        }

        public SbkTopicConfigBuilder setCleanupPolicy(String cleanupPolicy) {
            this.cleanupPolicy = cleanupPolicy;
            return this;
        }
    }
}
