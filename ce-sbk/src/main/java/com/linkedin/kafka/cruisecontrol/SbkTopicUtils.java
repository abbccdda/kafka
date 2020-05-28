/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import kafka.admin.BrokerMetadata;
import kafka.admin.RackAwareMode;
import kafka.controller.ReplicaAssignment;
import kafka.log.LogConfig;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option$;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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
        String connectString = (String) config.get(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG);
        boolean zkSecurityEnabled = (Boolean) config.get(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
        KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(connectString,
                "KafkaCruiseControlTopicCheck",
                "EnsureTopicCreated",
                zkSecurityEnabled);
        AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);
        ConfluentAdmin adminClient = KafkaCruiseControlUtils.createAdmin(KafkaCruiseControlUtils.filterAdminClientConfigs(config));
        try {
            int numberOfBrokersInCluster = adminClient.describeCluster().nodes().get().size();

            int replicationFactor = topicConfig.replicationFactor;
            if (numberOfBrokersInCluster < replicationFactor) {
                throw new IllegalStateException(
                        String.format("Kafka cluster has %d brokers but the requested replication factor is %d " +
                                        "(brokers in cluster=%d, zookeeper.connect=%s)", numberOfBrokersInCluster,
                                replicationFactor, numberOfBrokersInCluster,
                                config.get(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG)));
            }

            Set<String> topics = adminClient.listTopics().names().get();
            return ensureTopicCreated(kafkaZkClient, adminZkClient, adminClient, topics,
                    topicConfig.topic, topicConfig.cleanupPolicy, topicConfig.minRetentionTimeMs,
                    replicationFactor, topicConfig.partitionCount);
        } catch (Exception ex) {
            LOG.error(String.format("Unexpected error when checking for %s topic existence.", topicConfig.topic), ex);
            throw new RuntimeException(ex);
        } finally {
            KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
            KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
        }
    }

    /**
     * Check that topic "topic" exists and has the correct configuration. If the topic does not exist, it
     * will be created. Return value indicates if the topic did exist (but NOT that the topic configuration
     * has been updated to match the desired configuration).
     *
     * @param kafkaZkClient -- KafkaZK Client to use for topic create/update operations
     * @param adminZkClient -- AdminZK client to use for topic create/update operations
     * @param adminClient -- for topic describe operations
     * @param allTopics -- List of topics known to the system
     * @param topic -- topic whose existence and configuration is under question
     * @param cleanupPolicy -- cleanup policy of the topic: `delete` or `compact`
     * @param retentionMs -- desired retention period for the topic
     * @param replicationFactor -- desired replication factor for the topic
     * @param partitionCount -- desired partition count for the topic
     * @return true if the topic exists and false if the topic had to be created.
     */
    private static boolean ensureTopicCreated(KafkaZkClient kafkaZkClient,
                                              AdminZkClient adminZkClient,
                                              ConfluentAdmin adminClient,
                                              Set<String> allTopics,
                                              String topic,
                                              String cleanupPolicy,
                                              long retentionMs,
                                              int replicationFactor,
                                              int partitionCount) {
        Properties props = new Properties();
        props.setProperty(LogConfig.RetentionMsProp(), Long.toString(retentionMs));
        props.setProperty(LogConfig.CleanupPolicyProp(), cleanupPolicy);
        if (!allTopics.contains(topic)) {
            LOG.info("DataBalancer: Creating topic {} ", topic);
            adminZkClient.createTopic(topic, partitionCount, replicationFactor, props,
                    RackAwareMode.Safe$.MODULE$, false, Option$.MODULE$.empty());
            return false;
        } else {
            try {
                LOG.info("DataBalancer: Adjusting topic {} configuration", topic);
                adminZkClient.changeTopicConfig(topic, props);
                TopicDescription topicDescription;
                try {
                    topicDescription = adminClient.describeTopics(Collections.singleton(topic)).values().get(topic).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e.getMessage());
                }
                maybeIncreaseTopicPartitionCount(kafkaZkClient, adminZkClient, topic, topicDescription, partitionCount);
            }  catch (RuntimeException re) {
                LOG.error("Skip updating configuration of topic " +  topic + " due to exception.", re);
            }
            return true;
        }
    }

    /**
     * Add new partitions to the Kafka topic if the topic has fewer partitions than {@code partitionCount}.
     *
     * @param kafkaZkClient KafkaZkClient class to use to add new partitions.
     * @param adminZkClient AdminZkClient class to use to add new partitions.
     * @param topic The topic to apply the change.
     * @param topicDescription The description of the topic ot apply the change.
     * @param partitionCount The target partition count of the topic.
     */
    @SuppressWarnings({"unchecked", "deprecation"})
    static void maybeIncreaseTopicPartitionCount(KafkaZkClient kafkaZkClient,
                                                 AdminZkClient adminZkClient,
                                                 String topic,
                                                 TopicDescription topicDescription,
                                                 int partitionCount) {
        if (partitionCount > topicDescription.partitions().size()) {
            scala.collection.mutable.Map<Object, ReplicaAssignment> existingAssignment = new scala.collection.mutable.HashMap<>();
            scala.collection.immutable.Set<String> topics = (scala.collection.immutable.Set<String>)
                    scala.collection.immutable.Set$.MODULE$.apply(
                            JavaConverters.asScalaBufferConverter(Collections.singletonList(topic)).asScala().toSeq());
            JavaConverters.asJavaIterable(kafkaZkClient.getFullReplicaAssignmentForTopics(topics))
                    .forEach(e -> existingAssignment.put(e._1.partition(), e._2));
            LOG.info("DataBalancer: Adjusting topic {} partition count to {}", topic, partitionCount);
            Seq<BrokerMetadata> brokerMetadatas = adminZkClient.getBrokerMetadatas(
                    RackAwareMode.Safe$.MODULE$, Option$.MODULE$.empty());
            adminZkClient.addPartitions(topic, existingAssignment, brokerMetadatas, partitionCount,
                    Option$.MODULE$.empty(), false, Option$.MODULE$.empty(), Option$.MODULE$.empty());
            LOG.info("Kafka topic " + topic + " now has " + partitionCount + " partitions.");
        }
    }

    public static final class SbkTopicConfig {

        public final long minRetentionTimeMs;
        public final int replicationFactor;
        public final String topic;
        public final int partitionCount;
        public final String cleanupPolicy;

        public SbkTopicConfig(long minRetentionTimeMs,
                              int replicationFactor,
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
        private int replicationFactor;
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
                                                          int defaultReplicationFactor) {
            String replicationFactor = (String) config.get(replicationFactorConfig);
            this.replicationFactor = replicationFactor == null || replicationFactor.isEmpty()
                    ? defaultReplicationFactor
                    : Integer.parseInt(replicationFactor);
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
