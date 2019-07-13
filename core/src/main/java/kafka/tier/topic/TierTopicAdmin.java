/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.topic;

import kafka.tier.exceptions.TierTopicIncorrectPartitionCountException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TierTopicAdmin {
    private static final Logger log = LoggerFactory.getLogger(TierTopicAdmin.class);
    private static final Map<String, String> TIER_TOPIC_CONFIG =
            Collections.unmodifiableMap(Stream.of(
                    new AbstractMap.SimpleEntry<>(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE),
                    new AbstractMap.SimpleEntry<>(TopicConfig.RETENTION_MS_CONFIG, "-1"),
                    new AbstractMap.SimpleEntry<>(TopicConfig.RETENTION_BYTES_CONFIG, "-1"))
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));

    /**
     * Create Tier Topic if one does not exist.
     * @param bootstrapServers Bootstrap Servers for the brokers where Tier Topic should be stored.
     * @param topicName The Tier Topic topic name.
     * @param partitions The number of partitions for the Tier Topic.
     * @param replication The replication factor for the Tier Topic.
     * @return boolean denoting whether the operation succeeded (true if topic already existed)
     * @throws KafkaException
     * @throws InterruptedException
     */
     public static boolean ensureTopicCreated(String bootstrapServers, String topicName,
                                              int partitions, short replication) throws KafkaException,
             TierTopicIncorrectPartitionCountException, InterruptedException {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(properties)) {
            return ensureTopicCreated(admin, topicName, partitions, replication);
        }
    }

    /**
     * Create the tier state topic if one does not exist.
     * If the tier state topic already exists, check that the partition count matches the configured
     * partition count, throwing an exception if it does not match the expected count.
     * @param admin Kafka Admin client
     * @param topicName The Tier Topic topic name.
     * @param partitions The number of partitions for the Tier Topic.
     * @param replication The replication factor for the Tier Topic.
     * @return boolean denoting whether the operation succeeded (true if topic already existed)
     * @throws KafkaException
     * @throws InterruptedException
     */
    static boolean ensureTopicCreated(AdminClient admin, String topicName, int partitions, short replication)
            throws TierTopicIncorrectPartitionCountException, InterruptedException {
        log.debug("creating tier topic {} with partitions={}, replicationFactor={}",
                topicName, partitions, replication);
        try {
            // we can't simply create the tier topic and check whether it already exists
            // as creation may be rejected if # live brokers < replication factor,
            // even if the topic already exists.
            // https://issues.apache.org/jira/browse/KAFKA-8125
            if (topicExists(admin, topicName, partitions)) {
                return true;
            } else {
                NewTopic newTopic =
                        new NewTopic(topicName, partitions, replication)
                                .configs(TIER_TOPIC_CONFIG);
                CreateTopicsResult result = admin.createTopics(Collections.singletonList(newTopic));
                result.values().get(topicName).get();
                return true;
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                log.debug("{} topic has already been created.", topicName);
                // When two brokers race to create the topic, and this broker did not succeed,
                // we should return false and retry this request
                return false;
            } else {
                log.info("{} topic could not be created by tier topic manager", topicName, e);
                return false;
            }
        }
    }

    /**
     * Determines whether the tier topic exists and validates the partition count
     * equals the expected partition count
     * @param adminClient the Kafka admin client
     * @param topicName the tier topic name
     * @return boolean denoting whether the topic exists
     */
    static boolean topicExists(AdminClient adminClient,
                               String topicName,
                               int expectedPartitionCount)
            throws InterruptedException, TierTopicIncorrectPartitionCountException, ExecutionException {
        try {
            DescribeTopicsResult describeTopicsResult =
                    adminClient.describeTopics(Collections.singleton(topicName));
            int currentPartitionCount =
                    describeTopicsResult.values().get(topicName).get().partitions().size();
            if (currentPartitionCount != expectedPartitionCount)
                throw new TierTopicIncorrectPartitionCountException(String.format("Number of "
                                + "partitions %d on tier topic: %s "
                                + "does not match the number of partitions configured %d. This may "
                                + "be due to a newly created topic seeing incomplete metadata.",
                        currentPartitionCount, topicName, expectedPartitionCount));
            return true;
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof UnknownTopicOrPartitionException)
                return false;

            throw ee;
        }
    }
}
