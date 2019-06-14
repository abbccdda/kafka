/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.topic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
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
                                          int partitions, short replication)
            throws KafkaException, InterruptedException {
        log.debug("creating tier topic {} with partitions={}, replicationFactor={}", topicName,
                partitions, replication);
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(properties)) {
            // we can't simply create the tier topic and check whether it already exists
            // as creation may be rejected if # live brokers < replication factor,
            // even if the topic already exists.
            // https://issues.apache.org/jira/browse/KAFKA-8125
            if (topicExists(admin, topicName)) {
                return true;
            } else {
                NewTopic newTopic =
                        new NewTopic(topicName, partitions, replication)
                                .configs(TIER_TOPIC_CONFIG);
                CreateTopicsResult result = admin.createTopics(Collections.singletonList(newTopic));
                result.values().get(topicName).get();
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                log.debug("{} topic has already been created.", topicName);
                return true;
            } else {
                log.info("{} topic could not be created by tier topic manager", topicName, e);
                return false;
            }
        }
        return true;
    }

    /**
     * Determines whether the tier topic exists
     * @param adminClient the Kafka admin client
     * @param topicName the tier topic name
     * @return boolean denoting whether the topic exists
     */
    private static boolean topicExists(AdminClient adminClient, String topicName) {
        try {
            adminClient.describeTopics(Collections.singleton(topicName)).values().get(topicName).get();
            return true;
        } catch (Exception e) {
            log.debug("error checking for existence of tier topic {}", topicName, e);
            return false;
        }
    }
}
