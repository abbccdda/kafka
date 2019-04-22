/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.client;

import kafka.tier.TierTopicManagerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Properties;

public class ConsumerBuilder implements TierTopicConsumerBuilder {
    private static final String CLIENT_ID_PREFIX = "__kafka.tiertopicmanager";
    private final TierTopicManagerConfig config;

    public ConsumerBuilder(TierTopicManagerConfig config) {
        this.config = config;
    }

    /**
     * Setup the internal kafka consumer for the tier topic manager.
     *
     * @param topicName the tier topic name.
     * @return a KafkaConsumer
     */
    public KafkaConsumer<byte[], byte[]> setupConsumer(String bootstrapServers,
                                                       String topicName,
                                                       String clientIdSuffix) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,
                clientId(config.clusterId, config.brokerId, clientIdSuffix));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return new KafkaConsumer<>(properties);
    }

    /**
     * Kafka clientId for materializing the tier topic.
     * This is set primarily for useful metrics, as the internal kafka consumer is the
     * primary source of tier topic manager metrics.
     */
    private static String clientId(String clusterId, int brokerId, String clientIdSuffix) {
        return String.format("%s.%s.%s.%s", CLIENT_ID_PREFIX, clusterId, brokerId, clientIdSuffix);
    }
}
