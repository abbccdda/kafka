/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.client;

import kafka.tier.TierTopicManagerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.Properties;

public class ProducerBuilder implements TierTopicProducerBuilder {
    private static final String CLIENT_ID_PREFIX = "__kafka.tiertopicmanager";
    private final TierTopicManagerConfig config;

    public ProducerBuilder(TierTopicManagerConfig config) {
        this.config = config;
    }

     /**
     * Setup the internal kafka producer for the tier topic manager.
     * @return a KafkaProducer
     */
    public KafkaProducer<byte[], byte[]> setupProducer(String bootstrapServers) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, Integer.toString(2000));
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId(config.clusterId, config.brokerId));
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.requestTimeoutMs);

        return new KafkaProducer<>(properties);
    }

    /**
     * Kafka clientId for materializing the tier topic.
     * This is set primarily for useful metrics, as the internal kafka producer is the
     * primary source of tier topic manager metrics.
     */
    private static String clientId(String clusterId, int brokerId) {
        return String.format("%s.%s.%s", CLIENT_ID_PREFIX, clusterId, brokerId);
    }

    public static boolean tierProducer(String clientId) {
        return clientId.startsWith(CLIENT_ID_PREFIX);
    }
}
