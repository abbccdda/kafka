/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.client;

import kafka.tier.topic.TierTopicManagerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class TierTopicProducerSupplier implements Supplier<Producer<byte[], byte[]>> {
    private static final String CLIENT_TYPE = "producer";
    private static final String SEPARATOR = "-";

    private final TierTopicManagerConfig config;
    private final AtomicInteger instanceId = new AtomicInteger(0);

    public TierTopicProducerSupplier(TierTopicManagerConfig config) {
        this.config = config;
    }

    @Override
    public Producer<byte[], byte[]> get() {
        String clientId = clientId(config.clusterId, config.brokerId, instanceId.getAndIncrement());
        return new KafkaProducer<>(properties(config, clientId));
    }

    // visible for testing
    public static String clientId(String clusterId, int brokerId, long instanceId) {
        return TierTopicClient.clientIdPrefix(CLIENT_TYPE) + SEPARATOR +
                clusterId + SEPARATOR +
                brokerId + SEPARATOR +
                instanceId;
    }

    private static Properties properties(TierTopicManagerConfig config, String clientId) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServersSupplier.get());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, Integer.toString(2000));
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.requestTimeoutMs);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        return properties;
    }
}
