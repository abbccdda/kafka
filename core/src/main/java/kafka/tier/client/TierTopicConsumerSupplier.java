/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.client;

import kafka.tier.topic.TierTopicManagerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class TierTopicConsumerSupplier implements Supplier<Consumer<byte[], byte[]>> {
    private static final String SEPARATOR = "-";
    private static final String CLIENT_ID_PREFIX = "__TierConsumer";

    private final TierTopicManagerConfig config;
    private final String clientIdSuffix;
    private final AtomicInteger instanceId = new AtomicInteger(0);

    public TierTopicConsumerSupplier(TierTopicManagerConfig config, String clientIdSuffix) {
        this.config = config;
        this.clientIdSuffix = clientIdSuffix;
    }

    @Override
    public Consumer<byte[], byte[]> get() {
        String clientId = clientId(config, instanceId.getAndIncrement(), clientIdSuffix);
        return new KafkaConsumer<>(properties(config.bootstrapServersSupplier.get(), clientId));
    }

    private static Properties properties(String bootstrapServers, String clientId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return properties;
    }

    private static String clientId(TierTopicManagerConfig config, int instanceId, String clientIdSuffix) {
        return CLIENT_ID_PREFIX + SEPARATOR +
                config.clusterId + SEPARATOR +
                config.brokerId + SEPARATOR +
                instanceId + SEPARATOR +
                clientIdSuffix;
    }
}
