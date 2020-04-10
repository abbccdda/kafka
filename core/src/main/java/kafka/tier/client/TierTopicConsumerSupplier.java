/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.client;

import kafka.tier.topic.TierTopicManagerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class TierTopicConsumerSupplier implements Supplier<Consumer<byte[], byte[]>> {
    private static final String CLIENT_TYPE = "consumer";
    private static final String SEPARATOR = "-";

    private final TierTopicManagerConfig config;
    private final String clientIdSuffix;
    private final AtomicInteger instanceId = new AtomicInteger(0);

    public TierTopicConsumerSupplier(TierTopicManagerConfig config, String clientIdSuffix) {
        this.config = config;
        this.clientIdSuffix = clientIdSuffix;
    }

    @Override
    public Consumer<byte[], byte[]> get() {
        String clientId = clientId(config.clusterId, config.brokerId, instanceId.getAndIncrement(), clientIdSuffix);
        return new KafkaConsumer<>(properties(config, clientId));
    }

    // visible for testing
    static Properties properties(TierTopicManagerConfig config, String clientId) {
        Properties properties = new Properties();

        for (Map.Entry<String, Object> configEntry : config.interBrokerClientConfigs.get().entrySet())
            properties.put(configEntry.getKey(), configEntry.getValue());

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // Explicitly remove the metrics reporter configuration, as it is not expected to be configured for tier topic clients
        properties.remove(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG);
        return properties;
    }

    // visible for testing
    static String clientId(String clusterId, int brokerId, int instanceId, String clientIdSuffix) {
        return TierTopicClient.clientIdPrefix(CLIENT_TYPE) + SEPARATOR +
                clusterId + SEPARATOR +
                brokerId + SEPARATOR +
                instanceId + SEPARATOR +
                clientIdSuffix;
    }
}
