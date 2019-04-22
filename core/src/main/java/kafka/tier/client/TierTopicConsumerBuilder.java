/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.client;

import org.apache.kafka.clients.consumer.Consumer;

public interface TierTopicConsumerBuilder {
    Consumer<byte[], byte[]> setupConsumer(String bootstrapServers,
                                           String topicName,
                                           String clientIdSuffix);
}
