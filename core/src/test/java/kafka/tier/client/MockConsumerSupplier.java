/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.client;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MockConsumerSupplier<K, V> implements Supplier<Consumer<K, V>> {
    private final MockProducer<K, V> producer;
    private final String clientIdSuffix;
    private final Set<TopicPartition> topicPartitions;
    private final Map<String, TierMockConsumer<K, V>> consumers = new HashMap<>();
    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> logs = new HashMap<>();

    private int instanceId = 0;
    private int numRecordsProcessed = 0;

    public MockConsumerSupplier(String clientIdSuffix, Set<TopicPartition> topicPartitions, MockProducer<K, V> producer) {
        this.clientIdSuffix = clientIdSuffix;
        this.topicPartitions = topicPartitions;
        this.producer = producer;
    }

    @Override
    public synchronized Consumer<K, V> get() {
        String clientId = instanceId + clientIdSuffix;
        instanceId++;

        TierMockConsumer<K, V> tierMockConsumer = new TierMockConsumer<>(topicPartitions);
        tierMockConsumer.consumeTillEnd(logs);
        consumers.put(clientId, tierMockConsumer);

        return tierMockConsumer.consumer;
    }

    public synchronized void moveRecordsFromProducer() {
        List<ProducerRecord<K, V>> toProcess = producer.history().subList(numRecordsProcessed, producer.history().size());

        for (ProducerRecord<K, V> record : toProcess) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            logs.putIfAbsent(topicPartition, new LinkedList<>());

            List<ConsumerRecord<K, V>> log = logs.get(topicPartition);
            log.add(new ConsumerRecord<>(record.topic(),
                    record.partition(),
                    log.size(),
                    record.key(),
                    record.value()));

            numRecordsProcessed += 1;
        }

        removeClosed();

        for (TierMockConsumer<K, V> consumer : consumers.values())
            consumer.consumeTillEnd(logs);
    }

    /**
     * Sets the mock consumer with clientIdSuffix to throw the provided exception on position() call.
     * The mock consumer resets the exception after the first poll.
     * @param exception exception thrown by mock consumer on position() call
     */
    public synchronized void setConsumerPositionException(KafkaException exception) {
        consumers.values()
                .forEach(tierMockConsumer -> tierMockConsumer.consumer.setPositionException(exception));
    }

    public synchronized List<Consumer<K, V>> consumers() {
        return consumers.values()
                .stream()
                .map(tierMockConsumer -> tierMockConsumer.consumer)
                .collect(Collectors.toList());
    }

    private void removeClosed() {
        consumers.keySet()
                .removeIf(clientId -> consumers.get(clientId).consumer.closed());
    }

    private static class TierMockConsumer<K, V> {
        private final MockConsumer<K, V> consumer;

        public TierMockConsumer(Set<TopicPartition> topicPartitions) {
            Map<TopicPartition, Long> beginningOffsets = new HashMap<>();

            topicPartitions.stream()
                    .forEach(topicPartition -> beginningOffsets.put(topicPartition, 0L));

            consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
            consumer.updateBeginningOffsets(beginningOffsets);
            consumer.updateEndOffsets(beginningOffsets);
        }

        public void consumeTillEnd(Map<TopicPartition, List<ConsumerRecord<K, V>>> logs) {
            Set<TopicPartition> assignment = consumer.assignment();

            for (TopicPartition topicPartition : assignment) {
                List<ConsumerRecord<K, V>> records = logs.get(topicPartition);

                if (records != null)
                    consumeTillEnd(topicPartition, records);
            }
        }

        private void consumeTillEnd(TopicPartition topicPartition, List<ConsumerRecord<K, V>> log) {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
            long currentEndOffset = endOffsets.getOrDefault(topicPartition, 0L);

            List<ConsumerRecord<K, V>> records = log.subList((int) currentEndOffset, log.size());

            for (ConsumerRecord<K, V> record : records)
                consumer.addRecord(record);

            endOffsets.put(topicPartition, (long) log.size());
            consumer.updateEndOffsets(endOffsets);
        }
    }
}
