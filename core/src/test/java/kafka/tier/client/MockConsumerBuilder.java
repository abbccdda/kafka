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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MockConsumerBuilder implements TierTopicConsumerBuilder {
    private final short numPartitions;
    private final MockProducer<byte[], byte[]> producer;
    // MockConsumer map with key as clientIdSuffix
    private final Map<String, MockConsumer<byte[], byte[]>> consumers = new HashMap<>();
    private final ArrayList<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    private long position = 0;

    public MockConsumerBuilder(Short numPartitions, MockProducer<byte[], byte[]> producer) {
        this.numPartitions = numPartitions;
        this.producer = producer;
    }

    /**
     * Setup the internal kafka consumer for the tier topic manager.
     * @param topicName the tier topic name.
     * @return a KafkaConsumer
     */
    public Consumer<byte[], byte[]> setupConsumer(String bootstrapServers,
                                                  String topicName,
                                                  String clientIdSuffix) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        consumer.assign(partitions(topicName));

        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition(topicName, 0), 0L);
        consumer.updateBeginningOffsets(offsets);
        consumer.updateEndOffsets(offsets);

        for (ConsumerRecord<byte[], byte[]> record: records) {
            consumer.addRecord(record);
            HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
            endOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            consumer.updateEndOffsets(endOffsets);
        }

        consumers.put(clientIdSuffix, consumer);

        return consumer;
    }

    public void moveRecordsFromProducer() {
        while (logEndOffset() < producer.history().size()) {
            ProducerRecord<byte[], byte[]> record = producer.history().get((int) logEndOffset());
            addRecord(new ConsumerRecord<byte[], byte[]>(
                    record.topic(),
                    record.partition(),
                    logEndOffset(),
                    record.key(),
                    record.value()));
        }
    }

    /** 
     * Sets the mock consumer with clientIdSuffix to throw the provided exception on position() call.
     * The mock consumer resets the exception after the first poll.
     * @param clientIdSuffix the consumer clientIdSuffix
     * @param exception exception thrown by mock consumer on position() call
     */
    public synchronized void setConsumerPositionException(String clientIdSuffix, KafkaException exception) {
        MockConsumer<byte[], byte[]> consumer = consumers.get(clientIdSuffix);
        consumer.setPositionException(exception);
    }

    private void addRecord(ConsumerRecord<byte[], byte[]> record) {
        HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());

        consumers.entrySet().removeIf(entries -> entries.getValue().closed());

        records.add(record);
        for (MockConsumer<byte[], byte[]> consumer: consumers.values()) {
            consumer.addRecord(record);
            consumer.updateEndOffsets(endOffsets);
        }
        position = record.offset() + 1;
    }

    public long logEndOffset() {
        return position;
    }

    private Collection<TopicPartition> partitions(String topicName) {
        return IntStream
                .range(0, numPartitions)
                .mapToObj(part -> new TopicPartition(topicName, part))
                .collect(Collectors.toList());
    }

}
