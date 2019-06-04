package org.apache.kafka.trogdor.workload;

import java.util.Collection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

public class SequentialOffsetsRecordBatchVerifier implements RecordBatchVerifier {
    private final Map<TopicPartition, Long> lastOffsets = new HashMap<>();

    @Override
    public void verifyRecords(ConsumerRecords<byte[], byte[]> consumerRecords) {
        for (ConsumerRecord record : consumerRecords) {
            final TopicPartition topicPartition = new TopicPartition(record.topic(),
                    record.partition());
            if (!lastOffsets.containsKey(topicPartition)) {
                lastOffsets.put(topicPartition, record.offset());
            } else {
                final long lastOffset = lastOffsets.get(topicPartition);
                if (lastOffset + 1 != record.offset()) {
                    throw new SequentialOffsetsVerificationException(
                            topicPartition,
                            lastOffset + 1,
                            record.offset()
                    );
                } else {
                    lastOffsets.put(topicPartition, record.offset());
                }
            }
        }
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        lastOffsets.clear();
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        lastOffsets.clear();
    }
}
