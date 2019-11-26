package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 * RecordBatchVerifier allows for verifying RecordBatches during ConsumeBench
 * workloads.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = SequentialOffsetsRecordBatchVerifier.class, name = "sequentialOffsets"),
})
public interface RecordBatchVerifier extends ConsumerRebalanceListener {
    /**
     * Apply verification to records returned by the consumer's last poll.
     * This is invoked on each call to the ConsumeMessages task.
     */
    void verifyRecords(ConsumerRecords<byte[], byte[]> consumerRecords);

    /**
     * Reset the tracked offset for a given partition. This is useful if the consumer
     * is seeked and the next offset is not expected to be the current tracked offset + 1
     * @param topicPartition topic partition to reset
     */
    void resetTrackedOffset(final TopicPartition topicPartition);
}

