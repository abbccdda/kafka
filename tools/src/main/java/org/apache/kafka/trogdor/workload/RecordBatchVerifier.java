package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Collection;
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
    class NoOpRecordBatchVerifier implements RecordBatchVerifier {

        @Override
        public void verifyRecords(ConsumerRecords<byte[], byte[]> consumerRecords) {
        }

        @Override
        public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        }

        @Override
        public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        }
    }

    /**
     * Apply verification to records returned by the consumer's last poll.
     * This is invoked on each call to the ConsumeMessages task.
     */
    void verifyRecords(ConsumerRecords<byte[], byte[]> consumerRecords);
}

