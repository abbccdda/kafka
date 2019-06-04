package org.apache.kafka.trogdor.workload;

import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class RecordBatchVerifierTest {

    @Test
    public void testSequentialOffsetRecordBatchVerifierNoFailure() {
        RecordBatchVerifier recordBatchVerifier = new SequentialOffsetsRecordBatchVerifier();
        // Test that empty records do not cause an exception
        recordBatchVerifier.verifyRecords(ConsumerRecords.empty());

        TopicPartition topicPartition0 = new TopicPartition("foo", 0);
        TopicPartition topicPartition1 = new TopicPartition("foo", 1);

        HashMap<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap =
                new HashMap<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>() {{
                    put(topicPartition0, Arrays.asList(
                            new ConsumerRecord<>(topicPartition0.topic(),
                                    topicPartition0.partition(), 0, null, null),
                            new ConsumerRecord<>(topicPartition0.topic(),
                                    topicPartition0.partition(), 1, null, null)));
                    put(topicPartition1, Arrays.asList(
                            new ConsumerRecord<>(topicPartition1.topic(),
                                    topicPartition1.partition(), 50, null, null),
                            new ConsumerRecord<>(topicPartition1.topic(),
                                    topicPartition1.partition(), 51, null, null)));
                }};

        ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(recordsMap);
        recordBatchVerifier.verifyRecords(consumerRecords);
    }

    @Test(expected = SequentialOffsetsVerificationException.class)
    public void testSequentialOffsetRecordBatchVerifierFailure() {
        RecordBatchVerifier recordBatchVerifier = new SequentialOffsetsRecordBatchVerifier();
        // Test that empty records do not cause an exception
        recordBatchVerifier.verifyRecords(ConsumerRecords.empty());

        TopicPartition topicPartition0 = new TopicPartition("foo", 0);

        HashMap<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap =
                new HashMap<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>() {{
                    put(topicPartition0, Arrays.asList(
                            new ConsumerRecord<>(topicPartition0.topic(),
                                    topicPartition0.partition(), 0, null, null),
                            new ConsumerRecord<>(topicPartition0.topic(),
                                    topicPartition0.partition(), 2, null, null)));
                }};

        ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(recordsMap);
        recordBatchVerifier.verifyRecords(consumerRecords);
    }

    @Test
    public void testSequentialOffsetRecordBatchVerifierClearsPreviousOffsetsOnPartitionAssignment() {
        RecordBatchVerifier recordBatchVerifier = new SequentialOffsetsRecordBatchVerifier();

        TopicPartition topicPartition0 = new TopicPartition("foo", 0);

        HashMap<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap =
            new HashMap<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>() {{
                put(topicPartition0, Collections.singletonList(
                    new ConsumerRecord<>(topicPartition0.topic(),
                        topicPartition0.partition(), 0, null, null)));
            }};

        recordBatchVerifier.verifyRecords(new ConsumerRecords<>(recordsMap));

        recordBatchVerifier.onPartitionsAssigned(null);

        recordsMap = new HashMap<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>() {{
            put(topicPartition0, Collections.singletonList(
                new ConsumerRecord<>(topicPartition0.topic(),
                    topicPartition0.partition(), 2, null, null)));
        }};

        recordBatchVerifier.verifyRecords(new ConsumerRecords<>(recordsMap));
    }
}
