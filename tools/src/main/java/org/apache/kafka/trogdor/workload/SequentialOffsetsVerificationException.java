package org.apache.kafka.trogdor.workload;

import org.apache.kafka.common.TopicPartition;

public class SequentialOffsetsVerificationException extends RuntimeException {
    SequentialOffsetsVerificationException(TopicPartition topicPartition, long expected,
                                           long witnessed) {
        super(String.format("Record failed SequentialOffsetsVerification on %s, "
                        + "expected offset %d but witnessed offset %d", topicPartition.toString(),
                expected, witnessed));
    }
}
