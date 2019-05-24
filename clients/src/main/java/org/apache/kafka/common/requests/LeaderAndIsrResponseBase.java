package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;

import java.util.Map;

public interface LeaderAndIsrResponseBase {
    Map<TopicPartition, Errors> responses();

    Errors error();

    Map<Errors, Integer> errorCounts();
}
