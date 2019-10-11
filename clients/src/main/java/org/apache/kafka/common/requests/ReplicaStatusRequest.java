/*
 * Copyright 2019 Confluent Inc.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ReplicaStatusRequestData;
import org.apache.kafka.common.message.ReplicaStatusRequestData.ReplicaStatusTopic;
import org.apache.kafka.common.message.ReplicaStatusResponseData;
import org.apache.kafka.common.message.ReplicaStatusResponseData.ReplicaStatusTopicResponse;
import org.apache.kafka.common.message.ReplicaStatusResponseData.ReplicaStatusPartitionResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicaStatusRequest extends AbstractRequest {
    private final ReplicaStatusRequestData data;
    private final short version;

    private ReplicaStatusRequest(ReplicaStatusRequestData data, short version) {
        super(ApiKeys.REPLICA_STATUS, version);
        this.data = data;
        this.version = version;
    }

    public ReplicaStatusRequest(Struct struct, short version) {
        super(ApiKeys.REPLICA_STATUS, version);
        this.data = new ReplicaStatusRequestData(struct, version);
        this.version = version;
    }

    public ReplicaStatusRequestData data() {
        return data;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable ex) {
        ApiError apiError = ApiError.fromThrowable(ex);

        ReplicaStatusResponseData response = new ReplicaStatusResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(apiError.error().code());
        for (ReplicaStatusTopic topic : data.topics()) {
            ReplicaStatusTopicResponse topicResponse = new ReplicaStatusTopicResponse().setName(topic.name());
            for (int partition : topic.partitions()) {
                topicResponse.partitions().add(new ReplicaStatusPartitionResponse()
                        .setPartitionIndex(partition)
                        .setErrorCode(apiError.error().code())
                        .setReplicas(null));
            }
            response.topics().add(topicResponse);
        }

        return new ReplicaStatusResponse(response);
    }

    public static class Builder extends AbstractRequest.Builder<ReplicaStatusRequest> {
        private final ReplicaStatusRequestData data;

        public Builder(Set<TopicPartition> partitions) {
            super(ApiKeys.REPLICA_STATUS);

            Map<String, List<Integer>> topicToPartitions = new HashMap<>();
            for (TopicPartition partition : partitions) {
                topicToPartitions.computeIfAbsent(partition.topic(), k -> new ArrayList<Integer>()).add(partition.partition());
            }

            List<ReplicaStatusRequestData.ReplicaStatusTopic> dataTopics = new ArrayList<>(topicToPartitions.size());
            for (Map.Entry<String, List<Integer>> entry : topicToPartitions.entrySet()) {
                dataTopics.add(new ReplicaStatusRequestData.ReplicaStatusTopic()
                        .setName(entry.getKey())
                        .setPartitions(entry.getValue()));
            }

            this.data = new ReplicaStatusRequestData().setTopics(dataTopics);
        }

        @Override
        public ReplicaStatusRequest build(short version) {
            return new ReplicaStatusRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
