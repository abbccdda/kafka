/*
 * Copyright 2019 Confluent Inc.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.ReplicaStatusResponseData;
import org.apache.kafka.common.message.ReplicaStatusResponseData.ReplicaStatusPartitionResponse;
import org.apache.kafka.common.message.ReplicaStatusResponseData.ReplicaStatusReplicaResponse;
import org.apache.kafka.common.message.ReplicaStatusResponseData.ReplicaStatusTopicResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.replica.ReplicaStatus;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ReplicaStatusResponse extends AbstractResponse {
    private final ReplicaStatusResponseData data;

    public ReplicaStatusResponse(ReplicaStatusResponseData data) {
        this.data = data;
    }

    public ReplicaStatusResponse(Struct struct, short version) {
        this.data = new ReplicaStatusResponseData(struct, version);
    }

    public ReplicaStatusResponseData data() {
        return data;
    }

    public static ReplicaStatusResponse parse(ByteBuffer buffer, short version) {
        return new ReplicaStatusResponse(ApiKeys.REPLICA_STATUS.responseSchema(version).read(buffer), version);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();

        incrementErrorCount(data.errorCode(), errorCounts);
        for (ReplicaStatusTopicResponse topicResponse : data.topics()) {
            for (ReplicaStatusPartitionResponse partitionResponse : topicResponse.partitions()) {
                incrementErrorCount(partitionResponse.errorCode(), errorCounts);
            }
        }
        return errorCounts;
    }

    private void incrementErrorCount(short errorCode, Map<Errors, Integer> errorCounts) {
        Errors error = Errors.forCode(errorCode);
        errorCounts.put(error, errorCounts.getOrDefault(error, 0) + 1);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public void complete(Map<TopicPartition, KafkaFutureImpl<List<ReplicaStatus>>> result) {
        for (ReplicaStatusTopicResponse topicResponse : data.topics()) {
            for (ReplicaStatusPartitionResponse partitionResponse : topicResponse.partitions()) {
                TopicPartition topicPartition =
                        new TopicPartition(topicResponse.name(), partitionResponse.partitionIndex());
                KafkaFutureImpl<List<ReplicaStatus>> future = result.get(topicPartition);
                Objects.requireNonNull(future, "Replica status future must not be null for " + topicPartition);

                Errors error = Errors.forCode(partitionResponse.errorCode());
                if (error != Errors.NONE) {
                    future.completeExceptionally(error.exception());
                    continue;
                }

                List<ReplicaStatus> replicas = new ArrayList<ReplicaStatus>(partitionResponse.replicas().size());
                for (ReplicaStatusReplicaResponse replicaResponse : partitionResponse.replicas()) {
                    replicas.add(new ReplicaStatus(
                        replicaResponse.id(),
                        replicaResponse.isLeader(),
                        replicaResponse.isObserver(),
                        replicaResponse.isIsrEligible(),
                        replicaResponse.isInIsr(),
                        replicaResponse.isCaughtUp(),
                        replicaResponse.logStartOffset(),
                        replicaResponse.logEndOffset(),
                        replicaResponse.lastCaughtUpTimeMs(),
                        replicaResponse.lastFetchTimeMs(),
                        Optional.ofNullable(replicaResponse.linkName())));
                }
                future.complete(replicas);
            }
        }
    }
}
