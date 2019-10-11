/*
 * Copyright 2019 Confluent Inc.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.ReplicaStatusResponseData;
import org.apache.kafka.common.message.ReplicaStatusResponseData.ReplicaStatusTopicResponse;
import org.apache.kafka.common.message.ReplicaStatusResponseData.ReplicaStatusPartitionResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

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
}
