/*
 Copyright 2020 Confluent Inc.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.StartRebalanceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class StartRebalanceResponse extends AbstractResponse {

    private final StartRebalanceResponseData data;

    public StartRebalanceResponse(Struct struct) {
        this(struct, ApiKeys.START_REBALANCE.latestVersion());
    }

    public StartRebalanceResponse(StartRebalanceResponseData responseData) {
        this.data = responseData;
    }

    StartRebalanceResponse(Struct struct, short version) {
        this.data = new StartRebalanceResponseData(struct, version);
    }

    public static StartRebalanceResponse parse(ByteBuffer buffer, short version) {
        return new StartRebalanceResponse(ApiKeys.START_REBALANCE.parseResponse(version, buffer), version);
    }

    public StartRebalanceResponseData data() {
        return data;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return true;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        if (data.errorCode() != Errors.NONE.code())
            return Collections.singletonMap(Errors.forCode(data.errorCode()), data.brokersToDrain().size());
        return errorCounts(data.brokersToDrain().stream().map(p -> Errors.forCode(p.errorCode())).collect(Collectors.toList()));
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
