/*
 Copyright 2020 Confluent Inc.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.RemoveBrokersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class RemoveBrokersResponse extends AbstractResponse {

    private final RemoveBrokersResponseData data;

    public RemoveBrokersResponse(Struct struct) {
        this(struct, ApiKeys.REMOVE_BROKERS.latestVersion());
    }

    public RemoveBrokersResponse(RemoveBrokersResponseData responseData) {
        this.data = responseData;
    }

    RemoveBrokersResponse(Struct struct, short version) {
        this.data = new RemoveBrokersResponseData(struct, version);
    }

    public static RemoveBrokersResponse parse(ByteBuffer buffer, short version) {
        return new RemoveBrokersResponse(ApiKeys.REMOVE_BROKERS.parseResponse(version, buffer), version);
    }

    public RemoveBrokersResponseData data() {
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
            return Collections.singletonMap(Errors.forCode(data.errorCode()), data.brokersToRemove().size());
        return errorCounts(data.brokersToRemove().stream().map(p -> Errors.forCode(p.errorCode())).collect(Collectors.toList()));
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
