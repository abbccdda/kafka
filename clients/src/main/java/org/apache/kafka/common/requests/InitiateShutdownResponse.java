/*
 Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.InitiateShutdownResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class InitiateShutdownResponse extends AbstractResponse {

    private final InitiateShutdownResponseData data;

    public InitiateShutdownResponse(Struct struct) {
        this(struct, ApiKeys.INITIATE_SHUTDOWN.latestVersion());
    }

    public InitiateShutdownResponse(InitiateShutdownResponseData responseData) {
        this.data = responseData;
    }

    InitiateShutdownResponse(Struct struct, short version) {
        this.data = new InitiateShutdownResponseData(struct, version);
    }

    public static InitiateShutdownResponse parse(ByteBuffer buffer, short version) {
        return new InitiateShutdownResponse(ApiKeys.INITIATE_SHUTDOWN.parseResponse(version, buffer), version);
    }

    public InitiateShutdownResponseData data() {
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
        return Collections.singletonMap(Errors.forCode(data.errorCode()), 1);
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
