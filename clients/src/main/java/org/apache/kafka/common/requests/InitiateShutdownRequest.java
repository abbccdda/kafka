/*
 Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.InitiateShutdownRequestData;
import org.apache.kafka.common.message.InitiateShutdownResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class InitiateShutdownRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<InitiateShutdownRequest> {
        private final InitiateShutdownRequestData data;

        public Builder(long brokerEpoch) {
            super(ApiKeys.INITIATE_SHUTDOWN);
            this.data = new InitiateShutdownRequestData().setBrokerEpoch(brokerEpoch);
        }

        @Override
        public InitiateShutdownRequest build(short version) {
            return new InitiateShutdownRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private InitiateShutdownRequestData data;

    private InitiateShutdownRequest(InitiateShutdownRequestData data, short version) {
        super(ApiKeys.INITIATE_SHUTDOWN, version);
        this.data = data;
    }

    InitiateShutdownRequest(Struct struct, short version) {
        super(ApiKeys.INITIATE_SHUTDOWN, version);
        this.data = new InitiateShutdownRequestData(struct, version);
    }

    public static InitiateShutdownRequest parse(ByteBuffer buffer, short version) {
        return new InitiateShutdownRequest(ApiKeys.INITIATE_SHUTDOWN.parseRequest(version, buffer), version);
    }

    public InitiateShutdownRequestData data() {
        return data;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public InitiateShutdownResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);

        InitiateShutdownResponseData responseData = new InitiateShutdownResponseData()
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message())
                .setThrottleTimeMs(throttleTimeMs);

        return new InitiateShutdownResponse(responseData);
    }
}
