/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.DescribeBrokerRemovalsRequestData;
import org.apache.kafka.common.message.DescribeBrokerRemovalsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;

public class DescribeBrokerRemovalsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DescribeBrokerRemovalsRequest> {
        private final DescribeBrokerRemovalsRequestData data;

        public Builder() {
            super(ApiKeys.DESCRIBE_BROKER_REMOVALS);
            this.data = new DescribeBrokerRemovalsRequestData();
        }

        @Override
        public DescribeBrokerRemovalsRequest build(short version) {
            return new DescribeBrokerRemovalsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private DescribeBrokerRemovalsRequestData data;

    public DescribeBrokerRemovalsRequest(DescribeBrokerRemovalsRequestData data, short apiVersion) {
        super(ApiKeys.DESCRIBE_BROKER_REMOVALS, apiVersion);
        this.data = data;
    }

    public DescribeBrokerRemovalsRequest(Struct struct, short apiVersion) {
        super(ApiKeys.DESCRIBE_BROKER_REMOVALS, apiVersion);
        this.data = new DescribeBrokerRemovalsRequestData(struct, apiVersion);
    }

    public static DescribeBrokerRemovalsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeBrokerRemovalsRequest(ApiKeys.DESCRIBE_BROKER_REMOVALS.parseRequest(version, buffer), version);
    }

    public DescribeBrokerRemovalsRequestData data() {
        return data;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public DescribeBrokerRemovalsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);

        DescribeBrokerRemovalsResponseData responseData = new DescribeBrokerRemovalsResponseData()
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message())
                .setRemovedBrokers(Collections.emptyList())
                .setThrottleTimeMs(throttleTimeMs);

        return new DescribeBrokerRemovalsResponse(responseData);
    }
}
