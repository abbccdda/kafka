/*
 Copyright 2020 Confluent Inc.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.RemoveBrokersRequestData;
import org.apache.kafka.common.message.RemoveBrokersResponseData.RemoveBrokerResponse;
import org.apache.kafka.common.message.RemoveBrokersRequestData.BrokerId;
import org.apache.kafka.common.message.RemoveBrokersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RemoveBrokersRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<RemoveBrokersRequest> {
        private final RemoveBrokersRequestData data;

        public Builder(Set<BrokerId> brokersToRemove) {
            super(ApiKeys.REMOVE_BROKERS);
            this.data = new RemoveBrokersRequestData().setBrokersToRemove(new ArrayList<>(brokersToRemove));
        }

        @Override
        public RemoveBrokersRequest build(short version) {
            return new RemoveBrokersRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private RemoveBrokersRequestData data;

    private RemoveBrokersRequest(RemoveBrokersRequestData data, short version) {
        super(ApiKeys.REMOVE_BROKERS, version);
        this.data = data;
    }

    RemoveBrokersRequest(Struct struct, short version) {
        super(ApiKeys.REMOVE_BROKERS, version);
        this.data = new RemoveBrokersRequestData(struct, version);
    }

    public static RemoveBrokersRequest parse(ByteBuffer buffer, short version) {
        return new RemoveBrokersRequest(ApiKeys.REMOVE_BROKERS.parseRequest(version, buffer), version);
    }

    public RemoveBrokersRequestData data() {
        return data;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public RemoveBrokersResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        List<RemoveBrokerResponse> brokerResponses = new ArrayList<>();

        if (data.brokersToRemove() != null) {
            for (BrokerId brokerId : data.brokersToRemove()) {
                brokerResponses.add(
                    new RemoveBrokerResponse()
                        .setBrokerId(brokerId.brokerId())
                        .setErrorCode(apiError.error().code())
                        .setErrorMessage(apiError.message())
                );
            }
        }
        RemoveBrokersResponseData responseData = new RemoveBrokersResponseData()
                .setBrokersToRemove(brokerResponses)
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message())
                .setThrottleTimeMs(throttleTimeMs);

        return new RemoveBrokersResponse(responseData);
    }
}
