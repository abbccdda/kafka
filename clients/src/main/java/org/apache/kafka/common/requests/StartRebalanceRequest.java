/*
 Copyright 2020 Confluent Inc.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.StartRebalanceRequestData;
import org.apache.kafka.common.message.StartRebalanceResponseData.DrainBrokerResponse;
import org.apache.kafka.common.message.StartRebalanceRequestData.BrokerId;
import org.apache.kafka.common.message.StartRebalanceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class StartRebalanceRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<StartRebalanceRequest> {
        private final StartRebalanceRequestData data;

        public Builder(Set<BrokerId> brokersToDrain) {
            super(ApiKeys.START_REBALANCE);
            this.data = new StartRebalanceRequestData().setBrokersToDrain(new ArrayList<>(brokersToDrain));
        }

        @Override
        public StartRebalanceRequest build(short version) {
            return new StartRebalanceRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private StartRebalanceRequestData data;

    private StartRebalanceRequest(StartRebalanceRequestData data, short version) {
        super(ApiKeys.START_REBALANCE, version);
        this.data = data;
    }

    StartRebalanceRequest(Struct struct, short version) {
        super(ApiKeys.START_REBALANCE, version);
        this.data = new StartRebalanceRequestData(struct, version);
    }

    public static StartRebalanceRequest parse(ByteBuffer buffer, short version) {
        return new StartRebalanceRequest(ApiKeys.START_REBALANCE.parseRequest(version, buffer), version);
    }

    public StartRebalanceRequestData data() {
        return data;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public StartRebalanceResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        List<DrainBrokerResponse> brokerResponses = new ArrayList<>();

        if (data.brokersToDrain() != null) {
            for (BrokerId brokerId : data.brokersToDrain()) {
                brokerResponses.add(
                    new DrainBrokerResponse()
                        .setBrokerId(brokerId.brokerId())
                        .setErrorCode(apiError.error().code())
                        .setErrorMessage(apiError.message())
                );
            }
        }
        StartRebalanceResponseData responseData = new StartRebalanceResponseData()
                .setBrokersToDrain(brokerResponses)
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message())
                .setThrottleTimeMs(throttleTimeMs);

        return new StartRebalanceResponse(responseData);
    }
}
