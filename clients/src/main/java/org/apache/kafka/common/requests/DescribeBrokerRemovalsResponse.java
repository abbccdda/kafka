/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.DescribeBrokerRemovalsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class DescribeBrokerRemovalsResponse extends AbstractResponse {

    private final DescribeBrokerRemovalsResponseData data;

    public DescribeBrokerRemovalsResponse(Struct struct) {
        this(struct, ApiKeys.DESCRIBE_BROKER_REMOVALS.latestVersion());
    }

    public DescribeBrokerRemovalsResponse(DescribeBrokerRemovalsResponseData data) {
        this.data = data;
    }

    public DescribeBrokerRemovalsResponse(Struct struct, short version) {
        this.data = new DescribeBrokerRemovalsResponseData(struct, version);
    }

    public DescribeBrokerRemovalsResponseData data() {
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

    public static DescribeBrokerRemovalsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeBrokerRemovalsResponse(ApiKeys.DESCRIBE_BROKER_REMOVALS.parseResponse(version, buffer), version);
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
