/*
 Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.InitiateShutdownResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class InitiateShutdownResponseTest {

    @Test
    public void testErrorCountsFromGetErrorResponse() {
        InitiateShutdownResponse resp = new InitiateShutdownResponse(
                new InitiateShutdownResponseData().setErrorCode(Errors.STALE_BROKER_EPOCH.code())
        );
        Map<Errors, Integer> errors = resp.errorCounts();
        assertEquals(1, errors.size());
        assertEquals(Integer.valueOf(1), errors.get(Errors.STALE_BROKER_EPOCH));
    }
}
