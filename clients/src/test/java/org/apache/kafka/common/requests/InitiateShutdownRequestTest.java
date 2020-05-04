/*
 Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.Errors;
import org.junit.Test;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class InitiateShutdownRequestTest {
    @Test
    public void testErrorCountsFromGetErrorResponse() {
        InitiateShutdownRequest shutdownReq = new InitiateShutdownRequest.Builder(1).build();

        InitiateShutdownResponse response = shutdownReq.getErrorResponse(0, Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
        assertEquals(Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 1), response.errorCounts());
    }
}
