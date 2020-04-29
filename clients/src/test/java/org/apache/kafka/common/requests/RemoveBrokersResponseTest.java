/*
Copyright 2020 Confluent Inc.
*/
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.RemoveBrokersRequestData.BrokerId;
import org.apache.kafka.common.message.RemoveBrokersResponseData;
import org.apache.kafka.common.message.RemoveBrokersResponseData.RemoveBrokerResponse;
import org.apache.kafka.common.protocol.Errors;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RemoveBrokersResponseTest {

    @Test
    public void testErrorCountsFromGetErrorResponse() {
        Set<BrokerId> brokerIds = new HashSet<>();
        brokerIds.add(new BrokerId().setBrokerId(1));
        brokerIds.add(new BrokerId().setBrokerId(2));
        RemoveBrokersRequest request = new RemoveBrokersRequest.Builder(brokerIds).build();
        RemoveBrokersResponse response = request.getErrorResponse(0, Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
        assertEquals(Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 2), response.errorCounts());
    }

    @Test
    public void testErrorCountsWithTopLevelError() {
        List<RemoveBrokerResponse> removeBrokerResponses = new ArrayList<>();
        removeBrokerResponses.add(new RemoveBrokerResponse()
                .setBrokerId(1)
                .setErrorCode(Errors.BROKER_NOT_AVAILABLE.code())
        );
        removeBrokerResponses.add(new RemoveBrokerResponse()
                .setBrokerId(2)
                .setErrorCode(Errors.REASSIGNMENT_IN_PROGRESS.code())
        );

        RemoveBrokersResponse response = new RemoveBrokersResponse(
                new RemoveBrokersResponseData()
                .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                .setBrokersToRemove(removeBrokerResponses)
        );
        assertEquals(Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 2), response.errorCounts());
    }

    @Test
    public void testErrorCountsNoTopLevelError() {
        List<RemoveBrokerResponse> removeBrokerResponses = new ArrayList<>();
        removeBrokerResponses.add(new RemoveBrokerResponse()
                .setBrokerId(1)
                .setErrorCode(Errors.BROKER_NOT_AVAILABLE.code())
        );
        removeBrokerResponses.add(new RemoveBrokerResponse()
                .setBrokerId(2)
                .setErrorCode(Errors.REASSIGNMENT_IN_PROGRESS.code())
        );
        removeBrokerResponses.add(new RemoveBrokerResponse()
                .setBrokerId(3)
                .setErrorCode(Errors.REASSIGNMENT_IN_PROGRESS.code())
        );
        removeBrokerResponses.add(new RemoveBrokerResponse()
                .setBrokerId(4)
        );
        RemoveBrokersResponse response = new RemoveBrokersResponse(
                new RemoveBrokersResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setBrokersToRemove(removeBrokerResponses)
        );

        Map<Errors, Integer> errorCounts = response.errorCounts();
        assertEquals(3, errorCounts.size());
        assertEquals(1, errorCounts.get(Errors.NONE).intValue());
        assertEquals(1, errorCounts.get(Errors.BROKER_NOT_AVAILABLE).intValue());
        assertEquals(2, errorCounts.get(Errors.REASSIGNMENT_IN_PROGRESS).intValue());
    }

    @Test
    public void testToString() {
        List<RemoveBrokerResponse> removeBrokerResponses = new ArrayList<>();
        removeBrokerResponses.add(new RemoveBrokerResponse()
                .setBrokerId(1)
        );
        removeBrokerResponses.add(new RemoveBrokerResponse()
                .setBrokerId(2)
                .setErrorCode(Errors.REASSIGNMENT_IN_PROGRESS.code())
        );
        RemoveBrokersResponse response = new RemoveBrokersResponse(
                new RemoveBrokersResponseData()
                        .setBrokersToRemove(removeBrokerResponses)
        );

        String responseStr = response.toString();
        assertTrue(responseStr.contains(RemoveBrokersResponse.class.getSimpleName()));
        assertTrue(responseStr.contains(removeBrokerResponses.toString()));
        assertTrue(responseStr.contains("errorCode=" + Errors.NONE.code()));
    }

}
