/*
Copyright 2020 Confluent Inc.
*/
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.StartRebalanceRequestData.BrokerId;
import org.apache.kafka.common.message.StartRebalanceResponseData;
import org.apache.kafka.common.message.StartRebalanceResponseData.DrainBrokerResponse;
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

public class StartRebalanceResponseTest {

    @Test
    public void testErrorCountsFromGetErrorResponse() {
        Set<BrokerId> brokerIds = new HashSet<>();
        brokerIds.add(new BrokerId().setBrokerId(1));
        brokerIds.add(new BrokerId().setBrokerId(2));
        StartRebalanceRequest request = new StartRebalanceRequest.Builder(brokerIds).build();
        StartRebalanceResponse response = request.getErrorResponse(0, Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
        assertEquals(Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 2), response.errorCounts());
    }

    @Test
    public void testErrorCountsWithTopLevelError() {
        List<DrainBrokerResponse> drainBrokerResponses = new ArrayList<>();
        drainBrokerResponses.add(new DrainBrokerResponse()
                .setBrokerId(1)
                .setErrorCode(Errors.BROKER_NOT_AVAILABLE.code())
        );
        drainBrokerResponses.add(new DrainBrokerResponse()
                .setBrokerId(2)
                .setErrorCode(Errors.REASSIGNMENT_IN_PROGRESS.code())
        );

        StartRebalanceResponse response = new StartRebalanceResponse(
                new StartRebalanceResponseData()
                .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                .setBrokersToDrain(drainBrokerResponses)
        );
        assertEquals(Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 2), response.errorCounts());
    }

    @Test
    public void testErrorCountsNoTopLevelError() {
        List<DrainBrokerResponse> drainBrokerResponses = new ArrayList<>();
        drainBrokerResponses.add(new DrainBrokerResponse()
                .setBrokerId(1)
                .setErrorCode(Errors.BROKER_NOT_AVAILABLE.code())
        );
        drainBrokerResponses.add(new DrainBrokerResponse()
                .setBrokerId(2)
                .setErrorCode(Errors.REASSIGNMENT_IN_PROGRESS.code())
        );
        drainBrokerResponses.add(new DrainBrokerResponse()
                .setBrokerId(3)
                .setErrorCode(Errors.REASSIGNMENT_IN_PROGRESS.code())
        );
        drainBrokerResponses.add(new DrainBrokerResponse()
                .setBrokerId(4)
        );
        StartRebalanceResponse response = new StartRebalanceResponse(
                new StartRebalanceResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setBrokersToDrain(drainBrokerResponses)
        );

        Map<Errors, Integer> errorCounts = response.errorCounts();
        assertEquals(3, errorCounts.size());
        assertEquals(1, errorCounts.get(Errors.NONE).intValue());
        assertEquals(1, errorCounts.get(Errors.BROKER_NOT_AVAILABLE).intValue());
        assertEquals(2, errorCounts.get(Errors.REASSIGNMENT_IN_PROGRESS).intValue());
    }

    @Test
    public void testToString() {
        List<DrainBrokerResponse> drainBrokerResponses = new ArrayList<>();
        drainBrokerResponses.add(new DrainBrokerResponse()
                .setBrokerId(1)
        );
        drainBrokerResponses.add(new DrainBrokerResponse()
                .setBrokerId(2)
                .setErrorCode(Errors.REASSIGNMENT_IN_PROGRESS.code())
        );
        StartRebalanceResponse response = new StartRebalanceResponse(
                new StartRebalanceResponseData()
                        .setBrokersToDrain(drainBrokerResponses)
        );

        String responseStr = response.toString();
        assertTrue(responseStr.contains(StartRebalanceResponse.class.getSimpleName()));
        assertTrue(responseStr.contains(drainBrokerResponses.toString()));
        assertTrue(responseStr.contains("errorCode=" + Errors.NONE.code()));
    }

}
