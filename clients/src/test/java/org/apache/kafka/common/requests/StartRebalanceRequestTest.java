/*
Copyright 2020 Confluent Inc.
*/
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.StartRebalanceRequestData.BrokerId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;


public class StartRebalanceRequestTest {

    @Test
    public void testStartRebalanceRequestBuilder() {
        Set<BrokerId> brokerIds = new HashSet<>();
        brokerIds.add(new BrokerId().setBrokerId(2));
        brokerIds.add(new BrokerId().setBrokerId(3));
        StartRebalanceRequest request = new StartRebalanceRequest.Builder(brokerIds).build();
        assertEquals(new ArrayList<>(brokerIds), request.data().brokersToDrain());
    }
}
