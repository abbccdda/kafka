/*
Copyright 2020 Confluent Inc.
*/
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.RemoveBrokersRequestData.BrokerId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;


public class RemoveBrokersRequestTest {

    @Test
    public void testRemoveBrokerRequestBuilder() {
        Set<BrokerId> brokerIds = new HashSet<>();
        brokerIds.add(new BrokerId().setBrokerId(2));
        brokerIds.add(new BrokerId().setBrokerId(3));
        RemoveBrokersRequest request = new RemoveBrokersRequest.Builder(brokerIds).build();
        assertEquals(new ArrayList<>(brokerIds), request.data().brokersToRemove());
    }
}
