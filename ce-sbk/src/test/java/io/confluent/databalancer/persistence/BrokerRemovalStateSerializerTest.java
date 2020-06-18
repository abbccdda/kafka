/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.persistence;

import io.confluent.databalancer.operation.BrokerRemovalStateMachine;
import io.confluent.databalancer.record.RemoveBroker;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BrokerRemovalStateSerializerTest {

    @Test
    public void testSerializationAndDeserialization() {
        for (BrokerRemovalStateMachine.BrokerRemovalState state: BrokerRemovalStateMachine.BrokerRemovalState.values()) {
            RemoveBroker.RemovalState serializedState = BrokerRemovalStateSerializer.serialize(state);
            assertEquals(state.toString(),
                    state,
                    BrokerRemovalStateSerializer.deserialize(serializedState));
        }
    }
}