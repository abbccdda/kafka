/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer;

import io.confluent.databalancer.operation.BrokerRemovalStateTracker;
import io.confluent.databalancer.persistence.ApiStatePersistenceStore;

import java.util.Map;

public interface DataBalanceEngineContext {

    /**
     * Return persistence store associated with this DataBalanceEngineContext.
     */
    ApiStatePersistenceStore getPersistenceStore();

    /**
     * Return the #{@link BrokerRemovalStateTracker} for the active broker removal operations
     */
    Map<Integer, BrokerRemovalStateTracker> getBrokerRemovalsStateTrackers();
}
