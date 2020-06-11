/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer;

import io.confluent.databalancer.persistence.ApiStatePersistenceStore;

public interface DataBalanceEngineContext {

    /**
     * Return persistence store associated with this DataBalanceEngineContext.
     */
    ApiStatePersistenceStore getPersistenceStore();

}
