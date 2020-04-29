/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RemoveBrokersResult {
    private final Map<Integer, KafkaFuture<Void>>  futures;

    protected RemoveBrokersResult(Map<Integer, KafkaFuture<Void>> futures) {
        this.futures = futures;
    }

    /**
     *  Return a map of broker ids to futures which can be used to check the status of individual broker removals.
     */
    public Map<Integer, KafkaFuture<Void>> values() {
        return futures;
    }

    /**
     * Return a future which succeeds only if all the broker removal operations succeed.
     */
    public KafkaFuture<List<Integer>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).
            thenApply(v -> new ArrayList<>(futures.keySet()));
    }
}
