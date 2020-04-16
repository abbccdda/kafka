/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.Confluent;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

/**
 * The result of {@link ConfluentAdmin#createClusterLinks(Collection, CreateClusterLinksOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@Confluent
@InterfaceStability.Evolving
public class CreateClusterLinksResult {

    private final Map<String, KafkaFuture<Void>> result;

    public CreateClusterLinksResult(Map<String, KafkaFuture<Void>> result) {
        this.result = result;
    }

    /**
     * Returns a map from link name to the future status of the operation.
     */
    public Map<String, KafkaFuture<Void>> result() {
        return result;
    }

    /**
     * Returns a future which succeeds only if all cluster link creation requests succeed.
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(result.values().toArray(new KafkaFuture[0]));
    }
}
