/*
 * Copyright 2019 Confluent Inc.
 */

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.replica.ReplicaStatus;

import java.util.List;
import java.util.Map;

/**
 * The result of {@link ConfluentAdmin#replicaStatus(Set, ReplicaStatusOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ReplicaStatusResult {
    private final Map<TopicPartition, KafkaFuture<List<ReplicaStatus>>> result;

    public ReplicaStatusResult(Map<TopicPartition, KafkaFuture<List<ReplicaStatus>>> result) {
        this.result = result;
    }

    /**
     * Returns a map from topic and partition to the status of each of its replicas.
     */
    public Map<TopicPartition, KafkaFuture<List<ReplicaStatus>>> result() {
        return result;
    }

    /**
     * Returns a future which succeeds only if all replica status requests succeed.
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(result.values().toArray(new KafkaFuture[0]));
    }
}
