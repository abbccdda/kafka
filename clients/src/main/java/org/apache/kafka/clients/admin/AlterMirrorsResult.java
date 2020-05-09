/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.Confluent;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.requests.AlterMirrorsResponse;

import java.util.List;

/**
 * The result of {@link ConfluentAdmin#alterMirrors(Collection, AlterMirrorsOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@Confluent
@InterfaceStability.Evolving
public class AlterMirrorsResult {

    private final List<KafkaFuture<AlterMirrorsResponse.Result>> result;

    public AlterMirrorsResult(List<KafkaFuture<AlterMirrorsResponse.Result>> result) {
        this.result = result;
    }

    /**
     * Returns a list of the mirror control operations' results in the order they were provided.
     */
    public List<KafkaFuture<AlterMirrorsResponse.Result>> result() {
        return result;
    }

    /**
     * Returns a future which succeeds only if all mirror control operations succeed.
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(result.toArray(new KafkaFuture[0]));
    }
}
