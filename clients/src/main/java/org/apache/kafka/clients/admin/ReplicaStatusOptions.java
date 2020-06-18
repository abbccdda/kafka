/*
 * Copyright 2019 Confluent Inc.
 */

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Options for {@link ConfluentAdmin#replicaStatus(Set, ReplicaStatusOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ReplicaStatusOptions extends AbstractOptions<ReplicaStatusOptions> {

    private boolean includeLinkedReplicas = false;

    /**
     * Whether the request should also include the replica status for linked replicas.
     */
    public boolean includeLinkedReplicas() {
        return this.includeLinkedReplicas;
    }

    /**
     * Sets whether the request should also include the replica status for linked replicas.
     */
    public ReplicaStatusOptions includeLinkedReplicas(boolean includeLinkedReplicas) {
        this.includeLinkedReplicas = includeLinkedReplicas;
        return this;
    }
}
