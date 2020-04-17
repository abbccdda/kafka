/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.Confluent;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.requests.ClusterLinkListing;

import java.util.Collection;

/**
 * The result of {@link ConfluentAdmin#listClusterLinks(ListClusterLinksOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@Confluent
@InterfaceStability.Evolving
public class ListClusterLinksResult {

    private final KafkaFuture<Collection<ClusterLinkListing>> result;

    public ListClusterLinksResult(KafkaFuture<Collection<ClusterLinkListing>> result) {
        this.result = result;
    }

    /**
     * Returns a future collection of cluster links.
     */
    public KafkaFuture<Collection<ClusterLinkListing>> result() {
        return result;
    }
}
