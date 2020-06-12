/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.Confluent;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for {@link ConfluentAdmin#listClusterLinks(ListClusterLinksOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@Confluent
@InterfaceStability.Evolving
public class ListClusterLinksOptions extends AbstractOptions<ListClusterLinksOptions> {

    private Optional<Collection<String>> linkNames = Optional.empty();
    private boolean includeTopics = false;

    /**
     * Names of the cluster links to include in the response, otherwise if empty, then all
     * cluster links are listed.
     */
    public Optional<Collection<String>> linkNames() {
        return this.linkNames;
    }

    /**
     * Sets the names of the cluster links to include in the response, otherwise if empty,
     * then sets all cluster links to be listed.
     */
    public ListClusterLinksOptions linkNames(Optional<Collection<String>> linkNames) {
        this.linkNames = Objects.requireNonNull(linkNames);
        return this;
    }

    /**
     * Whether to include the topics that are linked for the cluster link.
     */
    public boolean includeTopics() {
        return this.includeTopics;
    }

    /**
     * Set whether to include the topics that are linked for the cluster link.
     */
    public ListClusterLinksOptions includeTopics(boolean includeTopics) {
        this.includeTopics = includeTopics;
        return this;
    }
}
