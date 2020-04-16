/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.Confluent;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Options for {@link ConfluentAdmin#deleteClusterLinks(Collection, DeleteClusterLinksOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@Confluent
@InterfaceStability.Evolving
public class DeleteClusterLinksOptions extends AbstractOptions<DeleteClusterLinksOptions> {

    private boolean validateOnly = false;
    private boolean force = false;

    /**
     * Whether to validate that the cluster links can be deleted, but not actually delete them.
     */
    public boolean validateOnly() {
        return this.validateOnly;
    }

    /**
     * Sets whether to validate that the cluster links can be deleted, but not actually delete them.
     */
    public DeleteClusterLinksOptions validateOnly(boolean validateOnly) {
        this.validateOnly = validateOnly;
        return this;
    }

    /**
     * Whether to force deletion of the cluster link even if it's in use.
     */
    public boolean force() {
        return this.force;
    }

    /**
     * Sets whether to force deletion of the cluster link even if it's in use.
     */
    public DeleteClusterLinksOptions force(boolean force) {
        this.force = force;
        return this;
    }
}
