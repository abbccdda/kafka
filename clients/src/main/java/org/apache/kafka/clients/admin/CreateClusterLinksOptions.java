/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.Confluent;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Options for {@link ConfluentAdmin#createClusterLinks(Collection, CreateClusterLinksOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@Confluent
@InterfaceStability.Evolving
public class CreateClusterLinksOptions extends AbstractOptions<CreateClusterLinksOptions> {

    private boolean validateOnly = false;
    private boolean validateLink = false;

    /**
     * Whether to validate that the cluster links can be created, but not actually create them.
     */
    public boolean validateOnly() {
        return this.validateOnly;
    }

    /**
     * Sets whether to validate that the cluster links can be created, but not actually create them.
     */
    public CreateClusterLinksOptions validateOnly(boolean validateOnly) {
        this.validateOnly = validateOnly;
        return this;
    }

    /**
     * Whether to validate the links to the clusters before creation.
     */
    public boolean validateLink() {
        return this.validateLink;
    }

    /**
     * Sets whether to validate the links to the clusters before creation.
     */
    public CreateClusterLinksOptions validateLink(boolean validateLink) {
        this.validateLink = validateLink;
        return this;
    }
}
