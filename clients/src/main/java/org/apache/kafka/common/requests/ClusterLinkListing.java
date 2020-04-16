/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import java.util.Objects;
import java.util.UUID;

/**
 * A listing of a cluster that's linked.
 */
public class ClusterLinkListing {

    private final String linkName;
    private final UUID linkId;
    private final String clusterId;

    /**
     * Creates an cluster link listing.
     *
     * @param linkName The link name for the cluster link
     * @param linkId The UUID associated with the cluster link
     * @param clusterId The linked cluster's ID, or null if none.
     */
    public ClusterLinkListing(String linkName, UUID linkId, String clusterId) {
        this.linkName = Objects.requireNonNull(linkName, "linkName");
        this.linkId = Objects.requireNonNull(linkId, "linkId");
        this.clusterId = clusterId;
    }

    /**
     * The link name for the cluster link.
     */
    public String linkName() {
        return linkName;
    }

    /**
     * The UUID associated with the cluster link.
     */
    public UUID linkId() {
        return linkId;
    }

    /**
     * The linked cluster's ID, or null if none.
     */
    public String clusterId() {
        return clusterId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterLinkListing that = (ClusterLinkListing) o;
        return Objects.equals(linkName, that.linkName) &&
            Objects.equals(linkId, that.linkId) &&
            Objects.equals(clusterId, that.clusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(linkName, linkId, clusterId);
    }

    @Override
    public String toString() {
        return "ClusterLinkListing(linkName=" + linkName + ", linkId=" + linkId + ", clusterId=" + clusterId + ")";
    }
}
