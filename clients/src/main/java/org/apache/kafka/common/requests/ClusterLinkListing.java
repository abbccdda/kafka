/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * A listing of a cluster that's linked.
 */
public class ClusterLinkListing {

    private final String linkName;
    private final UUID linkId;
    private final String clusterId;
    private final Optional<Collection<String>> topics;

    /**
     * Creates an cluster link listing.
     *
     * @param linkName The link name for the cluster link
     * @param linkId The UUID associated with the cluster link
     * @param clusterId The linked cluster's ID, or null if none.
     * @param topics The topics that are linked with the cluster link, otherwise if
     *               none, then the topics weren't requested.
     */
    public ClusterLinkListing(String linkName, UUID linkId, String clusterId,
                              Optional<Collection<String>> topics) {
        this.linkName = Objects.requireNonNull(linkName, "linkName");
        this.linkId = Objects.requireNonNull(linkId, "linkId");
        this.clusterId = clusterId;
        this.topics = Objects.requireNonNull(topics, "topics");
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

    /**
     * The topics that are linked with the cluster link, otherwise if empty, then the
     * topics weren't requested.
     */
    public Optional<Collection<String>> topics() {
        return topics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterLinkListing that = (ClusterLinkListing) o;
        return Objects.equals(linkName, that.linkName) &&
            Objects.equals(linkId, that.linkId) &&
            Objects.equals(clusterId, that.clusterId) &&
            Objects.equals(topics, that.topics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(linkName, linkId, clusterId, topics);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("ClusterLinkListing(linkName=").append(linkName)
                .append(", linkId=").append(linkId)
                .append(", clusterId=").append(clusterId);
        if (topics.isPresent()) {
            str.append(", topics=").append(topics.get());
        }
        return str.toString();
    }
}
