/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import java.util.Map;
import java.util.Objects;

/**
 * A new cluster link to be created via {@link Admin#createClusterLinks(Collection, CreateClusterLinksOptions)}.
 */
public class NewClusterLink {

    private final String linkName;
    private final String clusterId;
    private final Map<String, String> configs;

    /**
     * Creates a new cluster link with the specified configs.
     *
     * @param linkName the name assigned to the cluster link
     * @param clusterId the ID of the cluster link, or null if not validated
     * @param configs the initial configuration values
     */
    public NewClusterLink(String linkName, String clusterId, Map<String, String> configs) {
        this.linkName = Objects.requireNonNull(linkName, "Linked cluster name must not be null");
        this.clusterId = clusterId;
        this.configs = Objects.requireNonNull(configs, "Linked cluster configs must not be null");
    }

    /**
     * The name of the cluster link to be created.
     */
    public String linkName() {
        return this.linkName;
    }

    /**
     * The ID of the cluster link, or null if not provided.
     */
    public String clusterId() {
        return this.clusterId;
    }

    /**
     * The configuration for the new cluster link.
     */
    public Map<String, String> configs() {
        return this.configs;
    }

    @Override
    public String toString() {
        return "NewClusterLink(linkName=" + linkName + ", clusterId=" + clusterId + ", configs=" + configs + ")";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final NewClusterLink that = (NewClusterLink) o;
        return Objects.equals(linkName, that.linkName) &&
                Objects.equals(clusterId, that.clusterId) &&
                Objects.equals(configs, that.configs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(linkName, clusterId, configs);
    }
}
