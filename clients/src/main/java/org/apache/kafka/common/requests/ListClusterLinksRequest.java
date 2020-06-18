/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.ListClusterLinksRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class ListClusterLinksRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ListClusterLinksRequest> {

        private final ListClusterLinksRequestData data;

        public Builder(Optional<Collection<String>> linkNames, boolean includeTopics, int timeoutMs) {
            super(ApiKeys.LIST_CLUSTER_LINKS);

            this.data = new ListClusterLinksRequestData()
                    .setLinkNames(linkNames.map(names -> new ArrayList<>(names)).orElse(null))
                    .setIncludeTopics(includeTopics)
                    .setTimeoutMs(timeoutMs);
        }

        @Override
        public ListClusterLinksRequest build(short version) {
            return new ListClusterLinksRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ListClusterLinksRequestData data;

    public ListClusterLinksRequest(ListClusterLinksRequestData data, short version) {
        super(ApiKeys.LIST_CLUSTER_LINKS, version);
        this.data = data;
    }

    public ListClusterLinksRequest(Struct struct, short version) {
        super(ApiKeys.LIST_CLUSTER_LINKS, version);
        this.data = new ListClusterLinksRequestData(struct, version);
    }

    public Optional<List<String>> linkNames() {
        return Optional.ofNullable(data.linkNames());
    }

    public boolean includeTopics() {
        return data.includeTopics();
    }

    public int timeoutMs() {
        return data.timeoutMs();
    }

    @Override
    public ListClusterLinksResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new ListClusterLinksResponse(throttleTimeMs, e);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }
}
