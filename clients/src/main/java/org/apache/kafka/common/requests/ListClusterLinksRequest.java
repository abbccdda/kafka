/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.ListClusterLinksRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

public class ListClusterLinksRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ListClusterLinksRequest> {

        private final ListClusterLinksRequestData data;

        public Builder() {
            super(ApiKeys.LIST_CLUSTER_LINKS);

            this.data = new ListClusterLinksRequestData();
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

    @Override
    public ListClusterLinksResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new ListClusterLinksResponse(throttleTimeMs, e);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }
}
