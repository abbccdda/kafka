/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.DeleteClusterLinksRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.ArrayList;
import java.util.Collection;

public class DeleteClusterLinksRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DeleteClusterLinksRequest> {

        private final DeleteClusterLinksRequestData data;

        public Builder(Collection<String> linkNames, boolean validateOnly, boolean force, int timeoutMs) {
            super(ApiKeys.DELETE_CLUSTER_LINKS);

            this.data = new DeleteClusterLinksRequestData()
                .setLinkNames(new ArrayList<>(linkNames))
                .setValidateOnly(validateOnly)
                .setForce(force)
                .setTimeoutMs(timeoutMs);
        }

        @Override
        public DeleteClusterLinksRequest build(short version) {
            return new DeleteClusterLinksRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DeleteClusterLinksRequestData data;

    public DeleteClusterLinksRequest(DeleteClusterLinksRequestData data, short version) {
        super(ApiKeys.DELETE_CLUSTER_LINKS, version);
        this.data = data;
    }

    public DeleteClusterLinksRequest(Struct struct, short version) {
        super(ApiKeys.DELETE_CLUSTER_LINKS, version);
        this.data = new DeleteClusterLinksRequestData(struct, version);
    }

    public Collection<String> linkNames() {
        return data.linkNames();
    }

    public boolean validateOnly() {
        return data.validateOnly();
    }

    public boolean force() {
        return data.force();
    }

    public int timeoutMs() {
        return data.timeoutMs();
    }

    @Override
    public DeleteClusterLinksResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new DeleteClusterLinksResponse(data.linkNames(), throttleTimeMs, e);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }
}
