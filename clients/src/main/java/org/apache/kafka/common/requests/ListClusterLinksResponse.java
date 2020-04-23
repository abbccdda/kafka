/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.ListClusterLinksResponseData;
import org.apache.kafka.common.message.ListClusterLinksResponseData.EntryData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ListClusterLinksResponse extends AbstractResponse {

    private final ListClusterLinksResponseData data;

    public ListClusterLinksResponse(Collection<ClusterLinkListing> clusterLinks, int throttleTimeMs) {
        List<EntryData> entryDatas = new ArrayList<>(clusterLinks.size());
        for (ClusterLinkListing clusterLink : clusterLinks) {
            entryDatas.add(new EntryData()
                    .setLinkName(clusterLink.linkName())
                    .setLinkId(clusterLink.linkId())
                    .setClusterId(clusterLink.clusterId()));
        }

        this.data = new ListClusterLinksResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setEntries(entryDatas);
    }

    public ListClusterLinksResponse(int throttleTimeMs, Throwable e) {
        this.data = new ListClusterLinksResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code())
                .setErrorMessage(e.getMessage());
    }

    public ListClusterLinksResponse(Struct struct, short version) {
        this.data = new ListClusterLinksResponseData(struct, version);
    }

    public void complete(KafkaFutureImpl<Collection<ClusterLinkListing>> result) {
        Errors error = Errors.forCode(data.errorCode());
        if (error != Errors.NONE) {
            result.completeExceptionally(error.exception(data.errorMessage()));
            return;
        }

        List<ClusterLinkListing> clusterLinks = new ArrayList<>(data.entries().size());
        for (EntryData entryData : data.entries()) {
            clusterLinks.add(new ClusterLinkListing(entryData.linkName(), entryData.linkId(), entryData.clusterId()));
        }
        result.complete(clusterLinks);
    }

    // Visible for testing.
    public ListClusterLinksResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.singletonMap(Errors.forCode(data.errorCode()), 1);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }
}
