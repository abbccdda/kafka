/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.CreateClusterLinksRequestData;
import org.apache.kafka.common.message.CreateClusterLinksRequestData.ConfigData;
import org.apache.kafka.common.message.CreateClusterLinksRequestData.EntryData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateClusterLinksRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<CreateClusterLinksRequest> {

        private final CreateClusterLinksRequestData data;

        public Builder(Collection<NewClusterLink> newClusterLinks, boolean validateOnly, boolean validateLink, int timeoutMs) {
            super(ApiKeys.CREATE_CLUSTER_LINKS);

            List<EntryData> entryDatas = new ArrayList<>(newClusterLinks.size());
            for (NewClusterLink newClusterLink : newClusterLinks) {
                List<ConfigData> configDatas = new ArrayList<>(newClusterLink.configs().size());

                for (Map.Entry<String, String> configsEntry : newClusterLink.configs().entrySet()) {
                    configDatas.add(new ConfigData()
                            .setKey(configsEntry.getKey())
                            .setValue(configsEntry.getValue()));
                }

                entryDatas.add(new EntryData()
                        .setLinkName(newClusterLink.linkName())
                        .setClusterId(newClusterLink.clusterId())
                        .setConfigs(configDatas));
            }

            this.data = new CreateClusterLinksRequestData()
                    .setEntries(entryDatas)
                    .setValidateOnly(validateOnly)
                    .setValidateLink(validateLink)
                    .setTimeoutMs(timeoutMs);
        }

        @Override
        public CreateClusterLinksRequest build(short version) {
            return new CreateClusterLinksRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final CreateClusterLinksRequestData data;

    public CreateClusterLinksRequest(CreateClusterLinksRequestData data, short version) {
        super(ApiKeys.CREATE_CLUSTER_LINKS, version);
        this.data = data;
    }

    public CreateClusterLinksRequest(Struct struct, short version) {
        super(ApiKeys.CREATE_CLUSTER_LINKS, version);
        this.data = new CreateClusterLinksRequestData(struct, version);
    }

    public Collection<NewClusterLink> newClusterLinks() {
        List<NewClusterLink> newClusterLinks = new ArrayList<>(data.entries().size());
        for (EntryData entryData : data.entries()) {
            Map<String, String> configs = new HashMap<>(entryData.configs().size());
            for (ConfigData configData : entryData.configs()) {
                configs.put(configData.key(), configData.value());
            }
            newClusterLinks.add(new NewClusterLink(entryData.linkName(), entryData.clusterId(), configs));
        }
        return newClusterLinks;
    }

    public boolean validateOnly() {
        return data.validateOnly();
    }

    public boolean validateLink() {
        return data.validateLink();
    }

    public int timeoutMs() {
        return data.timeoutMs();
    }

    @Override
    public CreateClusterLinksResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        List<String> linkNames = new ArrayList<>(data.entries().size());
        for (EntryData entryData : data.entries()) {
            linkNames.add(entryData.linkName());
        }
        return new CreateClusterLinksResponse(linkNames, throttleTimeMs, e);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }
}
