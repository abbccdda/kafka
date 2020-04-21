/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.DeleteClusterLinksResponseData;
import org.apache.kafka.common.message.DeleteClusterLinksResponseData.EntryData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeleteClusterLinksResponse extends AbstractResponse {

    private final DeleteClusterLinksResponseData data;

    public DeleteClusterLinksResponse(Map<String, ApiError> results, int throttleTimeMs) {
        List<EntryData> entryDatas = new ArrayList<>(results.size());
        for (Map.Entry<String, ApiError> resultsEntry : results.entrySet()) {
            ApiError e = resultsEntry.getValue();
            entryDatas.add(new EntryData()
                    .setErrorCode(e.error().code())
                    .setErrorMessage(e.message())
                    .setLinkName(resultsEntry.getKey()));
        }

        this.data = new DeleteClusterLinksResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setEntries(entryDatas);
    }

    public DeleteClusterLinksResponse(Collection<String> linkNames, int throttleTimeMs, Throwable e) {
        short errorCode = Errors.forException(e).code();
        String errorMessage = e.getMessage();

        List<EntryData> entryDatas = new ArrayList<>(linkNames.size());
        for (String linkName : linkNames) {
            entryDatas.add(new EntryData()
                    .setErrorCode(errorCode)
                    .setErrorMessage(errorMessage)
                    .setLinkName(linkName));
        }

        this.data = new DeleteClusterLinksResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setEntries(entryDatas);
    }

    public DeleteClusterLinksResponse(Struct struct, short version) {
        this.data = new DeleteClusterLinksResponseData(struct, version);
    }

    public void complete(Map<String, KafkaFutureImpl<Void>> results) {
        for (EntryData entryData : data.entries()) {
            String linkName = entryData.linkName();
            KafkaFutureImpl<Void> future = results.get(linkName);
            if (future == null) {
                throw new IllegalArgumentException("Result must contain link with name: " + linkName);
            }

            Errors error = Errors.forCode(entryData.errorCode());
            if (error == Errors.NONE) {
                future.complete(null);
            } else {
                future.completeExceptionally(error.exception(entryData.errorMessage()));
            }
        }
    }

    // Visible for testing.
    public DeleteClusterLinksResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        for (EntryData entry : data.entries()) {
            Errors error = Errors.forCode(entry.errorCode());
            counts.put(error, counts.getOrDefault(error, 0) + 1);
        }
        return counts;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }
}
