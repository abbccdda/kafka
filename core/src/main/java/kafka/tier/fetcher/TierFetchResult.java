/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.AbortedTxn;
import org.apache.kafka.common.record.MemoryRecords;

import java.util.Collections;
import java.util.List;

public class TierFetchResult {
    public final ReclaimableMemoryRecords records;
    public final Throwable exception;
    public final List<AbortedTxn> abortedTxns;

    public TierFetchResult(ReclaimableMemoryRecords records, List<AbortedTxn> abortedTxns, Throwable exception) {
        this.records = records;
        this.abortedTxns = abortedTxns;
        this.exception = exception;
    }

    public static TierFetchResult emptyFetchResult() {
        return new TierFetchResult(ReclaimableMemoryRecords.EMPTY, Collections.emptyList(), null);
    }

    public boolean isEmpty() {
        return this.records == MemoryRecords.EMPTY;
    }
}
