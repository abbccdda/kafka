/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.AbortedTxn;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;

import java.util.Collections;
import java.util.List;

public class TierFetchResult {
    public final Records records;
    public final Throwable exception;
    public final List<AbortedTxn> abortedTxns;

    public TierFetchResult(Records records, List<AbortedTxn> abortedTxns, Throwable exception) {
        this.records = records;
        this.exception = exception;
        this.abortedTxns = abortedTxns;
    }
    public static TierFetchResult emptyFetchResult() {
        return new TierFetchResult(MemoryRecords.EMPTY, Collections.emptyList(), null);
    }

    public boolean isEmpty() {
        return this.records == MemoryRecords.EMPTY;
    }
}
