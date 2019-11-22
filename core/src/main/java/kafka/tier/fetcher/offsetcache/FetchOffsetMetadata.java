/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.fetcher.offsetcache;

import java.util.OptionalInt;

public class FetchOffsetMetadata {
    public final int bytePosition;
    public final OptionalInt recordBatchSize;

    public FetchOffsetMetadata(int bytePosition, OptionalInt recordBatchSize) {
        this.bytePosition = bytePosition;
        this.recordBatchSize = recordBatchSize;
    }

    @Override
    public String toString() {
        return "FetchOffsetMetadata(bytePosition=" + bytePosition
                + ", batchSize=" + recordBatchSize
                + ")";
    }
}
