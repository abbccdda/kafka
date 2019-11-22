/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.fetcher.offsetcache;

public class CachedFetchOffsetMetadata {
    private final FetchOffsetMetadata underlying;
    private long lastUsed;

    public CachedFetchOffsetMetadata(FetchOffsetMetadata underlying,
                                     long lastUsed) {
        this.underlying = underlying;
        this.lastUsed = lastUsed;
    }

    public void updateLastUsed(long lastUsed) {
        this.lastUsed = lastUsed;
    }

    public FetchOffsetMetadata underlying() {
        return underlying;
    }

    public long lastUsed() {
        return lastUsed;
    }

    @Override
    public String toString() {
        return "CachedFetchOffsetMetadata(" +
                "fetchOffsetMetadata=" + underlying +
                ", lastUsed=" + lastUsed +
                ')';
    }
}
