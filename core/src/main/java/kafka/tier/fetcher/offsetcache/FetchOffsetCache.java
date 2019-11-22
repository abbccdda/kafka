/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.fetcher.offsetcache;

import org.apache.kafka.common.utils.Time;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;

/**
 * The FetchOffsetCache caches known byte offset and record batch sizes in tiered segments.
 * Cache entries are looked up by a combination of objectId and the record offset.
 * The FetchOffsetCache uses FIFO size eviction scheme combined with time based eviction.
 */
public class FetchOffsetCache {
    private final LinkedHashMap<FetchKey, CachedFetchOffsetMetadata> cache;
    private final boolean enabled;
    private final Time time;
    private final long timeoutMs;

    private long hits = 0;
    private long accesses = 0;

    public FetchOffsetCache(Time time, int size, int timeoutMs) {
        this.time = time;
        this.timeoutMs = timeoutMs;
        this.cache = new LinkedHashMap<FetchKey, CachedFetchOffsetMetadata>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<FetchKey, CachedFetchOffsetMetadata> eldest) {
                return this.size() > size;
            }
        };
        this.enabled = size > 0;
    }

    public synchronized void put(UUID objectId,
                                 long offset,
                                 int byteOffset,
                                 OptionalInt batchSize) {
        put(objectId, offset, new FetchOffsetMetadata(byteOffset, batchSize));
    }

    public synchronized void put(UUID objectId,
                                 long offset,
                                 FetchOffsetMetadata fetchOffsetMetadata) {
        if (enabled)
            cache.put(new FetchKey(objectId, offset), new CachedFetchOffsetMetadata(fetchOffsetMetadata, time.hiResClockMs()));
    }

    public synchronized FetchOffsetMetadata get(UUID objectId, long offset) {
        FetchKey key = new FetchKey(objectId, offset);
        CachedFetchOffsetMetadata metadata = cache.get(key);
        if (metadata != null) {
            // ensure entry is viewed as more recent than other entries
            metadata.updateLastUsed(time.hiResClockMs());
            cache.put(key, metadata);
            hits++;
        }
        accesses++;

        return metadata != null ? metadata.underlying() : null;
    }

    public synchronized double hitRatio() {
        if (accesses == 0)
            return 1.0;

        return (double) hits / accesses;
    }

    public synchronized void expireEntries() {
        long currTime = time.hiResClockMs();
        cache.entrySet().removeIf(entry -> entry.getValue().lastUsed() + timeoutMs <= currTime);
    }

    public synchronized long size() {
        return cache.size();
    }
}
