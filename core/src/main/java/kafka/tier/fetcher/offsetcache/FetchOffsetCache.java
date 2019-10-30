package kafka.tier.fetcher.offsetcache;

import org.apache.kafka.common.utils.Time;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * The FetchOffsetCache caches known byte offset and record batch sizes in tiered segments.
 * Cache entries are looked up by a combination of objectId and the record offset.
 * The FetchOffsetCache uses FIFO size eviction scheme combined with time based eviction.
 */
public class FetchOffsetCache {
    private LinkedHashMap<SegmentFetchOffset, CachedMetadata> cache;
    private Time time;
    private long timeoutMs;
    private long hits = 0;
    private long accesses = 0;

    public FetchOffsetCache(Time time, int size, int timeoutMs) {
        this.time = time;
        this.timeoutMs = timeoutMs;
        cache = new LinkedHashMap<SegmentFetchOffset, CachedMetadata>(16, .75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<SegmentFetchOffset, CachedMetadata> eldest) {
                return this.size() > size;
            }
        };
    }

    public synchronized void put(UUID objectId, Long offset,
                                 Integer byteOffset, Integer nextBatchSize) {
        cache.put(new SegmentFetchOffset(objectId, offset),
                new CachedMetadata(byteOffset, nextBatchSize, time.hiResClockMs()));
    }

    public synchronized CachedMetadata get(UUID objectId, Long offset) {
        SegmentFetchOffset key = new SegmentFetchOffset(objectId, offset);
        CachedMetadata metadata = cache.get(key);
        if (metadata != null) {
            // ensure entry is viewed as more recent than other entries
            cache.put(key, new CachedMetadata(metadata.byteOffset, metadata.recordBatchSize, time.hiResClockMs()));
            hits++;
        }
        accesses++;

        return metadata;
    }

    public synchronized double hitRatio() {
        if (accesses == 0)
            return 1.0;

        return (double) hits / accesses;
    }

    public synchronized void expireEntries() {
        long currTime = time.hiResClockMs();
        cache.entrySet().removeIf(entry -> entry.getValue().lastUsed + timeoutMs <= currTime);
    }

    public synchronized long size() {
        return cache.size();
    }

    private static class SegmentFetchOffset {
        private final UUID objectId;
        private final Long offset;

        private SegmentFetchOffset(UUID objectId, Long offset) {
            this.objectId = objectId;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SegmentFetchOffset other = (SegmentFetchOffset) obj;
            return other.objectId.equals(objectId) && other.offset.equals(offset);
        }

        @Override
        public int hashCode() {
            return Objects.hash(objectId, offset);
        }

        @Override
        public String toString() {
            return "objectId: " + objectId + " offset: " + offset;
        }
    }
}
