package kafka.tier.fetcher.offsetcache;

public class CachedMetadata {
    public final int byteOffset;
    public final Integer recordBatchSize;
    public final long lastUsed;

    CachedMetadata(int byteOffset, Integer recordBatchSize, long time) {
        this.byteOffset = byteOffset;
        this.recordBatchSize = recordBatchSize;
        this.lastUsed = time;
    }

    @Override
    public String toString() {
        return "CachedMetadata(byteOffset=" + byteOffset
                + ", batchSize=" + recordBatchSize
                + ", lastUsed=" + lastUsed
                + ")";
    }
}
