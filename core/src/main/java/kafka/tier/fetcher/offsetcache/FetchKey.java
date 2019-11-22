/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.fetcher.offsetcache;

import java.util.Objects;
import java.util.UUID;

public class FetchKey {
    private final UUID objectId;
    private final long offset;

    public FetchKey(UUID objectId, long offset) {
        this.objectId = objectId;
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        FetchKey that = (FetchKey) o;
        return offset == that.offset &&
                Objects.equals(objectId, that.objectId);
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
