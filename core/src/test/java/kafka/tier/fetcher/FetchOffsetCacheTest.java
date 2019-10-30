package kafka.tier.fetcher;

import kafka.tier.fetcher.offsetcache.CachedMetadata;
import kafka.tier.fetcher.offsetcache.FetchOffsetCache;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

public class FetchOffsetCacheTest {
    @Test
    public void offsetCacheExpiry() {
        MockTime time = new MockTime(0, 0, 0);
        FetchOffsetCache offsetCache = new FetchOffsetCache(time, 100, 100);
        UUID objectId1 = UUID.randomUUID();
        offsetCache.put(objectId1, 100L, 100, 200);

        CachedMetadata cached1 = offsetCache.get(objectId1, 100L);
        assertEquals((Integer) 200, cached1.recordBatchSize);
        assertEquals(1.0, offsetCache.hitRatio(), 0.000001);
        time.sleep(99);
        offsetCache.expireEntries();

        CachedMetadata cached2 = offsetCache.get(objectId1, 100L);
        assertEquals("entry should still be present within expiry time",
                (Integer) 200, cached2.recordBatchSize);
        assertEquals(1.0, offsetCache.hitRatio(), 0.000001);
        // expiry time was reset on this fetch
        time.sleep(50);
        assertEquals("expiration timestamp should have been refreshed due to cache hit",
                1, offsetCache.size());
        // expiry time was reset on this fetch
        time.sleep(50);
        offsetCache.expireEntries();
        assertNull(offsetCache.get(objectId1, 100L));
        assertEquals("entry should have expired", 0, offsetCache.size());
        assertEquals(0.66666666666666, offsetCache.hitRatio(), 0.000001);
    }

    @Test
    public void offsetCacheMaxSize() {
        MockTime time = new MockTime(0, 0, 0);
        FetchOffsetCache offsetCache = new FetchOffsetCache(time, 2, 100);
        UUID objectId1 = UUID.randomUUID();
        offsetCache.put(objectId1, 100L, 100, 200);
        offsetCache.put(objectId1, 200L, 100, 200);
        offsetCache.put(objectId1, 300L, 100, 200);
        assertEquals(2, offsetCache.size());
        assertNull("least recent entry should have expired", offsetCache.get(objectId1, 100L));
        assertNotNull(offsetCache.get(objectId1, 200L));
        assertNotNull(offsetCache.get(objectId1, 300L));
        assertEquals(0.66666666666666, offsetCache.hitRatio(), 0.000001);
        assertEquals(2, offsetCache.size());
    }
}
