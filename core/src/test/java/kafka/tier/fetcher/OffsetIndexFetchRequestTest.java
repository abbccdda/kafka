/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.OffsetIndex;
import kafka.log.OffsetPosition;
import kafka.tier.TopicIdPartition;
import kafka.tier.store.TierObjectStore;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.CancellationException;

public class OffsetIndexFetchRequestTest {
    private OffsetIndex offsetIndex = null;
    private int maxEntries = 3;
    private long baseOffset = 45L;
    private TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
    private TierObjectStore.ObjectMetadata tierObjectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, baseOffset);

    @Before
    public void setup() {
        this.offsetIndex = new OffsetIndex(nonExistentTempFile(), baseOffset, maxEntries * 8, true);
    }

    @After
    public void teardown() {
        if (this.offsetIndex != null) {
            this.offsetIndex.file().delete();
        }
    }

    @Test
    public void emptyIndexFileTest() {
        CancellationContext cancellationContext = CancellationContext.newContext();
        TierObjectStore mockTierObjectStore = FetchRequestTestUtils.fileReturningTierObjectStore(offsetIndex.file(), null);

        try {
            OffsetPosition result = OffsetIndexFetchRequest.fetchOffsetPositionForStartingOffset(cancellationContext, mockTierObjectStore, tierObjectMetadata, 55);
            Assert.assertEquals("an empty index file should return a segment position of 0", 0, result.position());
        } catch (Exception e) {
            Assert.fail("unexpected exception");
        } finally {
            cancellationContext.cancel();
            mockTierObjectStore.close();
        }
    }

    @Test
    public void seekIndexFileExceptionTest() {
        CancellationContext cancellationContext = CancellationContext.newContext();

        offsetIndex.append(50, 50);
        offsetIndex.append(55, 55);
        offsetIndex.append(60, 60);
        offsetIndex.flush();

        TierObjectStore mockTierObjectStore = FetchRequestTestUtils.fileReturningTierObjectStore(offsetIndex.file(), null);

        try {
            try {
                OffsetIndexFetchRequest.fetchOffsetPositionForStartingOffset(cancellationContext, FetchRequestTestUtils.ioExceptionThrowingTierObjectStore(), tierObjectMetadata, 30);
            } catch (IOException e) {
                Assert.assertNotNull("IoExceptions are not propagated correctly", e);
            } catch (Exception e) {
                Assert.fail("Unexpected exception " + e);
            }

            CancellationContext canceledCancellationContext = CancellationContext.newContext();
            canceledCancellationContext.cancel();

            try {
                OffsetIndexFetchRequest.fetchOffsetPositionForStartingOffset(canceledCancellationContext, mockTierObjectStore, tierObjectMetadata, 30);
            } catch (CancellationException ignored) {

            } catch (Exception e) {
                Assert.fail("Unexpected exception");
            }

        } finally {
            cancellationContext.cancel();
            mockTierObjectStore.close();
        }
    }

    @Test
    public void seekIndexFileTest() {
        CancellationContext cancellationContext = CancellationContext.newContext();

        offsetIndex.append(50, 50);
        offsetIndex.append(55, 55);
        offsetIndex.append(60, 60);
        offsetIndex.flush();

        TierObjectStore mockTierObjectStore = FetchRequestTestUtils.fileReturningTierObjectStore(offsetIndex.file(), null);

        try {
            try {
                OffsetPosition offsetPosition =
                        OffsetIndexFetchRequest.fetchOffsetPositionForStartingOffset(cancellationContext,
                                mockTierObjectStore, tierObjectMetadata, 55);
                Assert.assertEquals("the desired offset position matches the appended "
                        + "position", 55, offsetPosition.position());
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("unexpected exception " + e);
            }

            try {
                OffsetPosition offsetPosition =
                        OffsetIndexFetchRequest.fetchOffsetPositionForStartingOffset(cancellationContext,
                                mockTierObjectStore, tierObjectMetadata, 100);
                Assert.assertEquals("if the desired offset is out of range, return the highest position in the index file",
                        60, offsetPosition.position());
            } catch (Exception e) {
                Assert.fail("unexpected exception");
            }

            try {
                OffsetPosition offsetPosition =
                        OffsetIndexFetchRequest.fetchOffsetPositionForStartingOffset(cancellationContext,
                                mockTierObjectStore, tierObjectMetadata, 30);

                Assert.assertEquals("if the desired offset is lower than all recorded offsets, return 0 for position",
                        0, offsetPosition.position());
            } catch (Exception e) {
                Assert.fail("unexpected exception");
            }

            Assert.assertFalse("The CancellationContext is not canceled", cancellationContext.isCancelled());


        } finally {
            cancellationContext.cancel();
            mockTierObjectStore.close();
        }
    }

    private File nonExistentTempFile() {
        File file;
        try {
            file = TestUtils.tempFile();
            Files.delete(file.toPath());
        } catch (IOException e) {
            return null;
        }
        return file;
    }
}