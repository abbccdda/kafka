/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.server.checkpoints.LeaderEpochCheckpointBuffer;
import kafka.server.epoch.EpochEntry;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreResponse;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.List;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class TierStateFetcher {
    private final static Logger log = LoggerFactory.getLogger(TierStateFetcher.class);
    private final TierObjectStore tierObjectStore;
    private final ExecutorService executorService;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public TierStateFetcher(Integer numThreads,
                            TierObjectStore tierObjectStore) {
        this.tierObjectStore = tierObjectStore;
        this.executorService = Executors.newFixedThreadPool(numThreads);
    }


    public void close() {
        if (stopped.compareAndSet(false, true)) {
            executorService.shutdownNow();
        }
    }

    /**
     * Send a request to the tier state fetcher executor, returning a future that will be
     * completed when the request has read the tier state from the object store.
     *
     * @param metadata the tier object metadata for this tier state.
     * @return Future to be completed with a list of epoch entries.
     */
    public CompletableFuture<List<EpochEntry>> fetchLeaderEpochState(TierObjectStore.ObjectMetadata metadata) {
        CompletableFuture<scala.collection.immutable.List<EpochEntry>> entries =
                new CompletableFuture<>();
        executorService.execute(() -> {
            try (TierObjectStoreResponse response = tierObjectStore.getObject(metadata,
                    TierObjectStore.FileType.EPOCH_STATE)) {
                try (InputStreamReader inputStreamReader = new InputStreamReader(response.getInputStream())) {
                    try (BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
                        LeaderEpochCheckpointBuffer checkPointBuffer = new LeaderEpochCheckpointBuffer(metadata.toString(), bufferedReader);
                        entries.complete(checkPointBuffer.read().toList());
                    }
                }
            } catch (Throwable e) {
                entries.completeExceptionally(e);
            }
        });
        return entries;
    }

    public CompletableFuture<ByteBuffer> fetchProducerStateSnapshot(TierObjectStore.ObjectMetadata metadata) {
        return CompletableFuture.supplyAsync(() -> {
            try (TierObjectStoreResponse response = tierObjectStore.getObject(metadata,
                    TierObjectStore.FileType.PRODUCER_STATE)) {
                final long objectSize = response.getStreamSize();
                if (objectSize > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Tiered producer state snapshot too large");
                } else {
                    final ByteBuffer buf = ByteBuffer.allocate((int) objectSize);
                    Utils.readFully(response.getInputStream(), buf);
                    buf.flip();
                    return buf;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executorService);
    }
}
