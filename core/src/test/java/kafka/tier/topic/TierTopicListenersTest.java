package kafka.tier.topic;

import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierSegmentDeleteInitiate;
import kafka.tier.domain.TierSegmentUploadComplete;
import kafka.tier.state.OffsetAndEpoch;
import kafka.tier.state.TierPartitionState;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TierTopicListenersTest {
    @Test
    public void addAndRemoveTrackedTest() {
        MockTime time = new MockTime();
        TierTopicListeners listeners = new TierTopicListeners(time);
        TopicIdPartition topicIdPartition = new TopicIdPartition("mytopic", UUID.randomUUID(), 0);
        UUID objectId = UUID.randomUUID();
        CompletableFuture<TierPartitionState.AppendResult> result = new CompletableFuture<>();
        TierSegmentDeleteInitiate metadata = new TierSegmentDeleteInitiate(topicIdPartition, 0,
                objectId, new OffsetAndEpoch(30, Optional.of(1)));
        listeners.addTracked(metadata, result);
        assertEquals(1, listeners.numListeners());
        time.sleep(1);
        assertEquals(Duration.ofMillis(1).toNanos(), listeners.maxListenerTimeNanos().get().longValue());

        Optional<CompletableFuture<TierPartitionState.AppendResult>> removedFutureOpt =
                listeners.getAndRemoveTracked(metadata);
        assertEquals(0, listeners.numListeners());
        assertFalse(listeners.maxListenerTimeNanos().isPresent());
        assertTrue(removedFutureOpt.isPresent());
        removedFutureOpt.get().complete(TierPartitionState.AppendResult.ACCEPTED);
        try {
            assertEquals(TierPartitionState.AppendResult.ACCEPTED, result.get());
        } catch (InterruptedException | ExecutionException e) {
            fail();
        }
    }

    @Test
    public void replaceListenerTest() {
        MockTime time = new MockTime();
        TierTopicListeners listeners = new TierTopicListeners(time);
        TopicIdPartition topicIdPartition = new TopicIdPartition("mytopic", UUID.randomUUID(), 0);
        UUID objectId = UUID.randomUUID();
        CompletableFuture<TierPartitionState.AppendResult> result = new CompletableFuture<>();
        TierSegmentDeleteInitiate metadata = new TierSegmentDeleteInitiate(topicIdPartition, 0,
                objectId, new OffsetAndEpoch(30, Optional.of(1)));
        listeners.addTracked(metadata, result);
        assertEquals(1, listeners.numListeners());

        TierSegmentDeleteInitiate replace = new TierSegmentDeleteInitiate(topicIdPartition, 0,
                objectId, new OffsetAndEpoch(30, Optional.of(1)));
        CompletableFuture<TierPartitionState.AppendResult> result2 = new CompletableFuture<>();
        listeners.addTracked(replace, result2);
        assertEquals(1, listeners.numListeners());
        assertTrue(result.isCompletedExceptionally());
        assertFalse(result2.isDone());
    }

    @Test
    public void shutdownTest() {
        MockTime time = new MockTime();
        TierTopicListeners listeners = new TierTopicListeners(time);
        TopicIdPartition topicIdPartition = new TopicIdPartition("mytopic", UUID.randomUUID(), 0);
        UUID objectId = UUID.randomUUID();
        CompletableFuture<TierPartitionState.AppendResult> result = new CompletableFuture<>();
        TierSegmentDeleteInitiate metadata = new TierSegmentDeleteInitiate(topicIdPartition, 0,
                objectId, new OffsetAndEpoch(30, Optional.of(1)));
        listeners.addTracked(metadata, result);
        assertEquals(1, listeners.numListeners());
        assertEquals(0, listeners.maxListenerTimeNanos().get().longValue());
        listeners.shutdown();
        assertFalse(listeners.maxListenerTimeNanos().isPresent());
        assertTrue(result.isCancelled());
        assertEquals(0, listeners.numListeners());
    }

    @Test
    public void addRemoveListenersForTopicIdPartition() {
        MockTime time = new MockTime();
        TierTopicListeners listeners = new TierTopicListeners(time);
        TopicIdPartition topicIdPartition = new TopicIdPartition("mytopic", UUID.randomUUID(), 0);
        CompletableFuture<TierPartitionState.AppendResult> result1 = new CompletableFuture<>();
        TierSegmentDeleteInitiate deleteInitiate = new TierSegmentDeleteInitiate(topicIdPartition, 0,
                UUID.randomUUID(), new OffsetAndEpoch(30, Optional.of(1)));
        listeners.addTracked(deleteInitiate, result1);

        time.sleep(1);
        CompletableFuture<TierPartitionState.AppendResult> result2 = new CompletableFuture<>();
        TierSegmentUploadComplete uploadComplete = new TierSegmentUploadComplete(topicIdPartition, 0, UUID.randomUUID(),
                new OffsetAndEpoch(30, Optional.of(1)));
        listeners.addTracked(uploadComplete, result2);

        assertEquals(2, listeners.numListeners());
        assertEquals(Duration.ofMillis(1).toNanos(), listeners.maxListenerTimeNanos().get().longValue());
        Collection<CompletableFuture<TierPartitionState.AppendResult>> removedFutures =
                listeners.getAndRemoveAll(topicIdPartition);
        removedFutures.forEach(f -> f.complete(TierPartitionState.AppendResult.FENCED));
        assertFalse(listeners.maxListenerTimeNanos().isPresent());
        try {
            assertEquals(TierPartitionState.AppendResult.FENCED, result1.get());
            assertEquals(TierPartitionState.AppendResult.FENCED, result2.get());
        } catch (InterruptedException | ExecutionException e) {
            fail();
        }

        assertEquals(0, listeners.numListeners());
    }
}
