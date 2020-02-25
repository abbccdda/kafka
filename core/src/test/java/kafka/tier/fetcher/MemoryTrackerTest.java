/*
 Copyright 2018 Confluent Inc.
 */
package kafka.tier.fetcher;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MemoryTrackerTest {

    @Test
    public void testLeaseReclaim() {
        Time time = new MockTime();
        CancellationContext ctx = CancellationContext.newContext();
        MemoryTracker pool = new MemoryTracker(time, 1024);
        MemoryTracker.MemoryLease lease = pool.newLease(ctx, 1024);
        Assert.assertEquals(pool.leased(), lease.leased());
        lease.release();
        Assert.assertEquals(pool.leased(), 0);
    }

    @Test
    public void testTryLease() {
        Time time = new MockTime();
        CancellationContext ctx = CancellationContext.newContext();
        MemoryTracker pool = new MemoryTracker(time, 1024);
        MemoryTracker.MemoryLease lease = pool.newLease(ctx, 1024);
        Assert.assertFalse(pool.tryLease(1024).isPresent());
        lease.release();
        Assert.assertTrue(pool.tryLease(1024).isPresent());
    }

    @Test
    public void testTryLeaseBurst() {
        Time time = new MockTime();
        CancellationContext ctx = CancellationContext.newContext();
        MemoryTracker pool = new MemoryTracker(time, 1024);
        // allocate more than the pool contains, this should allow us to "burst" above
        // the pool limit.
        pool.newLease(ctx, 1024 * 5);
        Assert.assertFalse(pool.tryLease(1).isPresent());
    }

    @Test
    public void testLeaseExtend() {
        Time time = new MockTime();
        CancellationContext ctx = CancellationContext.newContext();
        MemoryTracker pool = new MemoryTracker(time, 1024);
        MemoryTracker.MemoryLease lease = pool.newLease(ctx, 512);
        Assert.assertTrue(lease.tryExtendLease(512));
        Assert.assertFalse(lease.tryExtendLease(512));
        lease.release();
        Assert.assertEquals(pool.leased(), 0);
    }

    @Test
    public void testCancelledNewLeaseClaimsNothing() throws InterruptedException {
        Time time = new MockTime();
        CancellationContext ctx = CancellationContext.newContext();
        MemoryTracker pool = new MemoryTracker(time, 1024);
        pool.newLease(CancellationContext.newContext(), 1024);
        Thread blocked = new Thread(() -> {
            pool.newLease(ctx, 1024);
        });
        ctx.cancel();
        pool.wakeup();
        blocked.join();
        Assert.assertEquals("expected no additional memory to be taken from the pool",
                pool.leased(), 1024);
    }

    @Test
    public void testReclaimedLeaseUnblocksWaiter() throws InterruptedException {
        Time time = new MockTime();
        CancellationContext ctx = CancellationContext.newContext();
        MemoryTracker pool = new MemoryTracker(time, 1024);
        MemoryTracker.MemoryLease oldLease = pool.newLease(CancellationContext.newContext(), 1024);
        Thread blocked = new Thread(() -> {
            pool.newLease(ctx, 1024);
        });
        oldLease.release();
        blocked.join(1000);
    }

    private boolean futureDone(Future<?> future, long timeout, TimeUnit unit) {
        try {
            future.get(timeout, unit);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Test // Verify that with a pool size of 0, memory is tracked but allocations are not blocked.
    public void testPoolSizeZeroIsUnrestricted() {
        Time time = new MockTime();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CancellationContext ctx = CancellationContext.newContext();
        MemoryTracker pool = new MemoryTracker(time, 0);
        Assert.assertTrue(pool.isDisabled());

        // Expect both leases to succeed despite the pool size being 0
        Optional<MemoryTracker.MemoryLease> lease1 = pool.tryLease(1024);
        Assert.assertTrue(lease1.isPresent());
        Optional<MemoryTracker.MemoryLease> lease2 = pool.tryLease(1024);
        Assert.assertTrue(lease2.isPresent());

        Future<?> fut = executor.submit(() -> {
            MemoryTracker.MemoryLease memoryLease2 = pool.newLease(ctx, 1024);
            memoryLease2.release();
        });

        Assert.assertTrue("expected MemoryTracker::newLease not to block", futureDone(fut, 5, TimeUnit.SECONDS));

        // Expect memory usage to be reflected by the pool still
        Assert.assertEquals(1024 + 1024, pool.leased());

        // Releasing memory should still be tracked
        lease1.ifPresent(MemoryTracker.MemoryLease::release);
        Assert.assertEquals(1024, pool.leased());
        lease2.ifPresent(MemoryTracker.MemoryLease::release);
        Assert.assertEquals(0, pool.leased());
    }

    @Test
    public void testChangingPoolSizeDynamicallyWakesBlockedRequests() {
        Time time = new MockTime();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CancellationContext ctx = CancellationContext.newContext();
        MemoryTracker pool = new MemoryTracker(time, 1024);
        // drain the memory pool
        pool.newLease(ctx, 1024);

        Future<?> fut = executor.submit(() -> {
            MemoryTracker.MemoryLease memoryLease2 = pool.newLease(ctx, 1024);
            memoryLease2.release();
        });
        Assert.assertFalse("memory acquisition should be blocked", futureDone(fut, 100, TimeUnit.MILLISECONDS));

        // Setting the pool size to 0 should allow
        pool.setPoolSize(0);
        Assert.assertTrue("expected setting the pool size to 0 would unblock memory acquisition", futureDone(fut, 5, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testChangingPoolSizeDynamically() {
        Time time = new MockTime();
        MemoryTracker pool = new MemoryTracker(time, 1024);
        Optional<MemoryTracker.MemoryLease> lease1 = pool.tryLease(1024);
        Assert.assertTrue(lease1.isPresent());
        Optional<MemoryTracker.MemoryLease> lease2 = pool.tryLease(1024);
        Assert.assertFalse(lease2.isPresent());

        pool.setPoolSize(2048);

        Optional<MemoryTracker.MemoryLease> lease3 = pool.tryLease(1024);
        Assert.assertTrue(lease3.isPresent());
        Optional<MemoryTracker.MemoryLease> lease4 = pool.tryLease(1024);
        Assert.assertFalse(lease4.isPresent());
    }

    @Test
    public void testOomTimeSensor() throws InterruptedException, ExecutionException, TimeoutException {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        MemoryTracker pool = new MemoryTracker(time, metrics, 1024);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        KafkaMetric depletedTime = metrics.metric(pool.memoryTrackerDepletedTimeMetricName);
        KafkaMetric depletedPercent = metrics.metric(pool.memoryTrackerDepletedPercentMetricName);

        CancellationContext ctx = CancellationContext.newContext();

        // Create a memory lease which drains the entire pool
        MemoryTracker.MemoryLease lease1 = pool.newLease(ctx, 1024);

        // Create another memory lease which blocks on memory being returned.
        Future<?> fut = executor.submit(() -> {
            MemoryTracker.MemoryLease lease2 = pool.newLease(ctx, 1024);
            lease2.release();
        });

        // Sleep for a bit to ensure that the thread claiming the memory lease has time to become
        // blocked.
        Thread.sleep(500);

        // Advance the mock clock to simulate time moving forwards.
        time.sleep(10000);

        // unblock lease2 by releasing lease1, this should cause the memoryPoolDepletedTime metric
        // to be recorded.
        lease1.release();
        fut.get();

        Assert.assertEquals("expected 10 seconds of blocked time", (double) depletedTime.metricValue(), 10000.0, 0);
        Assert.assertTrue("expected a nonzero amount of blocked time", (double) depletedPercent.metricValue() > 0);
    }
}