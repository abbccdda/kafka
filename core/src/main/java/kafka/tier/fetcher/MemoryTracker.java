/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Time;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

/**
 * MemoryTracker tracks memory usage for the TierFetcher. It allows for leasing out memory
 * allocations and blocking on memory to be returned.
 */
public class MemoryTracker implements AutoCloseable {
    private final Time time;
    private final Metrics metrics;
    private final MetricName leasedMetricName;
    private final MetricName poolSizeMetricName;
    private final MetricName oldestLeaseMetricName;
    private final Map<Long, Instant> creationTimes = new HashMap<>();
    // visible for testing
    final MetricName memoryTrackerDepletedPercentMetricName;
    // visible for testing
    final MetricName memoryTrackerDepletedTimeMetricName;
    private final Sensor oomTimeSensor;

    /**
     * Measures the amount of time that the MemoryTracker is out of memory. This directly
     * corresponds to the amount of time new memory acquisitions via `MemoryTracker::newLease()`
     * will be blocked.
     */
    private long oomPeriodStart = 0;

    /**
     * Maximum size of the MemoryTracker pool, modifiable at runtime.
     */
    private long poolSize;

    /**
     * The total amount of bytes which have been leased.
     */
    private long leased = 0;

    /**
     * A unique per-lease identifier used to track when a lease was created.
     */
    private long leaseId = Long.MIN_VALUE;
    private boolean closed = false;

    public MemoryTracker(Time time, long poolSize) {
        this(time, null, poolSize);
    }

    public MemoryTracker(Time time, Metrics metrics, long poolSize) {
        if (poolSize < 0) {
            throw new IllegalArgumentException("MemoryTracker pool size should be >= 0");
        }
        this.time = time;
        this.poolSize = poolSize;
        this.metrics = metrics;

        if (metrics != null) {
            String metricGroupName = "TierFetcherMemoryTracker";
            this.leasedMetricName = metrics.metricName("Leased", metricGroupName,
                    "The amount of memory currently leased in bytes");
            this.poolSizeMetricName = metrics.metricName("PoolSize", metricGroupName,
                    "The size of the memory pool in bytes, 0 if disabled");
            this.oldestLeaseMetricName = metrics.metricName("MaxLeaseLagMs", metricGroupName,
                    "The time difference between the oldest outstanding memory lease and the current time");
            this.memoryTrackerDepletedPercentMetricName = metrics.metricName(
                    "MemoryTrackerAvgDepletedPercent", metricGroupName, "The average percentage"
                            + "of time in milliseconds requests were blocked due to memory "
                            + "pressure");
            this.memoryTrackerDepletedTimeMetricName = metrics.metricName(
                    "MemoryTrackerDepletedTimeTotal", metricGroupName, "The total amount of time "
                            + "in milliseconds requests were blocked due to memory pressure");
            this.oomTimeSensor = metrics.sensor("MemoryTrackerUtilization");
            oomTimeSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryTrackerDepletedPercentMetricName, memoryTrackerDepletedTimeMetricName));

            MemoryTracker self = this;
            metrics.addMetric(leasedMetricName, (config, now) -> {
                synchronized (self) {
                    return leased;
                }
            });
            metrics.addMetric(poolSizeMetricName, (config, now) -> {
                synchronized (self) {
                    return poolSize;
                }
            });
            metrics.addMetric(oldestLeaseMetricName, (config, now) -> {
                synchronized (self) {
                    Instant smallest = null;
                    for (Instant candidate : creationTimes.values()) {
                        if (smallest == null || candidate.isBefore(smallest)) {
                            smallest = candidate;
                        }
                    }
                    if (smallest == null) {
                        // if there are no entries in the map, return 0 time delta
                        return 0;
                    }
                    return Math.min(0, now - smallest.toEpochMilli());
                }
            });
        } else {
            this.leasedMetricName = null;
            this.poolSizeMetricName = null;
            this.oldestLeaseMetricName = null;
            this.memoryTrackerDepletedPercentMetricName = null;
            this.memoryTrackerDepletedTimeMetricName = null;
            this.oomTimeSensor = null;
        }
    }

    /**
     * Set a new pool size for the Memory Tracker. All caller threads blocked on memory leases
     * will be woken up.
     */
    synchronized public void setPoolSize(long newPoolSize) {
        poolSize = newPoolSize;
        wakeup();
    }

    /**
     * Returns true if this MemoryTracker is disabled. The MemoryTracker is disabled if the
     * maximum pool size is equal to zero bytes.
     */
    synchronized public boolean isDisabled() {
        return poolSize == 0;
    }

    /**
     * Creates a new MemoryLease if the pool contains a positive amount of bytes.
     * If the available bytes are <= 0, then newLease will block on more memory becoming available.
     * The provided CancellationContext is checked each time the pool is woken up. If the
     * CancellationContext has been canceled, a CancellationException is thrown.
     */
    synchronized public MemoryLease newLease(CancellationContext ctx, long amount) {
        while (!ctx.isCancelled()) {
            if (this.closed)
                throw new IllegalStateException("MemoryTracker closed");
            Optional<MemoryLease> newLease = tryLease(amount);
            if (newLease.isPresent()) {
                stopOomPeriod();
                return newLease.get();
            } else {
                try {
                    startOomPeriod();
                    this.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new CancellationException("Memory lease request cancelled");
    }

    /**
     * Records the start of a period where the MemoryTracker is blocking requests on memory
     * reclamation.
     */
    private void startOomPeriod() {
        if (oomTimeSensor != null && oomPeriodStart == 0)
            oomPeriodStart = time.nanoseconds();
    }

    /**
     * Records the end of a period where the MemoryTracker is blocking requests on memory
     * reclamation. This will also reset the oomPeriodStart to allow future measurements to be
     * taken.
     */
    private void stopOomPeriod() {
        if (oomTimeSensor != null) {
            long start = oomPeriodStart;
            oomPeriodStart = 0;
            if (start != 0) {
                oomTimeSensor.record((time.nanoseconds() - start) / 1000000.0); // convert to ms
            }
        }
    }

    /**
     * Like newLease, this will attempt to create a new MemoryLease for the provided amount of
     * bytes. However unlike newLease, this will not block and instead return Optional.empty.
     *
     * If the MemoryTracker is "disabled", meaning it's poolSize is == 0, then leasing memory
     * will always succeed. Lease times will not be retained if the MemoryTracker is "disabled".
     */
    synchronized public Optional<MemoryLease> tryLease(long amount) {
        if (this.closed)
            throw new IllegalStateException("MemoryTracker closed");
        if ((poolSize - leased) > 0 || isDisabled()) {
            // There are bytes left in the pool, allow the allocation to succeed.
            this.leased += amount;
            this.leaseId += 1;
            Instant now = Instant.ofEpochMilli(time.hiResClockMs());
            if (!isDisabled())
                creationTimes.put(leaseId, now);
            return Optional.of(new MemoryLease(this, leaseId, amount));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Returns the number of bytes currently leased
     */
    synchronized public long leased() {
        return leased;
    }

    synchronized public long poolSize() {
        return poolSize;
    }

    /**
     * Wakeup all threads which are waiting on a memory lease.
     */
    synchronized public void wakeup() {
        this.notifyAll();
    }

    /**
     * Release the provided MemoryLease, returning it back to the pool of available memory.
     */
    synchronized private void release(MemoryLease lease) {
        this.leased -= lease.amount;
        this.creationTimes.remove(lease.leaseID);
        this.notifyAll();
    }

    @Override
    synchronized public void close() {
        if (this.metrics != null) {
            this.metrics.removeMetric(leasedMetricName);
            this.metrics.removeMetric(poolSizeMetricName);
            this.metrics.removeMetric(oldestLeaseMetricName);
            this.metrics.removeMetric(memoryTrackerDepletedPercentMetricName);
            this.metrics.removeMetric(memoryTrackerDepletedTimeMetricName);
        }
        this.closed = true;
        this.wakeup();
    }

    public static final class MemoryLease {
        private final long leaseID;
        private final MemoryTracker parent;
        private boolean released = false;
        private long amount;


        public MemoryLease(MemoryTracker parent, long leaseId, long amount) {
            this.parent = parent;
            this.leaseID = leaseId;
            this.amount = amount;
        }

        /**
         * Release this MemoryLease, returning it back to the pool of available memory in the
         * MemoryTracker
         */
        public void release() {
            if (!released) {
                released = true;
                this.parent.release(this);
            }
        }

        /**
         * Returns the amount of memory currently leased by this MemoryLease.
         */
        public long leased() {
            if (released) throw new IllegalStateException(this + " already reclaimed");
            else return amount;
        }

        /**
         * Attempts to extend the lease by amount additional bytes.
         * Returns true if successful.
         */
        public boolean tryExtendLease(long amount) {
            if (released) {
                throw new IllegalStateException("MemoryLease already reclaimed");
            } else {
                Optional<MemoryLease> newLease = parent.tryLease(amount);
                // Count the new lease towards this lease and throw it away without closing it.
                // This is safe because the new lease is accounted for by this lease.
                newLease.ifPresent(memoryLease -> this.amount += memoryLease.leased());
                return newLease.isPresent();
            }
        }

        @Override
        public String toString() {
            return "MemoryLease{" +
                    "leaseID=" + leaseID +
                    ", released=" + released +
                    ", amount=" + amount +
                    '}';
        }
    }
}

