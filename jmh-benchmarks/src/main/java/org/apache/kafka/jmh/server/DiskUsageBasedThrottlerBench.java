/*
 Copyright 2020 Confluent Inc.
 */

package org.apache.kafka.jmh.server;

import kafka.server.ActiveTenantsManager;
import kafka.server.BrokerBackpressureConfig;
import kafka.server.ClientQuotaManager;
import kafka.server.ClientQuotaManagerConfig;
import kafka.server.Defaults;
import kafka.server.DiskUsageBasedThrottler$;
import kafka.server.DiskUsageBasedThrottlingConfig;
import kafka.server.QuotaType;
import kafka.utils.MockTime;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import scala.collection.JavaConverters;
import scala.compat.java8.OptionConverters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class DiskUsageBasedThrottlerBench {
    private Path logDir;
    private Map<Boolean, ClientQuotaManager> quotaManagerMap;
    private final Time mockTime = new MockTime();
    private final long pollFrequency = 42;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        quotaManagerMap = new HashMap<>(2);
        logDir = Files.createTempDirectory(UUID.randomUUID().toString());
        if (!Files.isDirectory(logDir))
            throw new IOException("error creating test directory");

        final ArrayList<String> fileStores = new ArrayList<String>() {{
            add(logDir.toString());
        }};

        final boolean produceBackpressureEnabled = false;

        // the quotaManagerConfig object with disk throttling turned on
        final ClientQuotaManagerConfig activeQuotaManagerConfig = new ClientQuotaManagerConfig(
                Defaults.QuotaBytesPerSecond(),
                Defaults.DefaultNumQuotaSamples(),
                Defaults.DefaultQuotaWindowSizeSeconds(),
                new BrokerBackpressureConfig(produceBackpressureEnabled,
                        pollFrequency,
                        JavaConverters.asScalaIteratorConverter(new ArrayList<String>().iterator()).asScala().toSeq(),
                        Double.MAX_VALUE,
                        (double) ConfluentConfigs.BACKPRESSURE_REQUEST_MIN_BROKER_LIMIT_DEFAULT,
                        ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_DEFAULT),
                DiskUsageBasedThrottlingConfig.apply(Long.MAX_VALUE,
                        ConfluentConfigs.BACKPRESSURE_PRODUCE_THROUGHPUT_DEFAULT,
                        JavaConverters.asScalaIteratorConverter(fileStores.iterator()).asScala().toSeq(),
                        true,
                        pollFrequency,
                        ConfluentConfigs.BACKPRESSURE_DISK_RECOVERY_FACTOR_DEFAULT));

        // the same quotaManagerConfig with the disk throttling feature turned off
        final ClientQuotaManagerConfig inactiveQuotaManagerConfig = new ClientQuotaManagerConfig(
                Defaults.QuotaBytesPerSecond(),
                Defaults.DefaultNumQuotaSamples(),
                Defaults.DefaultQuotaWindowSizeSeconds(),
                new BrokerBackpressureConfig(produceBackpressureEnabled,
                        pollFrequency,
                        JavaConverters.asScalaIteratorConverter(new ArrayList<String>().iterator()).asScala().toSeq(),
                        Double.MAX_VALUE,
                        (double) ConfluentConfigs.BACKPRESSURE_REQUEST_MIN_BROKER_LIMIT_DEFAULT,
                        ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_DEFAULT),
                DiskUsageBasedThrottlingConfig.apply(Long.MAX_VALUE,
                        ConfluentConfigs.BACKPRESSURE_PRODUCE_THROUGHPUT_DEFAULT,
                        JavaConverters.asScalaIteratorConverter(fileStores.iterator()).asScala().toSeq(),
                        false,
                        pollFrequency,
                        ConfluentConfigs.BACKPRESSURE_DISK_RECOVERY_FACTOR_DEFAULT));

        final ClientQuotaManager activeQuotaManager = new ClientQuotaManager(activeQuotaManagerConfig,
                new Metrics(),
                QuotaType.Produce$.MODULE$,
                mockTime,
                "someThread",
                OptionConverters.<ClientQuotaCallback>toScala(Optional.empty()),
                OptionConverters.<ActiveTenantsManager>toScala(Optional.empty()));
        quotaManagerMap.put(true, activeQuotaManager);

        final ClientQuotaManager inactiveQuotaManager = new ClientQuotaManager(inactiveQuotaManagerConfig,
                new Metrics(),
                QuotaType.Produce$.MODULE$,
                mockTime,
                "someThread",
                OptionConverters.<ClientQuotaCallback>toScala(Optional.empty()),
                OptionConverters.<ActiveTenantsManager>toScala(Optional.empty()));
        quotaManagerMap.put(false, inactiveQuotaManager);

        quotaManagerMap.values().forEach(DiskUsageBasedThrottler$.MODULE$::registerListener);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        quotaManagerMap.values().forEach(quotaManager -> {
            quotaManager.shutdown();
            DiskUsageBasedThrottler$.MODULE$.deRegisterListener(quotaManager);
        });
        Files.deleteIfExists(logDir);
    }

    @Benchmark
    public void testWithDiskThrottlingEnabled() {
        mockTime.sleep(pollFrequency + 1);
        quotaManagerMap.get(true).checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds());
    }

    @Benchmark
    public void testWithDiskThrottlingDisabled() {
        mockTime.sleep(pollFrequency + 1);
        quotaManagerMap.get(false).checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds());
    }

}
