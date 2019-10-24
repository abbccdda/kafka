/*
 Copyright 2018 Confluent Inc.
 */

package org.apache.kafka.jmh.tier;

import kafka.api.ApiVersion$;
import kafka.log.Defaults;
import kafka.log.Log;
import kafka.log.LogConfig;
import kafka.log.LogManager;
import kafka.log.LogSegment;
import kafka.log.MergedLog;
import kafka.log.ProducerStateManager;
import kafka.log.TierLogComponents;
import kafka.log.TierLogSegment;
import kafka.server.BrokerTopicStats;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.state.TierPartitionState;
import kafka.tier.state.TierPartitionStateFactory;
import kafka.tier.store.MockInMemoryTierObjectStore;
import kafka.tier.store.TierObjectStoreConfig;
import kafka.utils.KafkaScheduler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
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
import scala.Function0;
import scala.Option;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction0;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MergedLogTierBenchmark {
    private TopicPartition topicPartition = new TopicPartition(UUID.randomUUID().toString(), 1);
    private File logDir = new File(System.getProperty("java.io.tmpdir"), topicPartition.toString());
    private BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
    private KafkaScheduler scheduler = new KafkaScheduler(0, "fake-prefix", false);
    private TierPartitionState state;
    private MergedLog mergedLog;
    private static final int NUM_TOTAL_SEGMENTS = 100;
    private static final int NUM_TIERED_SEGMENT = 50;
    private long timestamp = 0;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        scheduler.startup();
        LogConfig logConfig = createLogConfig(1000);
        Log log = createLog(logDir, logConfig);
        // setup merged log and tier partition states
        TierPartitionStateFactory partitionStateFactory = new TierPartitionStateFactory(true);
        TierLogComponents tierLogComponents = TierLogComponents.apply(Option.empty(),
                Option.apply(new MockInMemoryTierObjectStore(new TierObjectStoreConfig("cluster", 1))),
                new TierPartitionStateFactory(true));
        state = partitionStateFactory.initState(logDir, topicPartition, logConfig);

        mergedLog = new MergedLog(log, 0, state, tierLogComponents);
        TopicIdPartition topicIdPartition = new TopicIdPartition(topicPartition.topic(),
                UUID.randomUUID(), topicPartition.partition());
        state.setTopicId(topicIdPartition.topicId());

        while (log.logSegments().size() < NUM_TOTAL_SEGMENTS) {
            final MemoryRecords createRecords = buildRecords(0, timestamp,  1, 0);
            log.appendAsLeader(createRecords, 0, true, ApiVersion$.MODULE$.latestVersion());
            timestamp++;
        }
        // update hwm to allow segments to be deleted
        log.updateHighWatermark(log.logEndOffset());

        // simulate archiving to TierPartitionState
        state.onCatchUpComplete();
        state.append(new TierTopicInitLeader(topicIdPartition, 0, java.util.UUID.randomUUID(), 0));

        Iterator<LogSegment> iterator = log.logSegments().take(NUM_TIERED_SEGMENT).iterator();
        while (iterator.hasNext()) {
            LogSegment segment = iterator.next();
            TierUtils.uploadWithMetadata(state,
                    topicIdPartition,
                    0,
                    UUID.randomUUID(),
                    segment.baseOffset(),
                    segment.readNextOffset() - 1,
                    segment.maxTimestampSoFar(),
                    segment.size(),
                    false,
                    true,
                    false);
        }
        state.flush();

        // delete some segments so fetches will go through tier path
        mergedLog.deleteOldSegments();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        mergedLog.close();
        mergedLog.delete();
        state.close();
        state.delete();
        logDir.delete();
        scheduler.shutdown();
    }

    private LogConfig createLogConfig(int segmentBytes) {
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentMsProp(), Defaults.SegmentMs());
        logProps.put(LogConfig.SegmentBytesProp(), segmentBytes);
        logProps.put(LogConfig.RetentionMsProp(), "-1");
        logProps.put(LogConfig.RetentionBytesProp(), "-1");
        logProps.put(LogConfig.SegmentJitterMsProp(), Defaults.SegmentJitterMs());
        logProps.put(LogConfig.CleanupPolicyProp(), Defaults.CleanupPolicy());
        logProps.put(LogConfig.MaxMessageBytesProp(), Defaults.MaxMessageSize());
        logProps.put(LogConfig.IndexIntervalBytesProp(), Defaults.IndexInterval());
        logProps.put(LogConfig.SegmentIndexBytesProp(), Defaults.MaxIndexSize());
        logProps.put(LogConfig.MessageFormatVersionProp(), Defaults.MessageFormatVersion());
        logProps.put(LogConfig.FileDeleteDelayMsProp(), Defaults.FileDeleteDelayMs());
        logProps.put(LogConfig.TierEnableProp(), "true");
        logProps.put(LogConfig.TierLocalHotsetBytesProp(), "0");
        logProps.put(LogConfig.TierLocalHotsetMsProp(), "0");

        return LogConfig.apply(logProps, new scala.collection.immutable.HashSet<>());
    }

    private Log createLog(File dir, LogConfig config) {
        TopicPartition topicPartition = Log.parseTopicPartitionName(logDir);
        ProducerStateManager producerStateManager = new ProducerStateManager(topicPartition, logDir, LogManager.ProducerIdExpirationCheckIntervalMs());
        final Function0<Object> mergedLogStartOffsetCbk = new AbstractFunction0<Object>() {
            @Override
            public Long apply() {
                return 0L;
            }
        };
        return new Log(dir, config, 0L, scheduler, brokerTopicStats, Time.SYSTEM, 60 * 60 * 1000,
                LogManager.ProducerIdExpirationCheckIntervalMs(), topicPartition, producerStateManager, null,
                0L, mergedLogStartOffsetCbk);
    }

    private MemoryRecords buildRecords(long baseOffset,
                                       long timestamp,
                                       int count,
                                       long firstMessageId) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, baseOffset);
        for (int i = 0; i < count; i++)
            builder.append(timestamp, "key".getBytes(), ("value-" + (firstMessageId + i)).getBytes());
        return builder.build();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void readTier() {
        mergedLog.read(25L, 20, kafka.server.FetchHighWatermark$.MODULE$, true);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public long numSegments() {
        return mergedLog.numberOfSegments();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public long size() {
        return mergedLog.size();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public Option<FileRecords.TimestampAndOffset> fetchOffsetByTimestamp() {
        // timestamp 20 will be 20 segments into tiered section of log
        return mergedLog.fetchOffsetByTimestamp(20);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void tierableLogSegments() {
        mergedLog.tierableLogSegments();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public int tieredLogSegmentsFullIteration() {
        return mergedLog.tieredLogSegments().toStream().size();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public int tieredLogSegmentsPartialIteration() {
        int partialSize = 0;
        Iterator<TierLogSegment> iterator = mergedLog.tieredLogSegments().iterator();
        iterator.hasNext();
        partialSize += iterator.next().size();
        iterator.hasNext();
        partialSize += iterator.next().size();
        iterator.hasNext();
        partialSize += iterator.next().size();
        return partialSize;
    }
}
