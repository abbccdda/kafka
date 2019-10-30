package org.apache.kafka.jmh.tier;

import kafka.log.LogConfig;
import kafka.log.LogSegment;
import kafka.server.KafkaConfig;
import kafka.tier.TopicIdPartition;
import kafka.tier.fetcher.PendingFetch;
import kafka.tier.fetcher.TierFetchMetadata;
import kafka.tier.fetcher.TierFetchResult;
import kafka.tier.fetcher.TierFetcher;
import kafka.tier.fetcher.TierFetcherConfig;
import kafka.tier.store.MockInMemoryTierObjectStore;
import kafka.tier.store.S3TierObjectStore;
import kafka.tier.store.S3TierObjectStoreConfig;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreConfig;
import kafka.utils.KafkaScheduler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import scala.Option;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 4)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)

public class TierFetcherBenchmark {
    @Param({"500", "50000", "500000", "1000000", "2000000"})
    public static String approxBatchSize;
    @Param({"0", "100000", "200000", "300000", "400000", "500000"})
    public static String autoAbortSize;
    private final static boolean TEST_REAL_S3 = false;
    private final static int TARGET_SEGMENT_SIZE = 100_000_000;
    private final static int INDEX_INTERVAL_BYTES = 4096;
    private final static int SEGMENT_INDEX_BYTES = 10_000_000;
    private final static int TOTAL_FETCH_SIZE = 10_000_000;
    private final static int PARTITION_FETCH_MAX_BYTES = 1_000_000;

    private static MemoryRecords buildWithOffset(long baseOffset) {
        int approxBatchSizeInt = Integer.parseInt(approxBatchSize);
        ByteBuffer buffer = ByteBuffer.allocate(approxBatchSizeInt * 2);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME, baseOffset);
        byte[] array = new byte[approxBatchSizeInt];
        Arrays.fill(array, (byte) 254);
        builder.appendWithOffset(baseOffset, baseOffset, null, array);
        return builder.build();
    }

    @State(Scope.Thread)
    public static class FetchState {
        private final TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
        private TierObjectStore tierObjectStore;
        private TierObjectStore.ObjectMetadata objectMetadata;
        private LogSegment logSegment;
        private long finalSegmentSize;

        @Setup(Level.Trial)
        public void setupState() throws Exception {
            File logSegmentDir = new File(System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID().toString());
            logSegmentDir.mkdir();

            Properties logProps = new Properties();
            logProps.put(LogConfig.IndexIntervalBytesProp(), INDEX_INTERVAL_BYTES);
            logProps.put(LogConfig.SegmentIndexBytesProp(), SEGMENT_INDEX_BYTES);

            Set<String> override = Collections.emptySet();
            LogConfig logConfig = LogConfig.apply(logProps, scala.collection.JavaConverters.asScalaSetConverter(override).asScala().toSet());
            logSegment = LogSegment.open(logSegmentDir, 0, logConfig, Time.SYSTEM, false, 4096, true, "");

            File offsetIndexFile = logSegment.offsetIndex().file();
            File timestampIndexFile = logSegment.offsetIndex().file();
            File segmentFile = logSegment.log().file();

            while (segmentFile.length() < TARGET_SEGMENT_SIZE) {
                long nextOffset = logSegment.readNextOffset();
                MemoryRecords batch = buildWithOffset(nextOffset);
                logSegment.append(batch.batches().iterator().next().lastOffset(), 1L, 1, batch);
            }

            finalSegmentSize = segmentFile.length();

            logSegment.flush();
            logSegment.offsetIndex().flush();
            logSegment.offsetIndex().trimToValidSize();
            logSegment.timeIndex().flush();
            logSegment.timeIndex().trimToValidSize();

            tierObjectStore = getTierObjectStore();
            objectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0, false);
            tierObjectStore.putSegment(objectMetadata, segmentFile, offsetIndexFile, timestampIndexFile,
                    Optional.empty(), Optional.empty(), Optional.empty());
        }

        @TearDown(Level.Trial)
        public void teardown() {
            logSegment.close();
            logSegment.deleteIfExists();
        }
    }

    private static TierObjectStore getTierObjectStore() {
        TierObjectStore tierObjectStore;
        if (TEST_REAL_S3) {
            Properties props = new Properties();
            props.put(KafkaConfig.ZkConnectProp(), "IGNORED");
            props.put(KafkaConfig.TierS3BucketProp(), "");
            props.put(KafkaConfig.TierS3AwsAccessKeyIdProp(), "");
            props.put(KafkaConfig.TierS3AwsSecretAccessKeyProp(), "");
            props.put(KafkaConfig.TierS3RegionProp(), "us-west-2");
            props.put(KafkaConfig.TierS3AutoAbortThresholdBytesProp(), autoAbortSize);
            KafkaConfig kafkaConfig = new KafkaConfig(props);
            S3TierObjectStoreConfig s3TierObjectStoreConfig = new S3TierObjectStoreConfig("mycluster", kafkaConfig);
            tierObjectStore = new S3TierObjectStore(s3TierObjectStoreConfig);
        } else {
            tierObjectStore = new MockInMemoryTierObjectStore(new TierObjectStoreConfig("mycluster", 0));
        }

        return tierObjectStore;
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public int fetch(FetchState fetchState) {
        Metrics metrics = new Metrics();
        TierFetcherConfig fetcherConfig = new TierFetcherConfig();
        LogContext logContext = new LogContext("tierFetcher");
        // start a new tier fetcher so we use a fresh offset cache for the entire run
        TierFetcher tierFetcher = new TierFetcher(fetcherConfig, fetchState.tierObjectStore, Mockito.mock(KafkaScheduler.class), metrics, logContext);

        int sizeRead = 0;
        // pick an initial offset that requires an index fetch
        long nextOffset = 5;
        while (sizeRead < TOTAL_FETCH_SIZE) {
            TierFetchMetadata fetchMetadata = new TierFetchMetadata(fetchState.topicIdPartition.topicPartition(),
                    nextOffset, PARTITION_FETCH_MAX_BYTES, 10000000L, true,
                    fetchState.objectMetadata, Option.empty(), 0,
                    (int) fetchState.finalSegmentSize);
            PendingFetch pending = tierFetcher.buildFetch(Collections.singletonList(fetchMetadata),
                            IsolationLevel.READ_UNCOMMITTED,
                            ignored -> {
                            });
            pending.run();

            Map<TopicPartition, TierFetchResult> fetchResults = pending.finish();
            TierFetchResult fetchResult = fetchResults.get(fetchState.topicIdPartition.topicPartition());
            RecordBatch finalBatch = null;
            for (RecordBatch batch: fetchResult.records.batches()) {
                sizeRead += batch.sizeInBytes();
                finalBatch = batch;
            }
            nextOffset = finalBatch.nextOffset();
        }

        tierFetcher.close();
        return sizeRead;
    }
}
