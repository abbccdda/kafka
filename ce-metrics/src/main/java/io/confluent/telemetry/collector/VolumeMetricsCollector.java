// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.telemetry.collector;

import com.google.common.base.MoreObjects;
import com.google.common.base.Verify;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.collector.YammerMetricsCollector.Builder;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor;
import io.opencensus.proto.metrics.v1.Point;
import java.time.Instant;
import java.util.function.Predicate;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class VolumeMetricsCollector implements MetricsCollector {

    public static class VolumeMetricsCollectorConfig extends AbstractConfig {
        public static final String PREFIX = ConfluentTelemetryConfig.PREFIX_METRICS_COLLECTOR + "volume.";

        public static final String VOLUME_METRICS_UPDATE_PERIOD_MS = PREFIX + "update.ms";
        public static final long DEFAULT_VOLUME_METRICS_UPDATE_PERIOD = 15000L;
        public static final String VOLUME_METRICS_UPDATE_PERIOD_DOC =
            "The minimum interval at which to fetch new volume metrics.";

        private static final ConfigDef CONFIG = new ConfigDef()
            .define(
                VOLUME_METRICS_UPDATE_PERIOD_MS,
                ConfigDef.Type.LONG,
                DEFAULT_VOLUME_METRICS_UPDATE_PERIOD,
                ConfigDef.Importance.LOW,
                VOLUME_METRICS_UPDATE_PERIOD_DOC
            );

        public VolumeMetricsCollectorConfig(Map<?, ?> originals) {
            super(CONFIG, originals);
        }

        String[] getBrokerLogVolumes() {
            String logDirsString = null;
            Map<String, ?> originals = originals();
            if (originals.containsKey(KafkaConfig.LogDirsProp())) {
                logDirsString = (String) originals.get(KafkaConfig.LogDirsProp());
            }
            if (logDirsString == null) {
                if (originals.containsKey(KafkaConfig.LogDirProp())) {
                    logDirsString = (String) originals.get(KafkaConfig.LogDirProp());
                }
            }

            return logDirsString == null ? null : logDirsString.split("\\s*,\\s*");
        }

        long getUpdatePeriodMs() {
            return getLong(VOLUME_METRICS_UPDATE_PERIOD_MS);
        }

    }

    public static final String VOLUME_LABEL = "volume";
    private static final Logger log = LoggerFactory.getLogger(VolumeMetricsCollector.class);
    private final Predicate<MetricKey> metricFilter;

    /**
     * The last update time in monotonic nanoseconds.
     */
    private long lastUpdateNs;
    private Instant lastUpdateInstant;

    /**
     * The cached metrics.
     */
    private Map<String, VolumeInfo> cachedMetrics = null;

    /**
     * Cached list of labels per volume
     */
    private Map<String, Map<String, String>> labelsCache = new HashMap<>();

    /**
     * The minimum time in milliseconds that we will go in between updates.
     */
    // TODO: Figure out why we cache values (I suspect it is because we dont want to query the filesystem metadata too often, but should confirm)
    private long updatePeriodMs;
    /**
     * The log directories to fetch information about.
     */
    private final String[] logDirs;
    private final Context context;
    private final String diskTotalBytesName;
    private final String diskUsableBytesName;

    private VolumeMetricsCollector(Builder builder) {
        updatePeriodMs = builder.updatePeriodMs;
        logDirs = builder.logDirs;
        context = builder.context;
        metricFilter = builder.metricFilter;

        String domain = builder.domain;
        diskTotalBytesName = MetricsUtils.fullMetricName(domain, "volume", "disk_total_bytes");
        diskUsableBytesName = MetricsUtils.fullMetricName(domain, "volume", "disk_usable_bytes");
    }

    private synchronized Map<String, String> labelsFor(String volumeName) {
        Map<String, String> labels = labelsCache.get(volumeName);
        if (labels == null) {
            labels = new HashMap<>();
            if (context.isDebugEnabled()) {
                labels.put(MetricsCollector.LIBRARY, NO_LIBRARY);
                labels.put(ORIGINAL, "none");
            }
            labels.putAll(this.context.labels());
            labels.put(VOLUME_LABEL, volumeName);
            labelsCache.put(volumeName, labels);
        }
        return Collections.unmodifiableMap(labels);
    }

    @Override
    public List<Metric> collect() {
        List<Metric> out = new ArrayList<>();


        for (VolumeInfo volumeInfo : getMetrics().values()) {

            Map<String, String> labels = labelsFor(volumeInfo.name());

            if (metricFilter.test(new MetricKey(diskTotalBytesName, labels))) {
                out.add(MetricsUtils.metricWithSinglePointTimeseries(
                    diskTotalBytesName,
                    MetricDescriptor.Type.GAUGE_INT64,
                    labels,
                    Point.newBuilder()
                        .setTimestamp(MetricsUtils.now())
                        .setInt64Value(volumeInfo.totalBytes()).build(),
                    MetricsUtils.toTimestamp(lastUpdateInstant)
                    ));
            }

            if (metricFilter.test(new MetricKey(diskUsableBytesName, labels))) {
                out.add(MetricsUtils.metricWithSinglePointTimeseries(
                    diskUsableBytesName,
                    MetricDescriptor.Type.GAUGE_INT64,
                    labels,
                    Point.newBuilder()
                        .setTimestamp(MetricsUtils.now())
                        .setInt64Value(volumeInfo.usableBytes()).build(),
                    MetricsUtils.toTimestamp(lastUpdateInstant)));
            }
        }

        return out;
    }

    @Override
    public String toString() {
        return VolumeMetricsCollector.class.getCanonicalName();
    }

    public Map<String, VolumeInfo> getMetrics() {
        long curTimeNs = System.nanoTime();
        long deltaNs = curTimeNs - lastUpdateNs;
        long deltaMs = TimeUnit.MILLISECONDS.convert(deltaNs, TimeUnit.NANOSECONDS);
        if (deltaMs >= updatePeriodMs) {
            cachedMetrics = null;
        }
        if (cachedMetrics == null) {
            lastUpdateNs = curTimeNs;
            cachedMetrics = refreshCachedMetrics();
        }
        return Collections.unmodifiableMap(cachedMetrics);
    }

    private Map<String, VolumeInfo> refreshCachedMetrics() {
        lastUpdateInstant = Instant.now();
        Map<String, FileStore> fileStoreNameToObject = new HashMap<>();
        Map<String, Set<String>> fileStoreNameToLogDirs = new HashMap<>();
        for (String logDir : logDirs) {
            try {
                FileStore fileStore = pathToFileStore(logDir);
                fileStoreNameToObject.put(fileStore.name(), fileStore);
                if (!fileStoreNameToLogDirs.containsKey(fileStore.name())) {
                    fileStoreNameToLogDirs.put(fileStore.name(), new TreeSet<String>());
                }
                fileStoreNameToLogDirs.get(fileStore.name()).add(logDir);
            } catch (IOException e) {
                log.error("Failed to resolve path to FileStore", e);
            }
        }
        Map<String, VolumeInfo> metrics = new TreeMap<>();
        for (FileStore fileStore : fileStoreNameToObject.values()) {
            try {
                VolumeInfo volumeInfo = new VolumeInfo(fileStore,
                        fileStoreNameToLogDirs.get(fileStore.name()));
                if (log.isDebugEnabled()) {
                    log.debug("Read {}", volumeInfo.toString());
                }
                metrics.put(volumeInfo.name(), volumeInfo);
            } catch (RuntimeException | IOException e) {
                log.error("Failed to retrieve VolumeInfo from FileStore", e);
            }
        }
        return metrics;
    }

    private FileStore pathToFileStore(String path) throws IOException {
        Path pathObj = Paths.get(path);
        return Files.getFileStore(pathObj);
    }

    static class VolumeInfo {

        private final String name;
        private final long usableBytes;
        private final long totalBytes;
        private final Set<String> logDirs;

        private VolumeInfo(
                FileStore fileStore,
                Set<String> logDirs
        ) throws IOException {
            this.name = fileStore.name();
            this.usableBytes = fileStore.getUsableSpace();
            this.totalBytes = fileStore.getTotalSpace();
            this.logDirs = Collections.unmodifiableSet(logDirs);
        }

        public String name() {
            return name;
        }

        public long usableBytes() {
            return usableBytes;
        }

        public long totalBytes() {
            return totalBytes;
        }

        public Collection<String> logDirs() {
            return this.logDirs;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("name", name)
                    .add("usableBytes", usableBytes)
                    .add("totalBytes", totalBytes)
                    .add("logDirs", logDirs)
                    .toString();
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Create a new Builder using values from the {@link ConfluentTelemetryConfig}.
     */
    public static Builder newBuilder(ConfluentTelemetryConfig config) {
        VolumeMetricsCollectorConfig collectorConfig = config.getVolumeMetricsCollectorConfig();
        return new Builder()
            .setMetricFilter(config.getMetricFilter())
            .setLogDirs(collectorConfig.getBrokerLogVolumes())
            .setUpdatePeriodMs(collectorConfig.getUpdatePeriodMs());
    }

    public static class Builder {
        private String domain;
        private long updatePeriodMs;
        private String[] logDirs;
        private Context context;
        private Predicate<MetricKey> metricFilter = s -> true;

        private Builder() {
        }

        public Builder setDomain(String domain) {
            this.domain = domain;
            return this;
        }

        public Builder setUpdatePeriodMs(long updatePeriodMs) {
            this.updatePeriodMs = updatePeriodMs;
            return this;
        }

        public Builder setLogDirs(String[] logDirs) {
            this.logDirs = logDirs;
            return this;
        }

        public Builder setContext(Context context) {
            this.context = context;
            return this;
        }
        public Builder setMetricFilter(Predicate<MetricKey> metricFilter) {
            this.metricFilter = metricFilter;
            return this;
        }

        public VolumeMetricsCollector build() {
            Objects.requireNonNull(this.context);
            Objects.requireNonNull(this.domain);
            Objects.requireNonNull(this.logDirs);

            Verify.verify(this.updatePeriodMs > 0, "update interval cannot be less than 1");

            return new VolumeMetricsCollector(this);
        }

    }
}
