package io.confluent.telemetry.exporter.kafka;

import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.collector.MetricsCollector;
import io.confluent.telemetry.collector.MetricsCollectorProvider;
import io.confluent.telemetry.exporter.AbstractExporter;
import io.confluent.telemetry.exporter.Exporter;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

public class KafkaExporter extends AbstractExporter implements MetricsCollectorProvider {

    private static final Logger log = LoggerFactory.getLogger(KafkaExporter.class);

    // minimum interval at which to log kafka producer errors to avoid unnecessary log spam
    private static final int ERROR_LOG_INTERVAL_MS = 5000;

    public static final String VERSION_HEADER_KEY = "v";
    public static final byte[] V0_HEADER_BYTES = ByteBuffer.allocate(Integer.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putInt(0)
        .array();

    // version header to support future transition from opencensus to opentelemetry protobuf
    private static final Header V0_HEADER = new Header() {
        @Override
        public String key() {
            return VERSION_HEADER_KEY;
        }

        @Override
        public byte[] value() {
            return V0_HEADER_BYTES;
        }
    };
    private static final Iterable<Header> V0_HEADERS = Collections.singleton(V0_HEADER);

    private boolean isTopicCreated = false;
    private final Properties adminClientProperties;
    private final String topicName;
    private final boolean createTopic;
    private final int topicReplicas;
    private final int topicPartitions;
    private final Map<String, String> topicConfig;

    private final KafkaProducer<byte[], Metric> producer;
    private final AtomicLong droppedEventCount = new AtomicLong();
    private final AtomicReference<Exception> droppedEventException = new AtomicReference<>();
    private long lastLoggedTimestamp = 0;
    private long lastLoggedCount = 0;
    private volatile boolean isClosed = false;

    public KafkaExporter(Builder builder) {
        reconfigureWhitelist(builder.whitelistPredicate);
        this.adminClientProperties = Objects.requireNonNull(builder.adminClientProperties);
        this.topicName = Objects.requireNonNull(builder.topicName);
        this.topicConfig = Objects.requireNonNull(builder.topicConfig);
        this.createTopic = builder.createTopic;
        this.topicReplicas = builder.topicReplicas;
        this.topicPartitions = builder.topicPartitions;
        this.producer = new KafkaProducer<>(Objects.requireNonNull(builder.producerProperties));
    }

    public void reconfigure(KafkaExporterConfig exporterConfig) {
        reconfigureWhitelist(exporterConfig.buildMetricWhitelistFilter());
    }

    private boolean ensureTopic() {
        try (final AdminClient adminClient = AdminClient.create(this.adminClientProperties)) {
            try {
                adminClient.describeTopics(Collections.singleton(this.topicName)).all().get();
                log.debug("Metrics reporter topic {} already exists", this.topicName);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                    // something bad happened
                    log.warn(e.getMessage());
                    throw e;
                }

                adminClient
                        .createTopics(
                                Collections.singleton(new NewTopic(
                                        this.topicName,
                                        this.topicPartitions,
                                        (short) this.topicReplicas
                                ).configs(this.topicConfig))
                        )
                        .all()
                        .get();
                log.info("Created metrics reporter topic {}", this.topicName);
            }
            return true;
        } catch (ExecutionException e) {
            log.error("Error checking or creating metrics topic", e.getCause());
            return false;
        } catch (InterruptedException e) {
            log.warn("Confluent metrics reporter topic initialization interrupted");
            return false;
        }
    }

    @Override
    public void doEmit(MetricKey metricKey, Metric metric) {
        try {
            if (!maybeCreateTopic()) {
                return;
            }

            synchronized (this.producer) {
                // producer may already be closed if we are shutting down
                if (!Thread.currentThread().isInterrupted() && !isClosed) {
                    log.trace("Generated metric message : {}", metric);
                    this.producer.send(
                        new ProducerRecord<byte[], Metric>(
                            this.topicName,
                            null,
                            null,
                            metric,
                            V0_HEADERS
                        ),
                        (metadata, exception) -> {
                            if (exception != null) {
                                // avoid excessive logging in the callback,
                                // since this is executed in the producer IO thread
                                droppedEventCount.incrementAndGet();
                                droppedEventException.compareAndSet(null, exception);
                            }
                        }
                    );
                }

                long droppedCount = droppedEventCount.get();
                long droppedDelta = droppedCount - lastLoggedCount;
                long now;
                if (droppedDelta > 0 && lastLoggedTimestamp + ERROR_LOG_INTERVAL_MS < (now = Clock.systemUTC().millis())) {
                    log.warn("Failed to produce {} metrics messages", droppedDelta,
                             droppedEventException.getAndSet(null));
                    lastLoggedTimestamp = now;
                    lastLoggedCount = droppedCount;
                }
            }
        } catch (InterruptException e) {
            // broker is shutting shutdown, interrupt flag is taken care of by
            // InterruptException constructor
        }
    }

    private boolean maybeCreateTopic() {
        if (this.createTopic) {
            if (!this.isTopicCreated) {
                this.isTopicCreated = ensureTopic();
            }
            // if topic can't be created, do not publish metrics
            return this.isTopicCreated;
        }
        return true;
    }

    @Override
    public void close() throws Exception {
        if (this.producer != null) {
            synchronized (this.producer) {
                this.isClosed = true;
                this.producer.close(Duration.ofMillis(0));
            }
        }
    }

    @Override
    public MetricsCollector collector(Predicate<MetricKey> whitelistPredicate, Context context) {
        return new MetricsCollector() {
            private volatile Predicate<MetricKey> metricsWhitelistFilter = whitelistPredicate;
            long lastDroppedEventCount = 0;
            @Override
            public void collect(Exporter exporter) {
                long droppedTotal = droppedEventCount.get();
                long droppedDelta = droppedTotal - lastDroppedEventCount;
                lastDroppedEventCount = droppedTotal;

                String metricName = "io.confluent.telemetry/exporter/kafka/dropped/delta";
                Map<String, String> metricLabels = Collections.emptyMap();
                MetricKey metricKey = new MetricKey(metricName, metricLabels);
                if (metricsWhitelistFilter.test(metricKey)) {
                    exporter.emit(
                        new MetricKey(metricName, metricLabels), context.metricWithSinglePointTimeseries(
                            metricName,
                            Type.CUMULATIVE_INT64,
                            metricLabels,
                            Point.newBuilder().setTimestamp(MetricsUtils.now()).setInt64Value(droppedDelta).build()
                        )
                    );
                }
            }

            @Override
            public void reconfigureWhitelist(Predicate<MetricKey> whitelistPredicate) {
                this.metricsWhitelistFilter = whitelistPredicate;
            }
        };
    }

    public static Builder newBuilder() {
        return new Builder();
    }

  /**
   * Create a new Builder using values from the {@link ConfluentTelemetryConfig}.
   */
    public static Builder newBuilder(KafkaExporterConfig config) {
        return new Builder()
            .setWhitelistPredicate(config.buildMetricWhitelistFilter())
            .setCreateTopic(config.isCreateTopic())
            .setTopicConfig(config.getTopicConfig())
            .setTopicName(config.getTopicName())
            .setTopicReplicas(config.getTopicReplicas())
            .setTopicPartitions(config.getTopicPartitions())
            .setProducerProperties(config.getProducerProperties())
            .setAdminClientProperties(config.getProducerProperties());
    }

    public static final class Builder {
        private Predicate<MetricKey> whitelistPredicate;
        private Properties adminClientProperties;
        private String topicName;
        private boolean createTopic;
        private int topicReplicas;
        private int topicPartitions;
        private Map<String, String> topicConfig;
        private Properties producerProperties;

        private Builder() {
        }

        public Builder setWhitelistPredicate(Predicate<MetricKey> whitelistPredicate) {
            this.whitelistPredicate = whitelistPredicate;
            return this;
        }

        public Builder setAdminClientProperties(Properties adminClientProperties) {
            this.adminClientProperties = adminClientProperties;
            return this;
        }

        public Builder setTopicName(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public Builder setCreateTopic(boolean createTopic) {
            this.createTopic = createTopic;
            return this;
        }

        public Builder setTopicReplicas(int topicReplicas) {
            this.topicReplicas = topicReplicas;
            return this;
        }

        public Builder setTopicPartitions(int topicPartitions) {
            this.topicPartitions = topicPartitions;
            return this;
        }

        public Builder setTopicConfig(Map<String, String> topicConfig) {
            this.topicConfig = topicConfig;
            return this;
        }

        public Builder setProducerProperties(Properties producerProperties) {
            this.producerProperties = producerProperties;
            return this;
        }

        public KafkaExporter build() {
            return new KafkaExporter(this);
        }
    }
}
