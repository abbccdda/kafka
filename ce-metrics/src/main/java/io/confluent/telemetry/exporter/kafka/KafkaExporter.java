package io.confluent.telemetry.exporter.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.collector.MetricsCollector;
import io.confluent.telemetry.collector.MetricsCollectorProvider;
import io.confluent.telemetry.exporter.Exporter;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;

public class KafkaExporter implements Exporter, MetricsCollectorProvider {

    private static final Logger log = LoggerFactory.getLogger(KafkaExporter.class);

    // minimum interval at which to log kafka producer errors to avoid unnecessary log spam
    private static final int ERROR_LOG_INTERVAL_MS = 5000;

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


    public KafkaExporter(Builder builder) {
        this.adminClientProperties = Objects.requireNonNull(builder.adminClientProperties);
        this.topicName = Objects.requireNonNull(builder.topicName);
        this.topicConfig = Objects.requireNonNull(builder.topicConfig);
        this.createTopic = builder.createTopic;
        this.topicReplicas = builder.topicReplicas;
        this.topicPartitions = builder.topicPartitions;
        this.producer = new KafkaProducer<>(Objects.requireNonNull(builder.producerProperties));
    }

    private boolean ensureTopic() {
        try (final AdminClient adminClient = AdminClient.create(this.adminClientProperties)) {
            try {
                adminClient.describeTopics(Collections.singleton(this.topicName)).all().get();
                log.debug("Metrics reporter topic {} already exists", this.topicName);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                    // something bad happened
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
    public void emit(Metric metric) {
        try {
            if (!maybeCreateTopic()) {
                return;
            }

            synchronized (this.producer) {
                // producer may already be closed if we are shutting down
                if (!Thread.currentThread().isInterrupted()) {
                    log.trace("Generated metric message : {}", metric);
                    this.producer.send(
                        new ProducerRecord<>(
                            this.topicName,
                            null,
                            null,
                            metric
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
                this.producer.close(Duration.ofMillis(0));
            }
        }
    }

    @Override
    public MetricsCollector collector(ConfluentTelemetryConfig config, Context context, String domain) {
        return new MetricsCollector() {
            long lastDroppedEventCount = 0;
            @Override
            public void collect(Exporter exporter) {
                long droppedTotal = droppedEventCount.get();
                long droppedDelta = droppedTotal - lastDroppedEventCount;
                lastDroppedEventCount = droppedTotal;

                exporter.emit(context.metricWithSinglePointTimeseries(
                    "io.confluent.telemetry/exporter/kafka/dropped/delta",
                    Type.CUMULATIVE_INT64,
                    Collections.emptyMap(),
                    Point.newBuilder().setTimestamp(MetricsUtils.now()).setInt64Value(droppedDelta).build()
                ));
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
            .setCreateTopic(config.isCreateTopic())
            .setTopicConfig(config.getTopicConfig())
            .setTopicName(config.getTopicName())
            .setTopicReplicas(config.getTopicReplicas())
            .setTopicPartitions(config.getTopicPartitions())
            .setProducerProperties(config.getProducerProperties())
            .setAdminClientProperties(config.getProducerProperties());
    }

    public static final class Builder {
        private Properties adminClientProperties;
        private String topicName;
        private boolean createTopic;
        private int topicReplicas;
        private int topicPartitions;
        private Map<String, String> topicConfig;
        private Properties producerProperties;

        private Builder() {
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
