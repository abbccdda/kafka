package io.confluent.telemetry.exporter.kafka;

import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.Exporter;
import io.opencensus.proto.metrics.v1.Metric;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaExporter implements Exporter {

    private static final Logger log = LoggerFactory.getLogger(KafkaExporter.class);

    private boolean isTopicCreated = false;
    private Properties adminClientProperties;
    private String topicName;
    private boolean createTopic;
    private int topicReplicas;
    private int topicPartitions;
    private Map<String, String> topicConfig;

    private KafkaProducer<byte[], Metric> producer;

    public KafkaExporter(Builder builder) {
        this.adminClientProperties = Objects.requireNonNull(builder.adminClientProperties);
        this.topicName = Objects.requireNonNull(builder.topicName);
        this.topicConfig = Objects.requireNonNull(builder.topicConfig);
        this.createTopic = builder.createTopic;
        this.topicReplicas = builder.topicReplicas;
        this.topicPartitions = builder.topicPartitions;
        this.producer = new KafkaProducer<>(Objects.requireNonNull(builder.producerProperties));
    }

    public boolean ensureTopic() {

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
    public void write(Collection<Metric> metrics) throws RuntimeException {

        try {
            if (this.createTopic) {
                if (!this.isTopicCreated) {
                    this.isTopicCreated = ensureTopic();
                }
                // if topic can't be created, skip the rest
                if (!this.isTopicCreated) {
                    return;
                }
            }

            log.debug("Begin publishing metrics");


            synchronized (this.producer) {
                // producer may already be closed if we are shutting down
                if (!Thread.currentThread().isInterrupted()) {
                    for (Metric metricsMessage : metrics) {
                        log.trace("Generated metric message : {}", metricsMessage);
                        this.producer.send(
                                new ProducerRecord<byte[], Metric>(
                                        this.topicName,
                                        null,
// TODO : What is the timestamp here ?
//                                        metricsMessage.getTimeseries(0).getStartTimestamp().getSeconds(),
                                        null,
                                        metricsMessage
                                ),
                                new Callback() {
                                    @Override
                                    public void onCompletion(
                                            RecordMetadata metadata,
                                            Exception exception
                                    ) {
                                        if (exception != null) {
                                            log.warn("Failed to produce metrics message", exception);
                                        } else {
                                            log.debug(
                                                    "Produced metrics message of size {} with "
                                                            + "offset {} to topic partition {}-{}",
                                                    metadata.serializedValueSize(),
                                                    metadata.offset(),
                                                    metadata.topic(),
                                                    metadata.partition()
                                            );
                                        }
                                    }
                                }
                        );
                    }
                }
            }
        } catch (InterruptException e) {
            // broker is shutting shutdown, interrupt flag is taken care of by
            // InterruptException constructor
        } catch (Throwable t) {
            log.warn("Failed to publish metrics message in the reporter", t);
        }
    }

    @Override
    public void close() throws Exception {
        if (this.producer != null) {
            synchronized (this.producer) {
                this.producer.close(Duration.ofMillis(0));
            }
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

  /**
   * Create a new Builder using values from the {@link ConfluentTelemetryConfig}.
   */
    public static Builder newBuilder(ConfluentTelemetryConfig config) {
        KafkaExporterConfig exporterConfig = config.getKafkaExporterConfig();
        return new Builder()
            .setCreateTopic(exporterConfig.isCreateTopic())
            .setTopicConfig(exporterConfig.getTopicConfig())
            .setTopicName(exporterConfig.getTopicName())
            .setTopicReplicas(exporterConfig.getTopicReplicas())
            .setTopicPartitions(exporterConfig.getTopicPartitions())
            .setProducerProperties(exporterConfig.getProducerProperties())
            .setAdminClientProperties(exporterConfig.getProducerProperties());
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
