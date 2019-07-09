package io.confluent.telemetry.exporter;

import com.google.common.base.Verify;
import io.confluent.telemetry.Context;
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
    private Context context;

    private boolean isTopicCreated = false;
    private Properties adminClientProperties;
    private String topicName;
    private boolean createTopic;
    private int topicReplicas;
    private int topicPartitions;
    private Map<String, String> topicConfig;

    private Properties producerProperties;
    private KafkaProducer<byte[], Metric> producer;

    public KafkaExporter(Context context,
                         Properties adminClientProperties,
                         String topicName,
                         boolean createTopic,
                         int topicReplicas,
                         int topicPartitions,
                         Map<String, String> topicConfig,
                         Properties producerProperties) {

        this.context = context;
        this.adminClientProperties = adminClientProperties;
        this.topicName = topicName;
        this.createTopic = createTopic;
        this.topicReplicas = topicReplicas;
        this.topicPartitions = topicPartitions;
        this.topicConfig = topicConfig;
        this.producerProperties = producerProperties;
        this.producer = new KafkaProducer<>(this.producerProperties);
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

    public static final class Builder {
        private Context context;
        private boolean isTopicCreated = false;
        private Properties adminClientProperties;
        private String topicName;
        private boolean createTopic;
        private int topicReplicas;
        private int topicPartitions;
        private Map<String, String> topicConfig;
        private Properties producerProperties;

        private Builder() {
        }

        public Builder setContext(Context context) {
            this.context = context;
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

            Objects.requireNonNull(this.context);
            Objects.requireNonNull(this.adminClientProperties);
            Objects.requireNonNull(this.topicName);
            Objects.requireNonNull(this.topicConfig);
            Objects.requireNonNull(this.producerProperties);
            Verify.verify(this.topicReplicas > 0, "topic needs atleast 1 replica");
            Verify.verify(this.topicPartitions > 0, "topic needs atleast 1 partition");

            return new KafkaExporter(
                    this.context,
                    this.adminClientProperties,
                    this.topicName,
                    this.createTopic,
                    this.topicReplicas,
                    this.topicPartitions,
                    this.topicConfig,
                    this.producerProperties);
        }
    }
}
