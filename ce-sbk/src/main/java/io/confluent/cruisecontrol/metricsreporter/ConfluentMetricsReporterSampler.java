/*
 Copyright 2019 Confluent Inc.
 */

package io.confluent.cruisecontrol.metricsreporter;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.BrokerMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricsUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsProcessor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler;
import io.confluent.databalancer.StartupCheckInterruptedException;
import io.confluent.metrics.record.ConfluentMetric;
import io.confluent.metrics.reporter.ConfluentMetricsReporterConfig;
import io.confluent.serializers.ProtoSerde;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager.BROKER_CAPACITY_CONFIG_RESOLVER_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager.DEFAULT_BROKER_CAPACITY_CONFIG_RESOLVER_OBJECT_CONFIG;

/**
 * This class largely mimics the behavior of the CruiseControlMetricsReporterSampler. The difference is that
 * Cruise Control converts Kafka and Yammer metrics to a CruiseControlMetric type before writing to Kafka.
 * This class uses the Cruise Control metrics utils on the consumer side to convert the raw metrics before
 * handing them off to the metrics processor.
 */
public class ConfluentMetricsReporterSampler implements MetricSampler {
    private static final Logger LOG = LoggerFactory.getLogger(ConfluentMetricsReporterSampler.class);
    // Configurations
    public static final String METRIC_REPORTER_SAMPLER_BOOTSTRAP_SERVERS = "metric.reporter.sampler.bootstrap.servers";
    public static final String METRIC_REPORTER_TOPIC_PATTERN = "confluent.metrics.reporter.topic";
    public static final String METRIC_REPORTER_SAMPLER_GROUP_ID = "metric.reporter.sampler.group.id";
    // Default configs
    private static final String DEFAULT_METRIC_REPORTER_SAMPLER_GROUP_ID = "ConfluentMetricsReporterSampler";
    // static metric processor for metrics aggregation.
    private CruiseControlMetricsProcessor metricsProcessor;

    // static random token to avoid group conflict.
    private static final Random RANDOM = ThreadLocalRandom.current();
    // timeout for polling for a consumer assignment
    private static final int ASSIGNMENT_POLL_TIMEOUT = 10;
    // log a warning every 12000 polls (12000 * 10 ms = 2 minutes) if no assignment exists
    private static final int ASSIGNMENT_LOGGING_INTERVAL = 12000;
    // timeout for polling for metrics
    private static final long METRICS_POLL_TIMEOUT = TimeUnit.SECONDS.toMillis(5);

    private Consumer<byte[], byte[]> metricConsumer;
    private ProtoSerde<ConfluentMetric.MetricsMessage> serde =
            new ProtoSerde<>(ConfluentMetric.MetricsMessage.getDefaultInstance());

    /**
     * Get the metric sample of the given topic partition and replica from the Kafka cluster.
     * <p>
     * The samples include PartitionMetricSamples and BrokerMetricSamples.
     * <p>
     * Due to the lack of direct metrics at partition level, Kafka Cruise Control needs to estimate the CPU
     * utilization for each partition by using the following formula:
     * <p>
     * BROKER_CPU_UTIL = a * ALL_TOPIC_BYTES_IN_RATE + b * ALL_TOPIC_BYTES_OUT_RATE + c * ALL_FOLLOWER_BYTES_IN_RATE
     * <p>
     * LEADER_PARTITION_CPU_UTIL = a * LEADER_PARTITION_BYTES_IN + b * LEADER_PARTITION_BYTES_OUT
     * <p>
     * FOLLOWER_PARTITION_CPU_UTIL = c * LEADER_PARTITION_BYTES_IN
     * <p>
     * Kafka Cruise Control needs to know the parameters of a, b and c for cost evaluation of leader and
     * partition movement.
     *
     * @param cluster            the metadata of the cluster.
     * @param assignedPartitions the topic partition
     * @param startTimeMs        the start time of the sampling period.
     * @param endTimeMs          the end time of the sampling period.
     * @param mode               The sampling mode.
     * @param metricDef          the metric definitions.
     * @param timeout            The sampling timeout to stop sampling even if there is more data to get.
     * @return the PartitionMetricSample of the topic partition and replica id
     */
    @Override
    public Samples getSamples(Cluster cluster,
                              Set<TopicPartition> assignedPartitions,
                              long startTimeMs,
                              long endTimeMs,
                              SamplingMode mode,
                              MetricDef metricDef,
                              long timeout) throws MetricSamplingException {
        // Ensure we have an assignment.
        long pollerCount = 0L;
        while (metricConsumer.assignment().isEmpty()) {
            pollerCount++;
            metricConsumer.poll(ASSIGNMENT_POLL_TIMEOUT);
            // Log a warning for an empty assignment on the interval to avoid spamming the log
            if ((pollerCount % ASSIGNMENT_LOGGING_INTERVAL) == 0) {
                LOG.warn("metricConsumer Assignment is empty .. Did you copy the cruise-control-metrics-reporter.jar to Kafka libs ?");
            }
        }
        // Now seek to the startTimeMs.
        Map<TopicPartition, Long> timestampToSeek = new HashMap<>();
        for (TopicPartition tp : metricConsumer.assignment()) {
            timestampToSeek.put(tp, startTimeMs);
        }
        Set<TopicPartition> assignment = new HashSet<>(metricConsumer.assignment());
        Map<TopicPartition, Long> endOffsets = metricConsumer.endOffsets(assignment);
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = metricConsumer.offsetsForTimes(timestampToSeek);
        // If some of the partitions does not have data, we simply seek to the end offset. To avoid losing metrics, we use
        // the end offsets before the timestamp query.
        assignment.removeAll(offsetsForTimes.keySet());
        for (TopicPartition tp : assignment) {
            metricConsumer.seek(tp, endOffsets.get(tp));
        }
        // For the partition that returned an offset, seek to the returned offsets.
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
            if (offsetAndTimestamp != null) {
                metricConsumer.seek(tp, offsetAndTimestamp.offset());
            } else {
                metricConsumer.seek(tp, endOffsets.get(tp));
            }
        }
        LOG.debug("Starting consuming from metrics reporter topic partitions {}", metricConsumer.assignment());
        metricConsumer.resume(metricConsumer.paused());
        int totalMetricsAdded = 0;
        long maxTimeStamp = -1L;
        long deadline = System.currentTimeMillis() + (endTimeMs - startTimeMs) / 2;
        do {
            ConsumerRecords<byte[], byte[]> records = metricConsumer.poll(METRICS_POLL_TIMEOUT);
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (record == null) {
                    // This means we cannot parse the metrics. It might happen when a newer type of metrics has been added and
                    // the current code is still old. We simply ignore that metric in this case.
                    LOG.debug("Cannot parse record.");
                    continue;
                }
                List<CruiseControlMetric> metrics = convertMetricRecord(record.value());
                for (CruiseControlMetric metric : metrics) {
                    if (startTimeMs <= metric.time() && metric.time() < endTimeMs) {
                        metricsProcessor.addMetric(metric);
                        maxTimeStamp = Math.max(maxTimeStamp, metric.time());
                        totalMetricsAdded++;
                    } else if (metric.time() >= endTimeMs) {
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        LOG.debug("Saw metric {} whose timestamp {} is larger than end time {}. Pausing partition {} at offset {}",
                                metric, metric.time(), endTimeMs, tp, record.offset());
                        metricConsumer.pause(Collections.singleton(tp));
                    } else {
                        LOG.debug("Discarding metric {} because the timestamp {} is smaller than the start time {}",
                                metric, metric.time(), startTimeMs);
                    }
                }
            }
        } while (!consumptionDone(endOffsets) && System.currentTimeMillis() < deadline);
        LOG.debug("Finished sampling for topic partitions {} in time range [{},{}]. Collected {} metrics.",
                metricConsumer.assignment(), startTimeMs, endTimeMs, totalMetricsAdded);

        try {
            if (totalMetricsAdded > 0) {
                return metricsProcessor.process(cluster, assignedPartitions, mode);
            } else {
                return new Samples(Collections.emptySet(), Collections.emptySet());
            }
        } finally {
            metricsProcessor.clear();
        }
    }

    /**
     * Check if the consumption is done or not. The consumption is done if the consumer has caught up with the
     * log end or all the partitions are paused.
     * @param endOffsets the log end for each partition.
     * @return true if the consumption is done, false otherwise.
     */
    private boolean consumptionDone(Map<TopicPartition, Long> endOffsets) {
        Set<TopicPartition> partitionsNotPaused = new HashSet<>(metricConsumer.assignment());
        partitionsNotPaused.removeAll(metricConsumer.paused());
        for (TopicPartition tp : partitionsNotPaused) {
            if (metricConsumer.position(tp) < endOffsets.get(tp)) {
                return false;
            }
        }
        return true;
    }

    protected List<CruiseControlMetric> convertMetricRecord(byte[] confluentMetric) {
        List<CruiseControlMetric> metricList = new ArrayList<>();
        ConfluentMetric.MetricsMessage metricsMessage = serde.deserialize(confluentMetric);
        for (ConfluentMetric.KafkaMeasurable km : metricsMessage.getKafkaMeasurableList()) {
            org.apache.kafka.common.MetricName metricName = convertKafkaMetricName(km.getMetricName());

            // Handle CPU usage as a special case since it's not handled by CC MetricsUtils
            if (metricName.name().equals("CpuUsage")) {
                this.addIfNotNull(metricList, new BrokerMetric(
                        RawMetricType.BROKER_CPU_UTIL,
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        km.getValue()));
            } else if (MetricsUtils.isInterested(metricName)) {
                this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        metricName.name(),
                        metricName.tags(),
                        km.getValue()));
            }
        }
        for (ConfluentMetric.YammerGauge gauge : metricsMessage.getYammerGaugeList()) {
            com.yammer.metrics.core.MetricName metricName = convertYammerMetricName(gauge.getMetricName());
            if (MetricsUtils.isInterested(metricName)) {
                Double value = null;
                switch (gauge.getNumericValueCase()) {
                    case LONGVALUE:
                        value = (double) gauge.getLongValue();
                        break;
                    case DOUBLEVALUE:
                        value = gauge.getDoubleValue();
                        break;
                    default:
                        break; // Leave value as null
                }
                if (value != null) {
                    this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                            metricsMessage.getTimestamp(),
                            metricsMessage.getBrokerId(),
                            convertYammerMetricName(gauge.getMetricName()),
                            value));
                }
            }
        }
        for (ConfluentMetric.YammerMeter meter : metricsMessage.getYammerMeterList()) {
            com.yammer.metrics.core.MetricName metricName = convertYammerMetricName(meter.getMetricName());
            if (MetricsUtils.isInterested(metricName)) {
                this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        convertYammerMetricName(meter.getMetricName()),
                        meter.getOneMinuteRate()));
            }
        }
        for (ConfluentMetric.YammerTimer timer : metricsMessage.getYammerTimerList()) {
            com.yammer.metrics.core.MetricName metricName = convertYammerMetricName(timer.getMetricName());
            if (MetricsUtils.isInterested(metricName)) {
                this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        metricName,
                        timer.getOneMinuteRate()));
                this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        metricName,
                        timer.getMax(),
                        MetricsUtils.ATTRIBUTE_MAX));
                this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        metricName,
                        timer.getMean(),
                        MetricsUtils.ATTRIBUTE_MEAN));
            }
        }
        for (ConfluentMetric.YammerHistogram histogram : metricsMessage.getYammerHistogramList()) {
            com.yammer.metrics.core.MetricName metricName = convertYammerMetricName(histogram.getMetricName());
            if (MetricsUtils.isInterested(metricName)) {
                this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        metricName,
                        histogram.getMax(),
                        MetricsUtils.ATTRIBUTE_MAX));
                this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        metricName,
                        histogram.getMean(),
                        MetricsUtils.ATTRIBUTE_MEAN));
            }
        }

        return metricList;
    }

    private void addIfNotNull(List<CruiseControlMetric> metricList, CruiseControlMetric metric) {
        if (metric != null) {
            metricList.add(metric);
        }
    }

    private org.apache.kafka.common.MetricName convertKafkaMetricName(ConfluentMetric.KafkaMetricName metricName) {
        return new org.apache.kafka.common.MetricName(
                metricName.getName(),
                metricName.getGroup(),
                "",
                metricName.getTagsMap());
    }

    private com.yammer.metrics.core.MetricName convertYammerMetricName(ConfluentMetric.YammerMetricName metricName) {
        return new com.yammer.metrics.core.MetricName(
                metricName.getGroup(),
                metricName.getType(),
                metricName.getName(),
                metricName.getScope().equals("") ? null : metricName.getScope(),
                metricName.getMBeanName());
    }

    @Override
    public void configure(Map<String, ?> configs) {

        BrokerCapacityConfigResolver capacityResolver = (BrokerCapacityConfigResolver) configs.get(BROKER_CAPACITY_CONFIG_RESOLVER_OBJECT_CONFIG);
        if (capacityResolver == null) {
            capacityResolver = (BrokerCapacityConfigResolver) configs.get(DEFAULT_BROKER_CAPACITY_CONFIG_RESOLVER_OBJECT_CONFIG);
            if (capacityResolver == null) {
                throw new IllegalArgumentException("Metrics reporter sampler configuration is missing broker capacity config resolver object.");
            }
        }
        boolean allowCpuCapacityEstimation = (Boolean) configs.get(KafkaCruiseControlConfig.SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_CONFIG);
        metricsProcessor = new CruiseControlMetricsProcessor(capacityResolver, allowCpuCapacityEstimation);

        Integer numSamplersString = (Integer) configs.get(KafkaCruiseControlConfig.NUM_METRIC_FETCHERS_CONFIG);
        if (numSamplersString != null && numSamplersString != 1) {
            throw new ConfigException("ConfluentMetricsReporterSampler is not thread safe. Please change " +
                    KafkaCruiseControlConfig.NUM_METRIC_FETCHERS_CONFIG + " to 1");
        }

        String metricReporterTopic = getMetricReporterTopic(configs);

        Properties consumerProps = getMetricConsumerProperties(configs);
        createMetricConsumer(consumerProps, metricReporterTopic);
        validateSamplingTopic(metricReporterTopic);
    }

    private static String getMetricReporterTopic(Map<String, ?> configs) {
        String metricReporterTopic = (String) configs.get(METRIC_REPORTER_TOPIC_PATTERN);
        if (metricReporterTopic == null) {
            metricReporterTopic = ConfluentMetricsReporterConfig.DEFAULT_TOPIC_CONFIG;
        }
        return metricReporterTopic;
    }

    private static Properties getMetricConsumerProperties(Map<String, ?> configs) {
        String bootstrapServers = (String) configs.get(METRIC_REPORTER_SAMPLER_BOOTSTRAP_SERVERS);
        if (bootstrapServers == null) {
            bootstrapServers = configs.get(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG).toString();
        }
        String groupId = (String) configs.get(METRIC_REPORTER_SAMPLER_GROUP_ID);
        if (groupId == null) {
            groupId = DEFAULT_METRIC_REPORTER_SAMPLER_GROUP_ID + "-" + RANDOM.nextLong();
        }

        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "-consumer-" + RANDOM.nextInt());
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        consumerProps.putAll(configs);
        return consumerProps;
    }

    private static Consumer<byte[], byte[]> createConsumerForMetricTopic(
            Properties consumerProps, String metricReporterTopic) {
        Consumer<byte[], byte[]> metricConsumer = new KafkaConsumer<>(consumerProps);
        metricConsumer.subscribe(Pattern.compile(metricReporterTopic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                metricConsumer.commitSync();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                // Do nothing
            }
        });

        return metricConsumer;
    }

    // Instance method to allow unit test purpose, otherwise same as `createConsumerForMetricTopic` method
    void createMetricConsumer(Properties consumerProps, String metricReporterTopic) {
        metricConsumer = createConsumerForMetricTopic(consumerProps, metricReporterTopic);
    }

    void validateSamplingTopic(String metricReporterTopic) {
        if (!checkIfMetricReporterTopicExist(metricReporterTopic, metricConsumer)) {
            throw new IllegalStateException("Cruise Control cannot find sampling topic matches " + metricReporterTopic
                    + " in the target cluster.");
        }
    }

    private static boolean checkIfMetricReporterTopicExist(
            String metricReporterTopic, Consumer<byte[], byte[]> metricConsumer) {
        Pattern topicPattern = Pattern.compile(metricReporterTopic);
        for (String topic : metricConsumer.listTopics().keySet()) {
            if (topicPattern.matcher(topic).matches()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Make sure any condition needed to start this {@code CruiseControlComponent} is satisfied.
     */
    public static void checkStartupCondition(KafkaCruiseControlConfig config,
                                             Semaphore abortStartupCheck) throws InterruptedException {
        Map<String, Object> configPairs = config.mergedConfigValues();
        String metricReporterTopic = getMetricReporterTopic(configPairs);
        Properties metricConsumerProperties = getMetricConsumerProperties(configPairs);

        try (Consumer<byte[], byte[]> metricConsumer = createConsumerForMetricTopic(
                metricConsumerProperties, metricReporterTopic)) {
            long maxTimeoutSec = 60;
            long currentTimeoutInSec = 1;
            while (!checkIfMetricReporterTopicExist(metricReporterTopic, metricConsumer)) {
                LOG.info("Waiting for {} seconds for metric reporter topic {} to become available.",
                        currentTimeoutInSec, metricReporterTopic);
                if (abortStartupCheck.tryAcquire(currentTimeoutInSec, TimeUnit.SECONDS)) {
                    throw new StartupCheckInterruptedException();
                }
                currentTimeoutInSec = Math.min(2 * currentTimeoutInSec, maxTimeoutSec);
            }
        }

        LOG.info("Metric Reporter Sampler ready to start.");
    }

    @Override
    public void close() {
        metricConsumer.close();
    }
}
