package io.confluent.cruisecontrol.metricsreporter;

import com.google.protobuf.InvalidProtocolBufferException;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.BrokerMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.PartitionMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.telemetry.exporter.kafka.KafkaExporter;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.metrics.v1.SummaryValue;
import io.opencensus.proto.metrics.v1.TimeSeries;

/**
 * This class reads Confluent telemetry metrics as byte arrays from the metrics topic, converts them to Cruise Control
 * metrics, and submits them to the metrics processor
 */
public class ConfluentTelemetryReporterSampler extends ConfluentMetricsSamplerBase {
    // Constants are package-private for unit tests
    static final String TOPIC_KEY = "topic";
    static final String PARTITION_KEY = "partition";
    static final String BROKER_KEY = "kafka.broker.id";
    static final String REQUEST_TYPE_KEY = "request";
    static final String PRODUCE_REQUEST_TYPE = "Produce";
    static final String CONSUMER_FETCH_REQUEST_TYPE = "FetchConsumer";
    static final String FOLLOWER_FETCH_REQUEST_TYPE = "FetchFollower";
    static final String BYTES_IN_PER_SEC = "io.confluent.kafka.server/broker_topic/bytes_in/rate/1_min";
    static final String BYTES_OUT_PER_SEC = "io.confluent.kafka.server/broker_topic/bytes_out/rate/1_min";
    static final String REPLICATION_BYTES_IN_PER_SEC = "io.confluent.kafka.server/broker_topic/replication_bytes_in/rate/1_min";
    static final String REPLICATION_BYTES_OUT_PER_SEC = "io.confluent.kafka.server/broker_topic/replication_bytes_out/rate/1_min";
    static final String TOTAL_FETCH_REQUEST_PER_SEC = "io.confluent.kafka.server/broker_topic/total_fetch_requests/rate/1_min";
    static final String TOTAL_PRODUCE_REQUEST_PER_SEC = "io.confluent.kafka.server/broker_topic/total_produce_requests/rate/1_min";
    static final String MESSAGES_IN_PER_SEC = "io.confluent.kafka.server/broker_topic/messages_in/rate/1_min";
    static final String REQUESTS_PER_SEC = "io.confluent.kafka.server/request/requests/rate/1_min";
    static final String REQUEST_QUEUE_SIZE = "io.confluent.kafka.server/request_channel/request_queue_size";
    static final String RESPONSE_QUEUE_SIZE = "io.confluent.kafka.server/request_channel/response_queue_size";
    static final String REQUEST_QUEUE_TIME_MS = "io.confluent.kafka.server/request/request_queue_time_ms";
    static final String LOCAL_TIME_MS = "io.confluent.kafka.server/request/local_time_ms";
    static final String TOTAL_TIME_MS = "io.confluent.kafka.server/request/total_time_ms";
    static final String SIZE = "io.confluent.kafka.server/log/size";
    static final String LOG_FLUSH_RATE = "io.confluent.kafka.server/log_flush/log_flush_rate_and_time_ms/rate/1_min";
    static final String LOG_FLUSH_TIME_MS = "io.confluent.kafka.server/log_flush/log_flush_rate_and_time_ms";
    static final String REQUEST_HANDLER_AVG_IDLE_PERCENT = "io.confluent.kafka.server/request_handler_pool/request_handler_avg_idle_percent/rate/1_min";
    static final String CPU_USAGE = "io.confluent.kafka.server/cpu/cpu_usage";
    static final String DISK_TOTAL_BYTES = "io.confluent.kafka.server/volume/disk_total_bytes";

    // Define mappings from telemetry metric names and labels to groups of Cruise Control topics
    private static final Map<String, TopicAndAllTopicMetricTypes> TOPIC_METRIC_MAP = buildTopicMetricMap();
    private static final Map<String, Map<String, TimerMetricTypes>> REQUEST_TIMER_METRIC_MAP = buildRequestTimerMetricMap();
    private static final TimerMetricTypes LOG_FLUSH_TIMER_METRIC_TYPES = new TimerMetricTypes(RawMetricType.BROKER_LOG_FLUSH_TIME_MS_50TH,
            RawMetricType.BROKER_LOG_FLUSH_TIME_MS_999TH, RawMetricType.BROKER_LOG_FLUSH_TIME_MS_MAX, RawMetricType.BROKER_LOG_FLUSH_TIME_MS_MEAN);


    private static int telemetryMessageVersion(ConsumerRecord<byte[], byte[]> record) {
        Header versionHeader = record.headers().lastHeader(KafkaExporter.VERSION_HEADER_KEY);
        if (versionHeader != null) {
            return ByteBuffer.wrap(versionHeader.value()).order(ByteOrder.LITTLE_ENDIAN).getInt();
        }
        // assume version 0 if version header not present
        return 0;
    }

    @Override
    protected List<CruiseControlMetric> convertMetricRecord(ConsumerRecord<byte[], byte[]> record) {
        if (telemetryMessageVersion(record) != 0) {
            return Collections.emptyList();
        }

        Metric metric;
        try {
            metric = Metric.parseFrom(record.value());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Received exception when parsing metric data", e);
            return Collections.emptyList();
        }

        List<CruiseControlMetric> ccMetrics = new ArrayList<>();
        for (TimeSeries ts : metric.getTimeseriesList()) {
            // Add common resourceLabels.
            Map<String, String> labels = new HashMap<>(metric.getResource().getLabelsMap());

            // Add labels to record.
            for (int i = 0; i < metric.getMetricDescriptor().getLabelKeysCount(); i++) {
                labels.put(metric.getMetricDescriptor().getLabelKeys(i).getKey(), ts.getLabelValues(i).getValue());
            }

            for (Point point : ts.getPointsList()) {
                createCruiseControlMetrics(metric.getMetricDescriptor().getName(), point, labels, ccMetrics);
            }
        }

        return ccMetrics;
    }

    @Override
    protected String defaultMetricSamplerGroupId() {
        return "ConfluentTelemetryReporterSampler";
    }

    private void createCruiseControlMetrics(String name, Point point, Map<String, String> labels, List<CruiseControlMetric> ccMetrics) {
        String topic = labels.get(TOPIC_KEY);
        if (labels.get(BROKER_KEY) == null) {
            return;
        }
        int brokerId = Integer.parseInt(labels.get(BROKER_KEY));
        long timestamp = Instant.ofEpochSecond(point.getTimestamp().getSeconds(), point.getTimestamp().getNanos()).toEpochMilli();
        SummaryValue.Snapshot snapshot = point.getSummaryValue().getSnapshot();
        switch (name) {
            // These metrics all have an instance per topic as well as an instance for all topics together. If the topic label
            // is present, the instance is per-topic. TOPIC_METRIC_MAP defines the mapping from the telemetry metric name to
            // the Cruise Control metric types for the individual topic and all-topic metrics.
            case BYTES_IN_PER_SEC:
            case BYTES_OUT_PER_SEC:
            case REPLICATION_BYTES_IN_PER_SEC:
            case REPLICATION_BYTES_OUT_PER_SEC:
            case TOTAL_FETCH_REQUEST_PER_SEC:
            case TOTAL_PRODUCE_REQUEST_PER_SEC:
            case MESSAGES_IN_PER_SEC:
                ccMetrics.add(buildTopicOrAllTopicMetric(name, topic, timestamp, brokerId, point.getDoubleValue()));
                break;

            case REQUESTS_PER_SEC:
                switch (labels.get(REQUEST_TYPE_KEY)) {
                    case PRODUCE_REQUEST_TYPE:
                        ccMetrics.add(new BrokerMetric(RawMetricType.BROKER_PRODUCE_REQUEST_RATE, timestamp, brokerId, point.getDoubleValue()));
                        break;
                    case CONSUMER_FETCH_REQUEST_TYPE:
                        ccMetrics.add(new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_RATE, timestamp, brokerId, point.getDoubleValue()));
                        break;
                    case FOLLOWER_FETCH_REQUEST_TYPE:
                        ccMetrics.add(new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_RATE, timestamp, brokerId, point.getDoubleValue()));
                        break;
                }
                break;

            case REQUEST_QUEUE_SIZE:
                ccMetrics.add(new BrokerMetric(RawMetricType.BROKER_REQUEST_QUEUE_SIZE, timestamp, brokerId, point.getInt64Value()));
                break;

            case RESPONSE_QUEUE_SIZE:
                ccMetrics.add(new BrokerMetric(RawMetricType.BROKER_RESPONSE_QUEUE_SIZE, timestamp, brokerId, point.getInt64Value()));
                break;

            // These metrics all have an instance for each of {Produce, Consumer Fetch, Follower Fetch}, each of which has
            // values for 50th percentile, 99.9th percentile, and mean
            case REQUEST_QUEUE_TIME_MS:
            case LOCAL_TIME_MS:
            case TOTAL_TIME_MS:
                if (REQUEST_TIMER_METRIC_MAP.get(name).containsKey(labels.get(REQUEST_TYPE_KEY))) {
                    ccMetrics.addAll(buildTimerMetrics(REQUEST_TIMER_METRIC_MAP.get(name).get(labels.get(REQUEST_TYPE_KEY)), snapshot,
                            timestamp, brokerId));
                }
                break;

            case SIZE:
                int partition = Integer.parseInt(labels.get(PARTITION_KEY));
                ccMetrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, timestamp, brokerId, topic, partition, point.getInt64Value()));
                break;

            case LOG_FLUSH_RATE:
                ccMetrics.add(new BrokerMetric(RawMetricType.BROKER_LOG_FLUSH_RATE, timestamp, brokerId, point.getDoubleValue()));
                break;

            case LOG_FLUSH_TIME_MS:
                ccMetrics.addAll(buildTimerMetrics(LOG_FLUSH_TIMER_METRIC_TYPES, snapshot, timestamp, brokerId));
                break;

            case REQUEST_HANDLER_AVG_IDLE_PERCENT:
                ccMetrics.add(new BrokerMetric(RawMetricType.BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT, timestamp, brokerId, point.getDoubleValue()));
                break;

            case CPU_USAGE:
                ccMetrics.add(new BrokerMetric(RawMetricType.BROKER_CPU_UTIL, timestamp, brokerId, point.getDoubleValue()));
                break;

            case DISK_TOTAL_BYTES:
                ccMetrics.add(new BrokerMetric(RawMetricType.BROKER_DISK_CAPACITY, timestamp, brokerId, point.getInt64Value()));
                break;
        }
    }

    private CruiseControlMetric buildTopicOrAllTopicMetric(String name, String topic, long timestamp, int brokerId, double value) {
        if (topic != null) {
            return new TopicMetric(TOPIC_METRIC_MAP.get(name).topicMetricType, timestamp, brokerId, topic, value);
        } else {
            return new BrokerMetric(TOPIC_METRIC_MAP.get(name).allTopicMetricType, timestamp, brokerId, value);
        }
    }

    private List<CruiseControlMetric> buildTimerMetrics(TimerMetricTypes metricTypes, SummaryValue.Snapshot snapshot, long timestamp, int brokerId) {
        List<CruiseControlMetric> ccMetrics = new ArrayList<>();

        for (SummaryValue.Snapshot.ValueAtPercentile percentileValue : snapshot.getPercentileValuesList()) {
            switch ((int) (percentileValue.getPercentile() * 10)) {
                case 500:
                    ccMetrics.add(new BrokerMetric(metricTypes.percentile500Type, timestamp, brokerId, percentileValue.getValue()));
                    break;
                case 999:
                    ccMetrics.add(new BrokerMetric(metricTypes.percentile999Type, timestamp, brokerId, percentileValue.getValue()));
                    break;
                case 1000:
                    ccMetrics.add(new BrokerMetric(metricTypes.maxType, timestamp, brokerId, percentileValue.getValue()));
            }
        }

        ccMetrics.add(new BrokerMetric(metricTypes.meanType, timestamp,
                brokerId, snapshot.getSum().getValue() / snapshot.getCount().getValue()));

        return ccMetrics;
    }

    // For each topic metric, map the telemetry metric name to Cruise Control's per-topic and all-topic types
    private static Map<String, TopicAndAllTopicMetricTypes> buildTopicMetricMap() {
        Map<String, TopicAndAllTopicMetricTypes> map = new HashMap<>();
        map.put(BYTES_IN_PER_SEC, new TopicAndAllTopicMetricTypes(RawMetricType.TOPIC_BYTES_IN, RawMetricType.ALL_TOPIC_BYTES_IN));
        map.put(BYTES_OUT_PER_SEC, new TopicAndAllTopicMetricTypes(RawMetricType.TOPIC_BYTES_OUT, RawMetricType.ALL_TOPIC_BYTES_OUT));
        map.put(REPLICATION_BYTES_IN_PER_SEC, new TopicAndAllTopicMetricTypes(RawMetricType.TOPIC_REPLICATION_BYTES_IN, RawMetricType.ALL_TOPIC_REPLICATION_BYTES_IN));
        map.put(REPLICATION_BYTES_OUT_PER_SEC, new TopicAndAllTopicMetricTypes(RawMetricType.TOPIC_REPLICATION_BYTES_OUT, RawMetricType.ALL_TOPIC_REPLICATION_BYTES_OUT));
        map.put(TOTAL_FETCH_REQUEST_PER_SEC, new TopicAndAllTopicMetricTypes(RawMetricType.TOPIC_FETCH_REQUEST_RATE, RawMetricType.ALL_TOPIC_FETCH_REQUEST_RATE));
        map.put(TOTAL_PRODUCE_REQUEST_PER_SEC, new TopicAndAllTopicMetricTypes(RawMetricType.TOPIC_PRODUCE_REQUEST_RATE, RawMetricType.ALL_TOPIC_PRODUCE_REQUEST_RATE));
        map.put(MESSAGES_IN_PER_SEC, new TopicAndAllTopicMetricTypes(RawMetricType.TOPIC_MESSAGES_IN_PER_SEC, RawMetricType.ALL_TOPIC_MESSAGES_IN_PER_SEC));
        return map;
    }

    // For each request time metric, map the telemetry metric name the the request type label to Cruise Control's 50th percentile,
    // 99.9th percentile, and mean metric types
    private static Map<String, Map<String, TimerMetricTypes>> buildRequestTimerMetricMap() {
        Map<String, Map<String, TimerMetricTypes>> map = new HashMap<>();
        Map<String, TimerMetricTypes> requestQueueTimeMap = new HashMap<>();
        requestQueueTimeMap.put(PRODUCE_REQUEST_TYPE, new TimerMetricTypes(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH,
                RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH, RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX,
                RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN));
        requestQueueTimeMap.put(CONSUMER_FETCH_REQUEST_TYPE, new TimerMetricTypes(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH,
                RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH, RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX,
                RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN));
        requestQueueTimeMap.put(FOLLOWER_FETCH_REQUEST_TYPE, new TimerMetricTypes(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH,
                RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH, RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX,
                RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN));
        map.put(REQUEST_QUEUE_TIME_MS, requestQueueTimeMap);

        Map<String, TimerMetricTypes> localTimeMap = new HashMap<>();
        localTimeMap.put(PRODUCE_REQUEST_TYPE, new TimerMetricTypes(RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_50TH,
                RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_999TH, RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_MAX,
                RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_MEAN));
        localTimeMap.put(CONSUMER_FETCH_REQUEST_TYPE, new TimerMetricTypes(RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH,
                RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH, RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX,
                RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN));
        localTimeMap.put(FOLLOWER_FETCH_REQUEST_TYPE, new TimerMetricTypes(RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH,
                RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH, RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX,
                RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN));
        map.put(LOCAL_TIME_MS, localTimeMap);

        Map<String, TimerMetricTypes> totalTimeMap = new HashMap<>();
        totalTimeMap.put(PRODUCE_REQUEST_TYPE, new TimerMetricTypes(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_50TH,
                RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_999TH, RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MAX,
                RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MEAN));
        totalTimeMap.put(CONSUMER_FETCH_REQUEST_TYPE, new TimerMetricTypes(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH,
                RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH, RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX,
                RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN));
        totalTimeMap.put(FOLLOWER_FETCH_REQUEST_TYPE, new TimerMetricTypes(RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH,
                RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH, RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX,
                RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN));
        map.put(TOTAL_TIME_MS, totalTimeMap);

        return map;
    }

    private static class TopicAndAllTopicMetricTypes {
        RawMetricType topicMetricType;
        RawMetricType allTopicMetricType;

        TopicAndAllTopicMetricTypes(RawMetricType topicMetricType, RawMetricType allTopicMetricType) {
            this.topicMetricType = topicMetricType;
            this.allTopicMetricType = allTopicMetricType;
        }
    }

    private static class TimerMetricTypes {
        RawMetricType percentile500Type;
        RawMetricType percentile999Type;
        RawMetricType maxType;
        RawMetricType meanType;

        TimerMetricTypes(RawMetricType percentile500Type, RawMetricType percentile999Type,
                         RawMetricType maxType, RawMetricType meanType) {
            this.percentile500Type = percentile500Type;
            this.percentile999Type = percentile999Type;
            this.maxType = maxType;
            this.meanType = meanType;
        }
    }
}
