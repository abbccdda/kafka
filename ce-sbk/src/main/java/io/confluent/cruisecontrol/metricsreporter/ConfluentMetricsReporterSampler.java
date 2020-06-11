/*
 Copyright 2019 Confluent Inc.
 */

package io.confluent.cruisecontrol.metricsreporter;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.BrokerMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricsUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.YammerMetricWrapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;

import io.confluent.metrics.record.ConfluentMetric;
import io.confluent.serializers.ProtoSerde;

/**
 * This class reads Kafka and Yammer metrics as byte arrays from the metrics topic, converts them to Cruise Control
 * metrics, and submits them to the metrics processor
 */
public class ConfluentMetricsReporterSampler extends ConfluentMetricsSamplerBase {
    private ProtoSerde<ConfluentMetric.MetricsMessage> serde =
            new ProtoSerde<>(ConfluentMetric.MetricsMessage.getDefaultInstance());

    @Override
    protected List<CruiseControlMetric> convertMetricRecord(ConsumerRecord<byte[], byte[]> confluentMetric) {
        List<CruiseControlMetric> metricList = new ArrayList<>();
        ConfluentMetric.MetricsMessage metricsMessage = serde.deserialize(confluentMetric.value());
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
            YammerMetricWrapper metricWrapper = new YammerMetricWrapper(convertYammerMetricName(gauge.getMetricName()));
            if (MetricsUtils.isInterested(metricWrapper)) {
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
                            metricWrapper,
                            value));
                }
            }
        }
        for (ConfluentMetric.YammerMeter meter : metricsMessage.getYammerMeterList()) {
            YammerMetricWrapper metricWrapper = new YammerMetricWrapper(convertYammerMetricName(meter.getMetricName()));
            if (MetricsUtils.isInterested(metricWrapper)) {
                this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        metricWrapper,
                        meter.getOneMinuteRate()));
            }
        }
        for (ConfluentMetric.YammerTimer timer : metricsMessage.getYammerTimerList()) {
            YammerMetricWrapper metricWrapper = new YammerMetricWrapper(convertYammerMetricName(timer.getMetricName()));
            if (MetricsUtils.isInterested(metricWrapper)) {
                this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        metricWrapper,
                        timer.getOneMinuteRate()));
                this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        metricWrapper,
                        timer.getMax(),
                        MetricsUtils.ATTRIBUTE_MAX));
                this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        metricWrapper,
                        timer.getMean(),
                        MetricsUtils.ATTRIBUTE_MEAN));
            }
        }
        for (ConfluentMetric.YammerHistogram histogram : metricsMessage.getYammerHistogramList()) {
            YammerMetricWrapper metricWrapper = new YammerMetricWrapper(convertYammerMetricName(histogram.getMetricName()));
            if (MetricsUtils.isInterested(metricWrapper)) {
                this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        metricWrapper,
                        histogram.getMax(),
                        MetricsUtils.ATTRIBUTE_MAX));
                this.addIfNotNull(metricList, MetricsUtils.toCruiseControlMetric(
                        metricsMessage.getTimestamp(),
                        metricsMessage.getBrokerId(),
                        metricWrapper,
                        histogram.getMean(),
                        MetricsUtils.ATTRIBUTE_MEAN));
            }
        }

        // Handle disk size as a special case since it's not handled by CC MetricsUtils
        CruiseControlMetric cruiseControlVolumeMetrics = convertVolumeMetrics(metricsMessage);
        this.addIfNotNull(metricList, cruiseControlVolumeMetrics);

        return metricList;
    }

    @Override
    protected String defaultMetricSamplerGroupId() {
        return "ConfluentMetricsReporterSampler";
    }

    private CruiseControlMetric convertVolumeMetrics(ConfluentMetric.MetricsMessage metricsMessage) {
        List<ConfluentMetric.VolumeMetrics> volumes = metricsMessage.getSystemMetrics().getVolumesList();
        if (!volumes.isEmpty()) {
            if (volumes.size() > 1) {
                throw new IllegalStateException("Dynamic disk size estimation not supported for multiple volumes");
            }
            double diskTotalBytes = (double) volumes.get(0).getTotalBytes();
            return new BrokerMetric(RawMetricType.BROKER_DISK_CAPACITY, metricsMessage.getTimestamp(),
                    metricsMessage.getBrokerId(), diskTotalBytes);
        } else {
            return null;
        }
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
}
