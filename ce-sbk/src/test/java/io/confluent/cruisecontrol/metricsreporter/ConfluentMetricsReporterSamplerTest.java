/*
 Copyright 2019 Confluent Inc.
 */

package io.confluent.cruisecontrol.metricsreporter;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import io.confluent.metrics.record.ConfluentMetric;
import io.confluent.serializers.ProtoSerde;
import org.apache.kafka.common.utils.Time;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager.BROKER_CAPACITY_CONFIG_RESOLVER_OBJECT_CONFIG;

public class ConfluentMetricsReporterSamplerTest {

    @Test
    public void testConfigure() {
        // should not throw exceptions with the default values
        ConfluentMetricsReporterSampler sampler = new ConfluentMetricsReporterSampler() {
            @Override
            void validateSamplingTopic(String metricReporterTopic) {

            }

            @Override
            void createMetricConsumer(Properties consumerProps, String metricReporterTopic) {

            }
        };
        Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
        props.put(BROKER_CAPACITY_CONFIG_RESOLVER_OBJECT_CONFIG, new BrokerCapacityConfigFileResolver());
        sampler.configure(new KafkaCruiseControlConfig(props).mergedConfigValues());
    }

    @Test
    public void testMetricsSampler() {
        ProtoSerde<ConfluentMetric.MetricsMessage> serde =
                new ProtoSerde<>(ConfluentMetric.MetricsMessage.getDefaultInstance());

        // Build metrics message
        long time = Time.SYSTEM.milliseconds();

        ConfluentMetric.MetricsMessage.Builder metricsMessageBuilder = ConfluentMetric.MetricsMessage.newBuilder();
        metricsMessageBuilder.setMetricType(ConfluentMetric.MetricType.BROKER);
        metricsMessageBuilder.setBrokerId(0);
        metricsMessageBuilder.setClientId("client");
        metricsMessageBuilder.setGroupId("kafka.server");
        metricsMessageBuilder.setClusterId("");
        metricsMessageBuilder.setTimestamp(time);

        // Each KafkaMeasurable should add 1 metric
        List<ConfluentMetric.KafkaMeasurable> kms = new ArrayList<>();

        ConfluentMetric.KafkaMeasurable.Builder kmBuilder = ConfluentMetric.KafkaMeasurable.newBuilder();
        kmBuilder.setValue(50.0);
        ConfluentMetric.KafkaMetricName.Builder nameBuilder = ConfluentMetric.KafkaMetricName.newBuilder();
        nameBuilder.setGroup("kafka.server");
        nameBuilder.setName("CpuUsage");
        nameBuilder.putTags("type", "BrokerTopicMetrics");
        kmBuilder.setMetricName(nameBuilder.build());
        ConfluentMetric.KafkaMeasurable km = kmBuilder.build();
        kms.add(km);

        kmBuilder.clear();
        kmBuilder = ConfluentMetric.KafkaMeasurable.newBuilder();
        kmBuilder.setValue(50.0);
        nameBuilder = ConfluentMetric.KafkaMetricName.newBuilder();
        nameBuilder.setGroup("kafka.server");
        nameBuilder.setName("BytesInPerSec");
        nameBuilder.putTags("type", "BrokerTopicMetrics");
        kmBuilder.setMetricName(nameBuilder.build());
        km = kmBuilder.build();
        kms.add(km);
        metricsMessageBuilder.addAllKafkaMeasurable(kms);

        ConfluentMetric.YammerMetricName.Builder yammerNameBuilder = ConfluentMetric.YammerMetricName.newBuilder();
        yammerNameBuilder.setGroup("kafka.server");
        yammerNameBuilder.setName("BytesInPerSec");
        yammerNameBuilder.setType("BrokerTopicMetrics");

        // Each gauge should add 1 metric
        ConfluentMetric.YammerGauge.Builder doubleGagueBuilder = ConfluentMetric.YammerGauge.newBuilder();
        doubleGagueBuilder.setMetricName(yammerNameBuilder.build());
        doubleGagueBuilder.setValue("50.0");
        doubleGagueBuilder.setDoubleValue(50.0);
        metricsMessageBuilder.addYammerGauge(doubleGagueBuilder.build());

        ConfluentMetric.YammerGauge.Builder longGaugeBuilder = ConfluentMetric.YammerGauge.newBuilder();
        longGaugeBuilder.setMetricName(yammerNameBuilder.build());
        longGaugeBuilder.setValue("50");
        longGaugeBuilder.setLongValue(50);
        metricsMessageBuilder.addYammerGauge(longGaugeBuilder.build());

        // Meter should add 1 metric
        ConfluentMetric.YammerMeter.Builder meterBuilder = ConfluentMetric.YammerMeter.newBuilder();
        meterBuilder.setMetricName(yammerNameBuilder.build());
        meterBuilder.setCount(10);
        meterBuilder.setDeltaCount(2);
        meterBuilder.setOneMinuteRate(50.0);
        meterBuilder.setFiveMinuteRate(50.0);
        meterBuilder.setFifteenMinuteRate(50.0);
        meterBuilder.setMeanRate(50.0);
        metricsMessageBuilder.addYammerMeter(meterBuilder.build());

        // Historgram should add 2 metrics
        ConfluentMetric.YammerHistogram.Builder histogramBuilder = ConfluentMetric.YammerHistogram.newBuilder();
        histogramBuilder.setMetricName(yammerNameBuilder.build());
        histogramBuilder.setCount(10);
        histogramBuilder.setDeltaCount(2);
        histogramBuilder.setMax(50.0);
        histogramBuilder.setMin(50.0);
        histogramBuilder.setMean(50.0);
        histogramBuilder.setStdDev(50.0);
        histogramBuilder.setSum(50.0);
        histogramBuilder.setMedian(50.0);
        histogramBuilder.setPercentile75Th(50.0);
        histogramBuilder.setPercentile95Th(50.0);
        histogramBuilder.setPercentile98Th(50.0);
        histogramBuilder.setPercentile99Th(50.0);
        histogramBuilder.setPercentile999Th(50.0);
        histogramBuilder.setSize(10);
        metricsMessageBuilder.addYammerHistogram(histogramBuilder.build());

        // Timer should add 3 metrics
        ConfluentMetric.YammerTimer.Builder timerBuilder = ConfluentMetric.YammerTimer.newBuilder();
        timerBuilder.setMetricName(yammerNameBuilder.build());
        timerBuilder.setCount(10);
        timerBuilder.setDeltaCount(2);
        timerBuilder.setMax(50.0);
        timerBuilder.setMin(50.0);
        timerBuilder.setMean(50.0);
        timerBuilder.setStdDev(50.0);
        timerBuilder.setSum(50.0);
        timerBuilder.setMedian(50.0);
        timerBuilder.setPercentile75Th(50.0);
        timerBuilder.setPercentile95Th(50.0);
        timerBuilder.setPercentile98Th(50.0);
        timerBuilder.setPercentile99Th(50.0);
        timerBuilder.setPercentile999Th(50.0);
        timerBuilder.setSize(10);
        timerBuilder.setOneMinuteRate(50.0);
        timerBuilder.setFiveMinuteRate(50.0);
        timerBuilder.setFifteenMinuteRate(50.0);
        timerBuilder.setMeanRate(50.0);
        metricsMessageBuilder.addYammerTimer(timerBuilder.build());

        byte[] metricsMessage = serde.serialize(metricsMessageBuilder.build());

        ConfluentMetricsReporterSampler sampler = new ConfluentMetricsReporterSampler();
        List<CruiseControlMetric> metricList = sampler.convertMetricRecord(metricsMessage);

        // There should be 10 total metrics
        Assert.assertEquals(10, metricList.size());
        for (CruiseControlMetric metric : metricList) {
            Assert.assertEquals(50.0, metric.value(), 0.00);
            Assert.assertEquals(time, metric.time());
            Assert.assertEquals(0, metric.brokerId());
        }
    }
}
