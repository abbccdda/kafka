package io.confluent.telemetry.integration;

import static io.confluent.telemetry.integration.TestUtils.createNewConsumer;

import io.confluent.metrics.reporter.integration.MetricReporterClusterTestHarness;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import io.confluent.telemetry.serde.OpencensusMetricsProto;
import io.opencensus.proto.metrics.v1.Metric;
import java.util.Collections;
import java.util.Properties;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.junit.After;
import org.junit.Before;

public class TelemetryClusterTestHarness extends MetricReporterClusterTestHarness {

  protected KafkaConsumer<byte[], byte[]> consumer;
  protected Serde<Metric> serde = new OpencensusMetricsProto();

  @Before
  public void setUp() throws Exception {
    super.setUp();
    consumer = createNewConsumer(brokerList);
    consumer.subscribe(Collections.singleton(KafkaExporterConfig.DEFAULT_TOPIC_NAME));
  }

  @After
  public void tearDown() throws Exception {
    consumer.close();
    super.tearDown();
  }

  protected void injectMetricReporterProperties(Properties props, String brokerList) {
    props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
  }





}
