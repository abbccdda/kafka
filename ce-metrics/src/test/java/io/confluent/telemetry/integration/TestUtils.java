package io.confluent.telemetry.integration;

import io.opencensus.proto.metrics.v1.LabelKey;
import io.opencensus.proto.metrics.v1.Metric;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class TestUtils {

  public static boolean labelExists(Metric m, String key) {
    return m.getMetricDescriptor().getLabelKeysList()
        .contains(LabelKey.newBuilder().setKey(key).build());
  }

  public static String getLabelValueFromFirstTimeSeries(Metric m, String key) {
    int index = m.getMetricDescriptor().getLabelKeysList()
        .indexOf(LabelKey.newBuilder().setKey(key).build());
    return m.getTimeseries(0).getLabelValues(index).getValue();
  }

  public static KafkaConsumer<byte[], byte[]> createNewConsumer(String brokerList) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "telemetry-metric-reporter-consumer");
    // The metric topic may not be there initially. So, we need to refresh metadata more frequently to pick it up once created.
    properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "400");
    return new KafkaConsumer<>(properties, new ByteArrayDeserializer(),
        new ByteArrayDeserializer());
  }

  public static AdminClient createAdminClient(String brokerList) {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    return AdminClient.create(properties);
  }

  public static boolean newRecordsCheck(KafkaConsumer<byte[], byte[]> consumer, String topic,
                                        long timeout, boolean expectEmpty) {
    consumer.unsubscribe();
    consumer.subscribe(Collections.singletonList(topic));
    consumer.seekToEnd(Collections.emptyList());
    long startMs = System.currentTimeMillis();
    while (System.currentTimeMillis() - startMs < timeout) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
      if (expectEmpty == records.isEmpty()) {
        return true;
      }
    }
    return false;
  }
}
