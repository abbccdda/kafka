package io.confluent.telemetry;


import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MetricsUtilsTest {

  private static final String DOMAIN = "io.confluent.kafka.server";

  @Test
  public void nameForKafkaMetric() {
    assertEquals(
        "io.confluent.kafka.server/tenant/error_total",
        MetricsUtils.fullMetricName(DOMAIN, "tenant-metrics", "error-total")
    );
  }

  @Test
  public void nameForYammerMeter() {
    assertEquals(
        "io.confluent.kafka.server/simple_acl_authorizer/zookeeper_sync_connects",
        MetricsUtils.fullMetricName(DOMAIN, "SimpleAclAuthorizer", "ZooKeeperSyncConnectsPerSec")
    );
  }
  @Test
  public void nameForYammerGauge() {
    assertEquals(
        "io.confluent.kafka.server/simple_acl_authorizer/session_state",
        MetricsUtils.fullMetricName(DOMAIN, "SimpleAclAuthorizer", "SessionState")
    );
  }

  @Test
  public void nameForYammerTimer() {
    assertEquals(
        "io.confluent.kafka.server/controller/partition_reassignment_rate_and_time_ms",
        MetricsUtils.fullMetricName(DOMAIN, "ControllerStats", "PartitionReassignmentRateAndTimeMs")
    );
  }
  @Test
  public void nameForRedundantKafka() {
    assertEquals(
        "io.confluent.kafka.server/controller/global_partition_count",
        MetricsUtils.fullMetricName(DOMAIN, "KafkaController", "GlobalPartitionCount")
    );
  }

  @Test
  public void nameForYammerHistogram() {
    assertEquals(
        "io.confluent.kafka.server/controller_event_manager/event_queue_time_ms",
        MetricsUtils.fullMetricName(DOMAIN, "ControllerEventManager", "EventQueueTimeMs")
    );
  }
}