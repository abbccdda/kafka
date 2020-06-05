package io.confluent.telemetry.provider;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.MetricsContext;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class UtilsTest {

  @Test
  public void testNotEmptyString() {
    Map<String, Object> a = new HashMap<>();
    a.put("A", "b");
    a.put("B", null);
    a.put("C", Integer.parseInt("1"));

    assertThat(Utils.notEmptyString(a, "A")).isTrue();
    assertThat(Utils.notEmptyString(a, "B")).isFalse();
    assertThat(Utils.notEmptyString(a, "C")).isFalse();
    assertThat(Utils.notEmptyString(a, "Z")).isFalse();
  }

  @Test
  public void getResourceLabels() {
    Map<String, String> actual = ImmutableMap.of(
        MetricsContext.NAMESPACE, "foo",
        ConfluentConfigs.RESOURCE_LABEL_TYPE, "CONNECT",
        ConfluentConfigs.RESOURCE_LABEL_VERSION, "v1",
        ConfluentConfigs.RESOURCE_LABEL_PREFIX + "region", "test",
        ConfluentConfigs.RESOURCE_LABEL_PREFIX + "pkc", "pkc-bar"
    );

    assertThat(Utils.getResourceLabels(actual)).containsOnly(
        Assertions.entry("type", "CONNECT"),
        Assertions.entry("version", "v1"),
        Assertions.entry("region", "test"),
        Assertions.entry("pkc", "pkc-bar")
        );
  }

}