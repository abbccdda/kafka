package io.confluent.telemetry.provider;

import static io.confluent.telemetry.provider.Utils.configEvent;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.protobuf.InvalidProtocolBufferException;
import io.cloudevents.CloudEvent;
import io.cloudevents.v1.AttributesImpl;
import io.confluent.telemetry.events.v1.ConfigEvent;
import io.opencensus.proto.resource.v1.Resource;
import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import scala.Option;

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

  @Test
  public void kafkaServerConfigEvent() throws InvalidProtocolBufferException {
    Properties brokerProps = TestUtils.createBrokerConfig(
            1, "localhost:1234", true, true,
            TestUtils.RandomPort(), Option.<SecurityProtocol>empty(), Option.<File>empty(), Option.<Properties>empty(), true,
            false, TestUtils.RandomPort(), false, TestUtils.RandomPort(), false, TestUtils.RandomPort(),
            Option.<String>empty(), 1, false, 1, (short) 1);

    KafkaConfig config = KafkaConfig.fromProps(brokerProps);

    CloudEvent<AttributesImpl, ConfigEvent> e = configEvent(config.values(), event -> true, Resource.newBuilder().putLabels("a", "b").build(), "test");
    assertThat(e.getData().isPresent()).isTrue();
    ConfigEvent ce = e.getData().get();
    // Int
    assertThat(ce.getConfig().getDataOrThrow(KafkaConfig.BrokerIdProp()).getIntValue()).isEqualTo(1);
    // Double
    assertThat(ce.getConfig().getDataOrThrow(KafkaConfig.SaslLoginRefreshWindowFactorProp()).getDoubleValue()).isEqualTo(0.8);
    // String
    assertThat(ce.getConfig().getDataOrThrow(KafkaConfig.SslEndpointIdentificationAlgorithmProp()).getStringValue()).isEqualTo("https");
    // Class
    assertThat(ce.getConfig().getDataOrThrow(KafkaConfig.BrokerInterceptorClassProp()).getStringValue()).isEqualTo("org.apache.kafka.server.interceptor.DefaultBrokerInterceptor");
    // Boolean
    assertThat(ce.getConfig().getDataOrThrow(KafkaConfig.LogMessageDownConversionEnableProp()).getBoolValue()).isTrue();
    // List
    assertThat(ce.getConfig().getDataOrThrow(KafkaConfig.SaslKerberosPrincipalToLocalRulesProp()).getArrayValue().getValuesList().size()).isGreaterThanOrEqualTo(1);
  }
}