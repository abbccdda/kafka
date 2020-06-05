package io.confluent.telemetry.provider;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ProviderRegistryTest {

  @Test
  public void getDefaultProviders() {
    assertThat(ProviderRegistry.getProvider(KafkaServerProvider.NAMESPACE).getClass())
        .hasSameClassAs(KafkaServerProvider.class);
    assertThat(ProviderRegistry.getProvider(KafkaStreamsProvider.NAMESPACE).getClass())
        .hasSameClassAs(KafkaStreamsProvider.class);
    assertThat(ProviderRegistry.getProvider(KafkaClientProvider.CONSUMER_NAMESPACE).getClass())
        .hasSameClassAs(KafkaClientProvider.class);
    assertThat(ProviderRegistry.getProvider(KafkaClientProvider.PRODUCER_NAMESPACE).getClass())
        .hasSameClassAs(KafkaClientProvider.class);
    assertThat(ProviderRegistry.getProvider(KafkaClientProvider.ADMIN_NAMESPACE).getClass())
        .hasSameClassAs(KafkaClientProvider.class);
    assertThat(ProviderRegistry.getProvider(KsqlProvider.NAMESPACE).getClass())
        .hasSameClassAs(KsqlProvider.class);
    assertThat(ProviderRegistry.getProvider(SchemaRegistryProvider.NAMESPACE).getClass())
        .hasSameClassAs(SchemaRegistryProvider.class);
  }

  @Test
  public void registerProvider() {
    ProviderRegistry.registerProvider("foo", KafkaServerProvider.class.getCanonicalName());
    assertThat(ProviderRegistry.getProvider("foo").getClass())
        .hasSameClassAs(KafkaServerProvider.class);
  }

  @Test
  public void failGetProvider() {
    assertThat(ProviderRegistry.getProvider("abc")).isNull();

    ProviderRegistry.registerProvider("bar", String.class.getCanonicalName());
    assertThat(ProviderRegistry.getProvider("bar")).isNull();
  }

}
