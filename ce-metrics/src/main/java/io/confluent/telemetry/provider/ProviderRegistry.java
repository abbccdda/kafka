package io.confluent.telemetry.provider;

import static io.confluent.telemetry.provider.Utils.notEmptyString;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProviderRegistry {

  private static final Logger log = LoggerFactory.getLogger(ProviderRegistry.class);

  public static Map<String, String> providers = new HashMap<>();

  static {

    /**
     * Register provider
     * key is _namespace that indicates the component exposing metrics
     * value is implementation of {@link Provider} for the component
     */

    // Kafka broker
    registerProvider(KafkaServerProvider.NAMESPACE, KafkaServerProvider.class.getCanonicalName());

    // Kafka Connect
    registerProvider(KafkaConnectProvider.NAMESPACE, KafkaConnectProvider.class.getCanonicalName());

    // Kafka clients
    registerProvider(KafkaClientProvider.ADMIN_NAMESPACE,
        KafkaClientProvider.class.getCanonicalName());
    registerProvider(KafkaClientProvider.PRODUCER_NAMESPACE,
        KafkaClientProvider.class.getCanonicalName());
    registerProvider(KafkaClientProvider.CONSUMER_NAMESPACE,
        KafkaClientProvider.class.getCanonicalName());

    // Kafka Streams
    registerProvider(KafkaStreamsProvider.NAMESPACE, KafkaStreamsProvider.class.getCanonicalName());

    // KSQL
    registerProvider(KsqlProvider.NAMESPACE, KsqlProvider.class.getCanonicalName());

    // Schema Registry
    registerProvider(SchemaRegistryProvider.NAMESPACE,
        SchemaRegistryProvider.class.getCanonicalName());

    // Confluent Control Center
    registerProvider(ControlCenterProvider.NAMESPACE, ControlCenterProvider.class.getCanonicalName());
  }

  public static Provider getProvider(String namespace) {
    if (!notEmptyString(providers, namespace)) {
      return null;
    }

    try {
      Class<?> clazz = Class.forName(providers.get(namespace));
      final Object object = clazz.getDeclaredConstructor().newInstance();
      if (object instanceof Provider) {
        return (Provider) object;
      } else {
        log.error("Provider {} with class {} does not implement the Provider interface",
            namespace,
            providers.get(namespace));
      }
    } catch (Exception e) {
      log.error("error while creating provider", e);
    }
    return null;
  }

  public static void registerProvider(String namespace, String provider) {
    providers.put(namespace, provider);
  }
}
