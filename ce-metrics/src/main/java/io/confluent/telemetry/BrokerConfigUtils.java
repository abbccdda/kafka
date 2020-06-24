package io.confluent.telemetry;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import kafka.server.KafkaConfig;
import kafka.server.Defaults;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This class was created to extract the interbroker client configs from the broker at runtime.
 *
 * Since the reporter is passed all of the broker configs during initialization, we decided that
 * it makes more sense to just extract them here as opposed to trying to duplicate configs and
 * special-case our reporter inside the broker code.
 *
 * This is essentially copied code from the KafkaConfig and Endpoint broker classes.
 */
public class BrokerConfigUtils {

  private static final Logger log = LoggerFactory.getLogger(BrokerConfigUtils.class);

  protected static final String RACK_PROP = KafkaConfig.RackProp();

  protected static final String HOST_NAME_PROP = KafkaConfig.HostNameProp();
  protected static final String HOST_NAME_DEFAULT = Defaults.HostName();

  protected static final String PORT_PROP = KafkaConfig.PortProp();
  protected static final int PORT_DEFAULT = Defaults.Port();

  protected static final String LISTENERS_PROP = KafkaConfig.ListenersProp();

  protected static final String LISTENER_SECURITY_PROTOCOL_MAP_PROP = KafkaConfig.ListenerSecurityProtocolMapProp();
  protected static final String LISTENER_SECURITY_PROTOCOL_MAP_DEFAULT =
          Joiner.on(",").withKeyValueSeparator(":").join(
            Arrays.stream(SecurityProtocol.values())
              .collect(Collectors.toMap(sp -> ListenerName.forSecurityProtocol(sp).value(), sp -> sp))
          );

  protected static final String INTER_BROKER_SECURITY_PROTOCOL_PROP = KafkaConfig.InterBrokerSecurityProtocolProp();
  protected static final String INTER_BROKER_SECURITY_PROTOCOL_DEFAULT = SecurityProtocol.PLAINTEXT.toString();

  protected static final String INTER_BROKER_LISTENER_NAME_PROP = KafkaConfig.InterBrokerListenerNameProp();
  protected static final String INTER_BROKER_LISTENER_NAME_DEFAULT = null;

  protected static final String URI_PARSE_REGEX_STRING = "^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)";

  private static String getHostName(Map<String, Object> config) {
    return getStringOrDefault(config, HOST_NAME_PROP, HOST_NAME_DEFAULT);
  }

  private static String getPort(Map<String, Object> config) {
    return getStringOrDefault(config, PORT_PROP, Integer.toString(PORT_DEFAULT));
  }

  private static String getListeners(Map<String, Object> config) {
    return getStringOrDefault(config, LISTENERS_PROP,
            "PLAINTEXT://" + getHostName(config) + ":" + getPort(config));
  }

  private static String getInterBrokerSecurityProtocol(Map<String, Object> config) {
    return getStringOrDefault(config, INTER_BROKER_SECURITY_PROTOCOL_PROP, INTER_BROKER_SECURITY_PROTOCOL_DEFAULT);
  }

  private static String getListenerSecurityProtocolMap(Map<String, Object> config) {
    return getStringOrDefault(config, LISTENER_SECURITY_PROTOCOL_MAP_PROP, LISTENER_SECURITY_PROTOCOL_MAP_DEFAULT);
  }

  private static String getInterBrokerListenerName(Map<String, Object> config) {
    return getStringOrDefault(config, INTER_BROKER_LISTENER_NAME_PROP, INTER_BROKER_LISTENER_NAME_DEFAULT);
  }

  private static String getStringOrDefault(Map<String, Object> config, String prop, String defaultValue) {
    Object val = config.get(prop);
    if (val == null) {
      return defaultValue;
    } else {
      return val.toString();
    }
  }

  private static Map<ListenerName, SecurityProtocol> listenerSecurityProtocolMap(Map<String, Object> config) {
    String value = getListenerSecurityProtocolMap(config);
    Map<ListenerName, SecurityProtocol> map = Maps.newHashMap();
    if (Strings.isNullOrEmpty(value)) {
      return map;
    }
    return
        Arrays.stream(value.split("\\s*,\\s*"))
            .collect(
                Collectors.toMap(
                    s -> ListenerName.normalised(s.substring(0, s.lastIndexOf(":")).trim()),
                    s -> SecurityProtocol.forName(s.substring(s.lastIndexOf(":") + 1).trim())
                )
            );
  }

  private static Map.Entry<ListenerName, SecurityProtocol> deriveInterBrokerListener(Map<String, Object> config, Map<ListenerName, SecurityProtocol> listenerSecurityProtocolMap) {
    String listenerNameStr = getInterBrokerListenerName(config);
    // We're omitting the check for both 'security.inter.broker.protocol' & 'inter.broker.listener.name'
    // because upon reconfiguration, the default value for 'security.inter.broker.protocol' is being set.
    // We can infer that if 'inter.broker.listener.name' is set, 'security.inter.broker.protocol' is not.
    // This is slightly different than the broker code for this reason.
    if (!Strings.isNullOrEmpty(listenerNameStr)) {
      ListenerName listenerName = ListenerName.normalised(listenerNameStr);
      SecurityProtocol securityProtocol = listenerSecurityProtocolMap.get(listenerName);
      if (securityProtocol == null) {
        // this should get caught by the broker itself
        throw new ConfigException("no security protocol for the provided listener name");
      }
      return new AbstractMap.SimpleEntry<>(listenerName, securityProtocol);
    } else {
      SecurityProtocol securityProtocol = SecurityProtocol.forName(getInterBrokerSecurityProtocol(config));
      return new AbstractMap.SimpleEntry<>(ListenerName.forSecurityProtocol(securityProtocol), securityProtocol);
    }
  }

  private static Endpoint createEndPoint(String connectionString, Map<ListenerName, SecurityProtocol> listenerSecurityProtocolMap) {
    Pattern uriParseRegex = Pattern.compile(URI_PARSE_REGEX_STRING);
    Matcher matcher = uriParseRegex.matcher(connectionString);
    if (!matcher.find()) {
      // this should get caught by the broker itself
      throw new ConfigException(connectionString + " is not a valid listener");
    }
    String listenerNameStr = matcher.group(1);
    String host = matcher.group(2);
    String port = matcher.group(3);
    ListenerName listenerName = ListenerName.normalised(listenerNameStr);
    return new Endpoint(listenerName.value(), listenerSecurityProtocolMap.get(listenerName), (host.isEmpty()) ? null : host, Integer.parseInt(port));
  }

  private static List<Endpoint> getEndpoints(Map<String, Object> config, Map<ListenerName, SecurityProtocol> listenerSecurityProtocolMap) {
    String listeners = getListeners(config);
    List<String> listenersList =
          Arrays.stream(listeners.split("\\s*,\\s*"))
              .filter(v -> !v.equals(""))
              .collect(Collectors.toList());
    return listenersList.stream()
        .map(l -> createEndPoint(l, listenerSecurityProtocolMap))
        .collect(Collectors.toList());
  }

  public static Endpoint getInterBrokerEndpoint(Map<String, Object> config) {
    Map<ListenerName, SecurityProtocol> listenerSecurityProtocolMap = listenerSecurityProtocolMap(config);
    ListenerName listenerName = deriveInterBrokerListener(config, listenerSecurityProtocolMap).getKey();
    List<Endpoint> endpoints = getEndpoints(config, listenerSecurityProtocolMap).stream()
        .filter(e -> {
          if (e.listenerName().isPresent()) {
            return e.listenerName().get().equals(listenerName.value());
          }
          return true;
        })
        .collect(Collectors.toList());
    if (endpoints.size() != 1) {
      // this should get caught by the broker itself
      throw new ConfigException("expecting a single interbroker endpoint");
    }
    Endpoint endpoint = endpoints.get(0);
    if (endpoint.port() == 0) {
      log.warn("Interbroker listener port is 0. Local exporter configuration may be invalid.");
    }
    return endpoint;
  }

  private static final MethodHandle INTER_BROKER_CLIENT_CONFIGS_METHOD;
  private static final Exception INTER_BROKER_CLIENT_CONFIGS_EXCEPTION;

  static {
    MethodHandle method = null;
    Exception exception = null;
    try {
      method =
          MethodHandles.publicLookup().findStatic(
              Class.forName("org.apache.kafka.common.config.internals.ConfluentConfigs"),
              "interBrokerClientConfigs",
              // note: first parameter is return-type
              MethodType.methodType(Map.class, Map.class, Endpoint.class)
          );
    } catch (Exception e) {
      exception = e;
    }
    if (method != null) {
      INTER_BROKER_CLIENT_CONFIGS_METHOD = method;
      INTER_BROKER_CLIENT_CONFIGS_EXCEPTION = null;
    } else {
      INTER_BROKER_CLIENT_CONFIGS_METHOD = null;
      INTER_BROKER_CLIENT_CONFIGS_EXCEPTION = exception;
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> interBrokerClientConfigs(Map<String, Object> config) {
    try {
      if (INTER_BROKER_CLIENT_CONFIGS_METHOD == null) {
        throw INTER_BROKER_CLIENT_CONFIGS_EXCEPTION;
      }
      return (Map<String, Object>) INTER_BROKER_CLIENT_CONFIGS_METHOD
          .invokeExact(config, getInterBrokerEndpoint(config));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<String, Object> deriveLocalProducerConfigs(Map<String, Object> originals) {
    try {
      Map<String, Object> clientConfigs = interBrokerClientConfigs(originals);
      Set<String> producerConfigs = ProducerConfig.configNames();
      Set<String> adminConfigs = AdminClientConfig.configNames();
      return clientConfigs
          .entrySet().stream()
          // filter non-producer configs
          .filter(e -> producerConfigs.contains(e.getKey()) || adminConfigs.contains(e.getKey()))
          // don't start a metrics reporter inside the local producer
          .filter(e -> !e.getKey().equals(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG))
          // don't use default client-id
          .filter(e -> !e.getKey().equals(CommonClientConfigs.CLIENT_ID_CONFIG))
          // remove broker compression type (since it does not translate)
          .filter(e -> !e.getKey().equals(KafkaConfig.CompressionTypeProp()))
          // we don't need to pass through null values
          .filter(e -> e.getValue() != null)
          .collect(
              Collectors.toMap(
                  e -> KafkaExporterConfig.PREFIX_PRODUCER + e.getKey(),
                  Map.Entry::getValue
              )
          );
    } catch (Exception e) {
      log.error("Exception invoking ConfluentConfigs.interBrokerClientConfigs", e);
      return ImmutableMap.of();
    }
  }

  public static Optional<String> getBrokerRack(Map<String, Object> configs) {
    return Optional.ofNullable((String) configs.get(RACK_PROP));
  }
}
