package io.confluent.telemetry;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import kafka.server.KafkaConfig;
import kafka.server.Defaults;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

  protected static final String BROKER_ID_PROP = KafkaConfig.BrokerIdProp();

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

  private static String getHostName(AbstractConfig config) {
    return getStringOrDefault(config, HOST_NAME_PROP, HOST_NAME_DEFAULT);
  }

  private static String getPort(AbstractConfig config) {
    return getStringOrDefault(config, PORT_PROP, Integer.toString(PORT_DEFAULT));
  }

  private static String getListeners(AbstractConfig config) {
    return getStringOrDefault(config, LISTENERS_PROP,
            "PLAINTEXT://" + getHostName(config) + ":" + getPort(config));
  }

  private static String getInterBrokerSecurityProtocol(AbstractConfig config) {
    return getStringOrDefault(config, INTER_BROKER_SECURITY_PROTOCOL_PROP, INTER_BROKER_SECURITY_PROTOCOL_DEFAULT);
  }

  private static String getListenerSecurityProtocolMap(AbstractConfig config) {
    return getStringOrDefault(config, LISTENER_SECURITY_PROTOCOL_MAP_PROP, LISTENER_SECURITY_PROTOCOL_MAP_DEFAULT);
  }

  private static String getInterBrokerListenerName(AbstractConfig config) {
    return getStringOrDefault(config, INTER_BROKER_LISTENER_NAME_PROP, INTER_BROKER_LISTENER_NAME_DEFAULT);
  }

  private static String getStringOrDefault(AbstractConfig config, String prop, String defaultValue) {
    Object val = config.originals().get(prop);
    if (val == null) {
      return defaultValue;
    } else {
      return val.toString();
    }
  }

  private static Map<ListenerName, SecurityProtocol> listenerSecurityProtocolMap(AbstractConfig config) {
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

  private static Map.Entry<ListenerName, SecurityProtocol> deriveInterBrokerListener(AbstractConfig config, Map<ListenerName, SecurityProtocol> listenerSecurityProtocolMap) {
    Map<String, Object> originals = config.originals();
    String listenerNameStr = getInterBrokerListenerName(config);
    if (!Strings.isNullOrEmpty(listenerNameStr) && originals.containsKey(INTER_BROKER_SECURITY_PROTOCOL_PROP)) {
      // this should get caught by the broker itself
      throw new ConfigException("cannot provide both " + INTER_BROKER_LISTENER_NAME_PROP + " and " + INTER_BROKER_SECURITY_PROTOCOL_PROP);
    } else if (!Strings.isNullOrEmpty(listenerNameStr)) {
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

  private static List<Endpoint> getEndpoints(AbstractConfig config, Map<ListenerName, SecurityProtocol> listenerSecurityProtocolMap) {
    String listeners = getListeners(config);
    List<String> listenersList =
          Arrays.stream(listeners.split("\\s*,\\s*"))
              .filter(v -> !v.equals(""))
              .collect(Collectors.toList());
    return listenersList.stream()
        .map(l -> createEndPoint(l, listenerSecurityProtocolMap))
        .collect(Collectors.toList());
  }

  public static Endpoint getInterBrokerEndpoint(AbstractConfig config) {
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

  public static boolean isBrokerConfig(AbstractConfig config) {
    return config.originals().containsKey(BROKER_ID_PROP);
  }
}
