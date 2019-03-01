// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import io.confluent.security.rbac.Scope;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

public class MetadataServiceConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;
  private static final String METADATA_SERVER_PREFIX = "confluent.metadata.server.";

  public static final String SCOPE_PROP = "confluent.metadata.server.scope";
  private static final String SCOPE_DEFAULT = "";
  private static final String SCOPE_DOC = "The root scope of the metadata server."
      + " By default, metadata from all scopes will be processed by this server";

  public static final String METADATA_SERVER_CLASS_PROP = "confluent.metadata.server.class";
  private static final String METADATA_SERVER_CLASS_DOC = "The fully qualified name of a class that implements"
      + " the " + MetadataServer.class + " interface. Additional configs for the metadata service may be"
      + " provided with the prefix '" + METADATA_SERVER_PREFIX + "'.";

  public static final String METADATA_SERVER_LISTENERS_PROP = "confluent.metadata.server.listeners";
  private static final String METADATA_SERVER_LISTENERS_DOC = "Comma-separated list of listener URLs"
      + " for metadata server to listener on if this broker hosts an embedded metadata server plugin."
      + " Specify hostname as 0.0.0.0 to bind to all interfaces. Examples of valid listeners are "
      + " are https://0.0.0.0:8090,http://127.0.0.1:8091.";

  public static final String METADATA_SERVER_ADVERTISED_LISTENERS_PROP = "confluent.metadata.server.advertised.listeners";
  private static final String METADATA_SERVER_ADVERTISED_LISTENERS_DEFAULT = null;
  private static final String METADATA_SERVER_ADVERTISED_LISTENERS_DOC = "Comma-separated list of advertised listener URLs"
      + " of metadata server if this broker hosts an embedded metadata server plugin. Metadata server URLs"
      + " must be unique across the cluster since they are used as node ids for master writer election."
      + " The URLs are also used for redirection of update requests to the master writer. If not specified,"
      + " 'confluent.metadata.server.listeners' config will be used. 0.0.0.0 may not be used as the host name"
      + " in advertised listeners.";

  static {
    CONFIG = new ConfigDef()
        .define(METADATA_SERVER_CLASS_PROP, Type.CLASS,
            Importance.HIGH, METADATA_SERVER_CLASS_DOC)
        .define(METADATA_SERVER_LISTENERS_PROP, Type.LIST,
            Importance.HIGH, METADATA_SERVER_LISTENERS_DOC)
        .define(METADATA_SERVER_ADVERTISED_LISTENERS_PROP, Type.LIST, METADATA_SERVER_ADVERTISED_LISTENERS_DEFAULT,
            Importance.HIGH, METADATA_SERVER_ADVERTISED_LISTENERS_DOC)
        .define(SCOPE_PROP, Type.STRING, SCOPE_DEFAULT,
            Importance.MEDIUM, SCOPE_DOC);
  }

  public final Scope scope;
  public final Collection<URL> metadataServerUrls;
  private final Class<MetadataServer> metadataServerClass;

  @SuppressWarnings("unchecked")
  public MetadataServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
    try {
      scope = new Scope(getString(SCOPE_PROP));
    } catch (Exception e) {
      throw new ConfigException("Invalid scope for metadata server", e);
    }

    metadataServerClass = (Class<MetadataServer>) getClass(METADATA_SERVER_CLASS_PROP);
    if (metadataServerClass == null)
      throw new ConfigException("Metadata server class not provided");

    Map<String, URL> listeners = urls(getList(METADATA_SERVER_LISTENERS_PROP));
    Map<String, URL> advertisedListeners;
    if (get(METADATA_SERVER_ADVERTISED_LISTENERS_PROP) != null)
      advertisedListeners = urls(getList(METADATA_SERVER_ADVERTISED_LISTENERS_PROP));
    else
      advertisedListeners = listeners;

    if (advertisedListeners.isEmpty())
      throw new ConfigException("Advertised listeners not specified");
    else if (!advertisedListeners.keySet().equals(listeners.keySet()))
      throw new ConfigException("Advertised listener protocols don't match listeners");
    else {
      metadataServerUrls = advertisedListeners.values();
    }
  }

  public MetadataServer metadataServer() {
    return getConfiguredInstances(Collections.singletonList(metadataServerClass.getName()),
        MetadataServer.class, metadataServerConfigs()).get(0);
  }

  public Map<String, Object> metadataServerConfigs() {
    return originalsWithPrefix(METADATA_SERVER_PREFIX);
  }

  @Override
  public String toString() {
    return Utils.mkString(values(), "", "", "=", "%n\t");
  }

  public static void main(String[] args) throws Exception {
    try (PrintStream out = args.length == 0 ? System.out
        : new PrintStream(new FileOutputStream(args[0]), false, StandardCharsets.UTF_8.name())) {
      out.println(CONFIG.toHtmlTable());
      if (out != System.out) {
        out.close();
      }
    }
  }

  private Map<String, URL> urls(List<String> listeners) {
    Map<String, URL> urls = new HashMap<>();
    for (String listener : listeners) {
      try {
        URL url = new URL(listener);
        urls.put(url.getProtocol(), url);
      } catch (MalformedURLException e) {
        throw new ConfigException("Invalid listener URL " + listener, e);
      }
    }
    if (urls.size() != listeners.size())
      throw new ConfigException("Multiple listeners specified for the same protocol: " + listeners);
    return urls;
  }
}
