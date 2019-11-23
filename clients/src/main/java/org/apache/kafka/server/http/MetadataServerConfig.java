// (Copyright) [2018 - 2019] Confluent, Inc.

package org.apache.kafka.server.http;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

public final class MetadataServerConfig extends AbstractConfig {

  private static final String METADATA_SERVER_PREFIX = "confluent.metadata.server.";

  public static final String METADATA_SERVER_NAME_PROP = "confluent.metadata.server.name";
  private static final String METADATA_SERVER_NAME_DEFAULT = MetadataServers.CONFLUENT.name();
  private static final String METADATA_SERVER_NAME_DOC =
      "Embedded metadata server implementation. Metadata servers supported are "
          + MetadataServers.names()
          + ". CONFLUENT metadata server will be installed by default.";

  public static final String METADATA_SERVER_LISTENERS_PROP =
      "confluent.metadata.server.listeners";
  private static final String METADATA_SERVER_LISTENERS_DEFAULT = "http://127.0.0.1:8090";
  private static final String METADATA_SERVER_LISTENERS_DOC =
      "Comma-separated list of listener URLs for metadata server to listener on if this broker "
          + "hosts an embedded metadata server plugin. Specify hostname as 0.0.0.0 to bind to all "
          + "interfaces. Examples of valid listeners are "
          + "https://0.0.0.0:8090,http://127.0.0.1:8091. The default value is "
          + METADATA_SERVER_LISTENERS_DEFAULT
          + ".";

  public static final String METADATA_SERVER_ADVERTISED_LISTENERS_PROP =
      "confluent.metadata.server.advertised.listeners";
  private static final String METADATA_SERVER_ADVERTISED_LISTENERS_DEFAULT = "";
  private static final String METADATA_SERVER_ADVERTISED_LISTENERS_DOC =
      "Comma-separated list of advertised listener URLs of metadata server if this broker hosts an"
          + "embedded metadata server plugin. Metadata server URLs must be unique across the "
          + "cluster since they are used as node ids for master writer election. The URLs are also "
          + "used for redirection of update requests to the master writer. If not specified, "
          + "'confluent.metadata.server.listeners' config will be used. 0.0.0.0 may not be used as "
          + "the host name in advertised listeners.";

  private static final ConfigDef CONFIG =
      new ConfigDef()
          .define(
              METADATA_SERVER_NAME_PROP,
              Type.STRING,
              METADATA_SERVER_NAME_DEFAULT,
              Importance.HIGH,
              METADATA_SERVER_NAME_DOC)
          .define(
              METADATA_SERVER_LISTENERS_PROP,
              Type.LIST,
              METADATA_SERVER_LISTENERS_DEFAULT,
              Importance.HIGH,
              METADATA_SERVER_LISTENERS_DOC)
          .define(
              METADATA_SERVER_ADVERTISED_LISTENERS_PROP,
              Type.LIST,
              METADATA_SERVER_ADVERTISED_LISTENERS_DEFAULT,
              Importance.HIGH,
              METADATA_SERVER_ADVERTISED_LISTENERS_DOC);

  private final String metadataServerName;
  private final List<URL> metadataServerListeners;
  private final List<URL> metadataServerAdvertisedListeners;

  public MetadataServerConfig(Map<?, ?> props) {
    super(CONFIG, props);
    metadataServerName = getString(METADATA_SERVER_NAME_PROP);
    metadataServerListeners = toUrls(getList(METADATA_SERVER_LISTENERS_PROP));
    metadataServerAdvertisedListeners = toUrls(getList(METADATA_SERVER_ADVERTISED_LISTENERS_PROP));

    checkUniqueProtocols(metadataServerListeners);
    checkUniqueProtocols(metadataServerAdvertisedListeners);
    if (!metadataServerAdvertisedListeners.isEmpty()) {
      checkSameProtocols(metadataServerListeners, metadataServerAdvertisedListeners);
    }
  }

  public boolean isMetadataServerEnabled() {
    return !metadataServerName.equals(MetadataServers.NONE.name())
        && !metadataServerListeners().isEmpty();
  }

  public String metadataServerName() {
    return metadataServerName;
  }

  public List<URL> metadataServerListeners() {
    return Collections.unmodifiableList(metadataServerListeners);
  }

  public List<URL> metadataServerAdvertisedListeners() {
    return Collections.unmodifiableList(metadataServerAdvertisedListeners);
  }

  public Map<String, Object> metadataServerConfigs() {
    return Collections.unmodifiableMap(originalsWithPrefix(METADATA_SERVER_PREFIX));
  }

  @Override
  public String toString() {
    return Utils.mkString(values(), "", "", "=", "%n\t");
  }

  private static ArrayList<URL> toUrls(List<String> specs) {
    ArrayList<URL> urls = new ArrayList<>();
    for (String spec : specs) {
      try {
        urls.add(new URL(spec));
      } catch (MalformedURLException e) {
        throw new ConfigException(String.format("Invalid URL: %s", spec), e);
      }
    }
    return urls;
  }

  private static void checkUniqueProtocols(List<URL> urls) {
    if (getProtocols(urls).size() != urls.size()) {
      throw new ConfigException(
          String.format("Multiple URLs specified for the same protocol: %s", urls));
    }
  }

  private static void checkSameProtocols(List<URL> left, List<URL> right) {
    if (!getProtocols(left).equals(getProtocols(right))) {
      throw new ConfigException(
          String.format("URLs protocols don't match: %s, %s", left, right));
    }
  }

  private static Set<String> getProtocols(List<URL> urls) {
    return urls.stream().map(URL::getProtocol).collect(Collectors.toSet());
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
}
