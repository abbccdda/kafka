// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.utils.JsonMapper;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class NodeMetadata implements Comparable<NodeMetadata> {

  private static final List<String> PROTOCOLS = Arrays.asList("https", "http");
  private final Map<String, URL> urls;

  @JsonCreator
  public NodeMetadata(@JsonProperty("urls") Collection<URL> urls) {
    if (urls == null || urls.isEmpty())
      throw new IllegalArgumentException("Node urls not specified");
    this.urls = urls.stream().collect(Collectors.toMap(URL::getProtocol, Function.identity()));
  }

  @JsonProperty
  public Collection<URL> urls() {
    return urls.values();
  }

  public URL url(String protocol) {
    return urls.get(protocol);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NodeMetadata)) {
      return false;
    }

    NodeMetadata that = (NodeMetadata) o;

    return Objects.equals(urls, that.urls);
  }

  @Override
  public int hashCode() {
    return Objects.hash(urls);
  }

  @Override
  public int compareTo(NodeMetadata o) {
    // Use a constant ordering based on currently supported protocols. Don't reject
    // unknown protocols in case we need to support new ones in future using rolling upgrade.
    for (String protocol : PROTOCOLS) {
      int result = compareUrls(this.urls.get(protocol), o.urls.get(protocol));
      if (result != 0)
        return result;
    }
    Set<String> unknownProtocols = urls.keySet().stream()
        .filter(protocol -> !PROTOCOLS.contains(protocol))
        .collect(Collectors.toSet());
    for (String protocol : unknownProtocols) {
      int result = compareUrls(this.urls.get(protocol), o.urls.get(protocol));
      if (result != 0)
        return result;
    }

    return urls.keySet().equals(o.urls.keySet()) ? 0 : 1;
  }

  private int compareUrls(URL url, URL otherUrl) {
    int result = 0;
    if (otherUrl == null) {
      if (url != null)
        result = -1;
      else
        result = 0;
    } else if (url == null)
      result = 1;
    else
      result = url.toString().compareTo(otherUrl.toString());
    return result;
  }

  @Override
  public String toString() {
    return "NodeMetadata(" +
        "urls=" + urls +
        ')';
  }

  public ByteBuffer serialize() {
    return JsonMapper.toByteBuffer(this);
  }

  public static NodeMetadata deserialize(ByteBuffer buffer) {
    return JsonMapper.fromByteBuffer(buffer, NodeMetadata.class);
  }
}
