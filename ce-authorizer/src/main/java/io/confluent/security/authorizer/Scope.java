// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Hierarchical scopes for role bindings. This is used to scope roles bindings or other scoped
 * metadata to individual clusters or other levels of scope. It is also used to limit the data
 * cached in embedded authorizers.
 *
 * Concrete scopes consist of a hierarchical scope path and one or more clusters that may be used
 * to uniquely identify any resource within the scope.
 *
 * For example, with a two level scope consisting of root scope "myorg" and clusters
 * "kafka-cluster:clusterA" and "kafka-cluster:clusterB", roles may be assigned at
 * cluster level for "clusterA" and "clusterB". Authorization service providing metadata for
 * all clusters will use the root scope "myorg" to process role bindings of both clusters,
 * while a broker belonging to "kafka-cluster:clusterA" only uses role bindings of clusterA.
 *
 * Cluster ids may not be globally unique. For example, Connect cluster id may be unique only within
 * the context of its kafka cluster. Hence the combination of cluster ids is used to define a unique
 * scope, e.g. {"kafka-cluster" : "kafkaClusterA", "connect-cluster" : "connectCluster1"}.
 */
public class Scope {

  public static final Scope ROOT_SCOPE = new Scope(Collections.emptyList(), Collections.emptyMap());
  private static final String KAFKA_CLUSTER_TYPE = "kafka-cluster";

  private final Scope parent;
  private final List<String> path;
  private final Map<String, String> clusters;

  @JsonCreator
  public Scope(@JsonProperty("path") List<String> path,
               @JsonProperty("clusters") Map<String, String> clusters) {
    this.path = path == null ? Collections.emptyList() : new ArrayList<>(path);
    this.clusters = clusters == null ? Collections.emptyMap() : new HashMap<>(clusters);
    if (!this.clusters.isEmpty())
      this.parent = new Scope(this.path, Collections.emptyMap());
    else if (!this.path.isEmpty())
      this.parent = new Scope(this.path.subList(0, this.path.size() - 1), Collections.emptyMap());
    else
      this.parent = null;
  }

  @JsonProperty
  public List<String> path() {
    return path;
  }

  @JsonProperty
  public Map<String, String>  clusters() {
    return clusters;
  }

  public Scope parent() {
    return parent;
  }

  public void validate(boolean concreteScope) {
    if (concreteScope) {
      if (clusters.isEmpty())
        throw new InvalidScopeException("Expected concrete scope with clusters, but not clusters provided");
    }
    clusters.forEach((k, v) -> {
      if (k == null || k.isEmpty())
        throw new InvalidScopeException("Empty cluster type for cluster id " + v);
      if (v == null || v.isEmpty())
        throw new InvalidScopeException("Empty cluster id for cluster type " + k);
    });
    path.forEach(p -> {
      if (p == null || p.isEmpty())
        throw new InvalidScopeException("Empty scope path entry");
    });
  }

  public boolean containsScope(Scope o) {
    if (o == null)
      return false;
    else if (this.equals(o))
      return true;
    else
      return containsScope(o.parent);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Scope)) {
      return false;
    }

    Scope that = (Scope) o;
    return Objects.equals(path, that.path) && Objects.equals(clusters, that.clusters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, clusters);
  }

  @Override
  public String toString() {
    return "Scope(" +
        "path='" + path + '\'' +
        ", clusters='" + clusters + '\'' +
        ')';
  }

  public static Scope kafkaClusterScope(String kafkaClusterId) {
    return new Builder().withCluster(KAFKA_CLUSTER_TYPE, kafkaClusterId).build();
  }

  public static Scope intermediateScope(String... scopeEntries) {
    return new Builder(Arrays.asList(scopeEntries)).build();
  }

  public static class Builder {
    private final List<String> path;
    private final Map<String, String> clusters;

    public Builder(String... path) {
      this(Arrays.asList(path));
    }

    public Builder(List<String> path) {
      this.path = path;
      this.clusters = new HashMap<>();
    }

    public Builder withKafkaCluster(String clusterId) {
      return withCluster(KAFKA_CLUSTER_TYPE, clusterId);
    }

    public Builder withCluster(String clusterType, String clusterId) {
      if (clusters.putIfAbsent(Objects.requireNonNull(clusterType, "clusterType"), Objects.requireNonNull(clusterId, "clusterId")) != null) {
        throw new IllegalArgumentException("Cluster already present in scope: " + clusterType);
      }
      return this;
    }

    public Scope build() {
      return new Scope(path, clusters);
    }
  }
}
