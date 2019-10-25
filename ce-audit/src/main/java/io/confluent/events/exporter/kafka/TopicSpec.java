/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.events.exporter.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TopicSpec {

  @JsonProperty("name")
  private String name;

  @JsonProperty("partitions")
  private int partitions;

  @JsonProperty("replicationFactor")
  private int replicationFactor;

  @JsonProperty("config")
  private Map<String, String> config = new HashMap<>();

  public TopicSpec() {
  }

  public TopicSpec(String name, int partitions, int replicationFactor, Map<String, String> config) {
    this.name = name;
    this.partitions = partitions;
    this.replicationFactor = replicationFactor;
    this.config = config;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String name() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int partitions() {
    return partitions;
  }

  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }

  public int replicationFactor() {
    return replicationFactor;
  }

  public void setReplicationFactor(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public Map<String, String> config() {
    return config;
  }

  public void setConfig(Map<String, String> config) {
    this.config = config;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TopicSpec topicSpec = (TopicSpec) o;
    return partitions == topicSpec.partitions
        && replicationFactor == topicSpec.replicationFactor
        && Objects.equals(name, topicSpec.name)
        && Objects.equals(config, topicSpec.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, partitions, replicationFactor, config);
  }

  @Override
  public String toString() {
    return "TopicSpec={"
        + "name='" + name() + '\''
        + ", partitions='" + partitions() + '\''
        + ", replicationFactor=" + replicationFactor()
        + ", config=" + config()
        + '}';
  }

  public static class Topics {

    public List<TopicSpec> topics;

    public Topics() {
    }

    public List<TopicSpec> topics() {
      return topics;
    }

    public void setTopics(List<TopicSpec> topics) {
      this.topics = topics;
    }

    @Override
    public String toString() {
      return "Topics{"
          + "topics=" + topics
          + '}';
    }
  }

  public static final class Builder {

    public String name;
    public int partitions = 0;
    public int replicationFactor = 0;
    public Map<String, String> config = new HashMap<>();

    private Builder() {
    }


    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setPartitions(int partitions) {
      this.partitions = partitions;
      return this;
    }

    public Builder setReplicationFactor(int replicationFactor) {
      this.replicationFactor = replicationFactor;
      return this;
    }

    public Builder setConfig(Map<String, String> config) {
      this.config = config;
      return this;
    }

    public Builder setTopicConfig(String key, String value) {
      this.config.put(key, value);
      return this;
    }

    public TopicSpec build() {
      TopicSpec topicSpec = new TopicSpec();
      Objects.requireNonNull(name, "topic name is required");
      topicSpec.setName(name);
      topicSpec.setConfig(ImmutableMap.copyOf(config));
      return topicSpec;
    }
  }
}
