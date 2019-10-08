package io.confluent.security.audit.router;

import static java.util.stream.Collectors.joining;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.crn.ConfluentResourceName;
import io.confluent.crn.CrnSyntaxException;
import io.confluent.security.authorizer.AuthorizeResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.kafka.common.utils.Utils;

public class AuditLogRouterJsonConfig {

  public static final Map<String, AuthorizeResult> ALLOWED_RESULTS =
      Utils.mkMap(Utils.mkEntry("allowed", AuthorizeResult.ALLOWED),
          Utils.mkEntry("denied", AuthorizeResult.DENIED));

  public static final String DEFAULT_TOPIC = "_confluent-audit-log";
  public static final String TOPIC_PREFIX = "_confluent-audit-log";
  public static final long DEFAULT_RETENTION_MS = 90L * 24 * 60 * 60 * 1000; // 90 days

  public static class DestinationTopic {

    @JsonProperty("retention_ms")
    public long retentionMs;

    public DestinationTopic(@JsonProperty("retention_ms") long retentionMs) {
      this.retentionMs = retentionMs;
    }
  }

  public static class Destinations {

    @JsonProperty("bootstrap_servers")
    public List<String> bootstrapServers;
    public Map<String, DestinationTopic> topics = new HashMap<>();

    public Destinations(@JsonProperty("bootstrap_servers") List<String> bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
    }

    public DestinationTopic putTopic(String key, DestinationTopic value) {
      if (key.isEmpty()) {  // Empty topic means discard this message
        return null;
      }
      return topics.put(key, value);
    }
  }

  public static class DefaultTopics {

    public String allowed;
    public String denied;

    public DefaultTopics(@JsonProperty("allowed") String allowed,
        @JsonProperty("denied") String denied) {
      this.allowed = allowed;
      this.denied = denied;
    }
  }

  public static class Metadata {

    @JsonProperty("resource_version")
    public String resourceVersion;
    @JsonProperty("updated_at")
    public String updatedAt;

    public Metadata(@JsonProperty("resource_version") String resourceVersion,
        @JsonProperty("updated_at") String updatedAt) {
      this.resourceVersion = resourceVersion;
      this.updatedAt = updatedAt;
    }
  }

  @JsonProperty("default_topics")
  public DefaultTopics defaultTopics;
  @JsonProperty("excluded_principals")
  public List<String> excludedPrincipals = new ArrayList<>();
  // CRN -> category -> result -> topic
  public Map<String, Map<String, Map<String, String>>> routes = new HashMap<>();
  public Metadata metadata;
  public Destinations destinations;


  public static void validateDefaultTopics(AuditLogRouterJsonConfig config,
      Set<String> allowedTopics) {
    if (config.defaultTopics == null) {
      throw new IllegalArgumentException("Default topics must be provided");
    }

    if (!allowedTopics.contains(config.defaultTopics.allowed) ||
        !allowedTopics.contains(config.defaultTopics.denied)) {
      throw new IllegalArgumentException(String.format(
          "Default topics %s and %s must appear in destinations",
          config.defaultTopics.allowed, config.defaultTopics.denied));
    }
  }

  public static void validateRoutes(AuditLogRouterJsonConfig config,
      Set<String> allowedTopics) {
    try {
      for (Entry<String, Map<String, Map<String, String>>> routeEntry : config.routes.entrySet()) {
        ConfluentResourceName.fromString(routeEntry.getKey()); // throws if this is invalid
        for (Entry<String, Map<String, String>> categoryResultTopic :
            routeEntry.getValue().entrySet()) {
          String category = categoryResultTopic.getKey();
          if (!AuditLogCategoryResultRouter.CATEGORIES.contains(category)) {
            throw new IllegalArgumentException(String.format("Unknown category: %s", category));
          }
          for (Entry<String, String> resultTopic : categoryResultTopic.getValue().entrySet()) {
            String result = resultTopic.getKey();
            if (!ALLOWED_RESULTS.containsKey(result)) {
              throw new IllegalArgumentException(
                  String.format("Result %s not one of the allowed results: %s", result,
                      String.join(",", ALLOWED_RESULTS.keySet())));
            }
            String topic = resultTopic.getValue();
            if (topic == null || topic.isEmpty()) {
              continue;
            }
            if (!allowedTopics.contains(topic)) {
              throw new IllegalArgumentException(
                  String.format("Topic name \"%s\" must be in destinations.topics", topic));
            }
          }
        }
      }
    } catch (CrnSyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static void validate(AuditLogRouterJsonConfig config) {
    if (config.destinations == null) {
      throw new IllegalArgumentException("Destinations must be provided");
    }
    Set<String> destinationTopicNames = config.destinations.topics.keySet();
    String misnamedDestinationTopicNames = destinationTopicNames.stream()
        .filter(topicName -> !topicName.startsWith(TOPIC_PREFIX))
        .sorted()
        .collect(joining(", "));
    if (!misnamedDestinationTopicNames.isEmpty()) {
      throw new IllegalArgumentException(String.format("Topics must start with %s: %s",
          TOPIC_PREFIX, misnamedDestinationTopicNames));
    }

    Set<String> allowedTopics = new HashSet<>(destinationTopicNames);
    // "" means suppress the message, which doesn't need to be in the destinations
    allowedTopics.add("");

    validateDefaultTopics(config, allowedTopics);
    validateRoutes(config, allowedTopics);
  }

  public static AuthorizeResult result(String resultName) {
    return ALLOWED_RESULTS.get(resultName);
  }

  /**
   * Throws IllegalArgumentException if the config is Invalid
   */
  public static AuditLogRouterJsonConfig load(String json)
      throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
    AuditLogRouterJsonConfig config = mapper.readValue(json, AuditLogRouterJsonConfig.class);
    validate(config);
    return config;
  }

  /*
   * For testing
   */
  public static String defaultConfig(String bootstrapServers,
      String defaultTopicAllowed, String defaultTopicDenied) {
    AuditLogRouterJsonConfig config = new AuditLogRouterJsonConfig();
    config.destinations = new Destinations(
        Arrays.asList(bootstrapServers.split(",")));
    config.destinations.putTopic(defaultTopicAllowed, new DestinationTopic(DEFAULT_RETENTION_MS));
    config.destinations.putTopic(defaultTopicDenied, new DestinationTopic(DEFAULT_RETENTION_MS));

    config.defaultTopics = new DefaultTopics(defaultTopicAllowed, defaultTopicDenied);

    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(config);
    } catch (JsonProcessingException e) {
      // Shouldn't happen because this is always the same
      throw new RuntimeException(e);
    }
  }

  public static String defaultConfigProduceConsumeInterbroker(String bootstrapServers,
      String crnAuthority,
      String defaultTopicAllowed, String defaultTopicDenied) {
    AuditLogRouterJsonConfig config = new AuditLogRouterJsonConfig();
    config.destinations = new Destinations(
        Arrays.asList(bootstrapServers.split(",")));
    config.destinations.putTopic(defaultTopicAllowed, new DestinationTopic(DEFAULT_RETENTION_MS));
    config.destinations.putTopic(defaultTopicDenied, new DestinationTopic(DEFAULT_RETENTION_MS));

    config.defaultTopics = new DefaultTopics(defaultTopicAllowed, defaultTopicDenied);

    config.routes.put("crn://" + crnAuthority + "/kafka=*",
        Utils.mkMap(Utils.mkEntry(AuditLogCategoryResultRouter.INTERBROKER_CATEGORY,
            Utils.mkMap(Utils.mkEntry("allowed", defaultTopicAllowed),
                Utils.mkEntry("denied", defaultTopicDenied)))));
    config.routes.put("crn://" + crnAuthority + "/kafka=*/topic=*",
        Utils.mkMap(Utils.mkEntry(AuditLogCategoryResultRouter.PRODUCE_CATEGORY,
            Utils.mkMap(Utils.mkEntry("allowed", defaultTopicAllowed),
                Utils.mkEntry("denied", defaultTopicDenied)))));
    config.routes.put("crn://" + crnAuthority + "/kafka=*/group=*",
        Utils.mkMap(Utils.mkEntry(AuditLogCategoryResultRouter.CONSUME_CATEGORY,
                Utils.mkMap(Utils.mkEntry("allowed", defaultTopicAllowed),
                    Utils.mkEntry("denied", defaultTopicDenied)))));
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(config);
    } catch (JsonProcessingException e) {
      // Shouldn't happen because this is always the same
      throw new RuntimeException(e);
    }
  }

  public static String defaultConfig(String bootstrapServers) {
    return defaultConfig(bootstrapServers, DEFAULT_TOPIC, DEFAULT_TOPIC);
  }

  public String bootstrapServers() {
    return String.join(",", destinations.bootstrapServers);
  }
}
