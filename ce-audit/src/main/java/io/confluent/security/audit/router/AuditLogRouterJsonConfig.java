package io.confluent.security.audit.router;

import static java.util.stream.Collectors.joining;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.authorizer.AuthorizeResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.common.utils.Utils;

public class AuditLogRouterJsonConfig {

  public static final Map<String, AuthorizeResult> ALLOWED_RESULTS =
      Utils.mkMap(Utils.mkEntry("allowed", AuthorizeResult.ALLOWED),
          Utils.mkEntry("denied", AuthorizeResult.DENIED));

  public class Metadata {

    public int configVersion;
    public String lastUpdated;
  }

  @JsonProperty("default_topics")
  public DefaultTopicRouter defaultTopics;
  @JsonProperty("excluded_principals")
  public List<String> excludedPrincipals = new ArrayList<>();
  // CRN -> category -> result -> topic
  public Map<String, Map<String, Map<String, String>>> routes = new HashMap<>();
  public Metadata metadata;

  public static void validate(AuditLogRouterJsonConfig config) {
    if (config.defaultTopics == null) {
      throw new IllegalArgumentException("Default topics must be provided");
    }
    config.defaultTopics.validate();
    for (Map<String, Map<String, String>> categoryMap : config.routes.values()) {
      for (Entry<String, Map<String, String>> categoryEntry : categoryMap.entrySet()) {
        String category = categoryEntry.getKey();
        if (!AuditLogCategoryResultRouter.CATEGORIES.contains(category)) {
          throw new IllegalArgumentException(String.format("Unknown category: %s", category));
        }
        for (Entry<String, String> resultEntry : categoryEntry.getValue().entrySet()) {
          String resultName = resultEntry.getKey();
          if (!ALLOWED_RESULTS.containsKey(resultName)) {
            throw new IllegalArgumentException(
                String.format("Result %s not one of the allowed results: %s", resultName,
                    ALLOWED_RESULTS.keySet().stream().collect(joining(","))));
          }
          String topic = resultEntry.getValue();
          if (topic == null || topic.isEmpty()) {
            continue;
          }
          if (!topic.startsWith(EventLogConfig.EVENT_TOPIC_PREFIX)) {
            throw new IllegalArgumentException(
                String.format("Topic name \"%s\" must begin with \"%s\"", topic,
                    EventLogConfig.EVENT_TOPIC_PREFIX));
          }
        }
      }
    }
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
    AuditLogRouterJsonConfig config = mapper.readValue(json, AuditLogRouterJsonConfig.class);
    validate(config);
    return config;
  }


}
