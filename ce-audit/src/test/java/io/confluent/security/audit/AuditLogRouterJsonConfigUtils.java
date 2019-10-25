/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.security.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.security.audit.router.AuditLogCategoryResultRouter;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig.DefaultTopics;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig.DestinationTopic;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig.Destinations;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;

public class AuditLogRouterJsonConfigUtils {

  public static String defaultConfig(String bootstrapServers,
      String defaultTopicAllowed, String defaultTopicDenied) {
    AuditLogRouterJsonConfig config = new AuditLogRouterJsonConfig();
    if (bootstrapServers != null) {
      config.destinations = new Destinations(
          Arrays.asList(bootstrapServers.split(",")));
    } else {
      config.destinations = new Destinations(null);
    }
    config.destinations.putTopic(defaultTopicAllowed, new DestinationTopic(
        AuditLogRouterJsonConfig.DEFAULT_RETENTION_MS));
    config.destinations.putTopic(defaultTopicDenied, new DestinationTopic(
        AuditLogRouterJsonConfig.DEFAULT_RETENTION_MS));

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
      String defaultTopicAllowed, String defaultTopicDenied,
      List<KafkaPrincipal> excludedPrincipals) {
    AuditLogRouterJsonConfig config = new AuditLogRouterJsonConfig();
    if (bootstrapServers != null) {
      config.destinations = new Destinations(
          Arrays.asList(bootstrapServers.split(",")));
    } else {
      config.destinations = new Destinations(null);
    }
    config.destinations.putTopic(defaultTopicAllowed, new DestinationTopic(
        AuditLogRouterJsonConfig.DEFAULT_RETENTION_MS));
    config.destinations.putTopic(defaultTopicDenied, new DestinationTopic(
        AuditLogRouterJsonConfig.DEFAULT_RETENTION_MS));

    config.excludedPrincipals = excludedPrincipals.stream()
        .map(KafkaPrincipal::toString).collect(Collectors.toList());

    config.defaultTopics = new DefaultTopics(defaultTopicAllowed, defaultTopicDenied);

    config.routes.put("crn://" + crnAuthority + "/kafka=*",
        Utils.mkMap(Utils.mkEntry(AuditLogCategoryResultRouter.INTERBROKER_CATEGORY,
            Utils.mkMap(Utils.mkEntry("allowed", defaultTopicAllowed),
                Utils.mkEntry("denied", defaultTopicDenied)))));
    config.routes.put("crn://" + crnAuthority + "/kafka=*/topic=*",
        Utils.mkMap(Utils.mkEntry(AuditLogCategoryResultRouter.PRODUCE_CATEGORY,
            Utils.mkMap(Utils.mkEntry("allowed", defaultTopicAllowed),
                Utils.mkEntry("denied", defaultTopicDenied))),
            Utils.mkEntry(AuditLogCategoryResultRouter.CONSUME_CATEGORY,
                Utils.mkMap(Utils.mkEntry("allowed", defaultTopicAllowed),
                    Utils.mkEntry("denied", defaultTopicDenied))),
            Utils.mkEntry(AuditLogCategoryResultRouter.DESCRIBE_CATEGORY,
                Utils.mkMap(Utils.mkEntry("allowed", ""),
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
    return defaultConfig(bootstrapServers, AuditLogRouterJsonConfig.DEFAULT_TOPIC,
        AuditLogRouterJsonConfig.DEFAULT_TOPIC);
  }
}
