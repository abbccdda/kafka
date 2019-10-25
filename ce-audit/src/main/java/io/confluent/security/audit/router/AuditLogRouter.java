package io.confluent.security.audit.router;

import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.crn.CachedCrnStringPatternMatcher;
import io.confluent.crn.CrnSyntaxException;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.authorizer.AuthorizeResult;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditLogRouter implements Router {

  private Logger log = LoggerFactory.getLogger(AuditLogRouter.class);
  public static final String SUPPRESSED = "";

  private AuditLogCategoryResultRouter defaultTopicRouter;
  private Set<KafkaPrincipal> excludedPrincipals;
  private CachedCrnStringPatternMatcher<AuditLogCategoryResultRouter> crnRouters;

  private static final Set<String> DEFAULT_SUPPRESSED_CATEGORIES = Utils.mkSet(
      AuditLogCategoryResultRouter.PRODUCE_CATEGORY,
      AuditLogCategoryResultRouter.CONSUME_CATEGORY,
      AuditLogCategoryResultRouter.INTERBROKER_CATEGORY,
      AuditLogCategoryResultRouter.DESCRIBE_CATEGORY,
      AuditLogCategoryResultRouter.HEARTBEAT_CATEGORY);

  private void setDefaultTopicRouter(AuditLogRouterJsonConfig config) {
    defaultTopicRouter = new AuditLogCategoryResultRouter();
    for (String category : AuditLogCategoryResultRouter.CATEGORIES) {
      if (DEFAULT_SUPPRESSED_CATEGORIES.contains(category)) {
        defaultTopicRouter
            .setRoute(category, AuthorizeResult.ALLOWED, "")
            .setRoute(category, AuthorizeResult.DENIED, "");
      } else {
        defaultTopicRouter
            .setRoute(category, AuthorizeResult.ALLOWED, config.defaultTopics.allowed)
            .setRoute(category, AuthorizeResult.DENIED, config.defaultTopics.denied);
      }
    }
  }

  public AuditLogRouter(AuditLogRouterJsonConfig config, int cacheEntries) {
    try {
      setDefaultTopicRouter(config);
      excludedPrincipals = config.excludedPrincipals.stream()
          .map(SecurityUtils::parseKafkaPrincipal)
          .collect(Collectors.toSet());
      crnRouters = new CachedCrnStringPatternMatcher<>(cacheEntries);
      for (String crnString : config.routes.keySet()) {
        AuditLogCategoryResultRouter router = new AuditLogCategoryResultRouter();
        for (Entry<String, Map<String, String>> categoryResultTopic :
            config.routes.get(crnString).entrySet()) {
          for (Entry<String, String> resultTopic : categoryResultTopic.getValue().entrySet()) {
            String category = categoryResultTopic.getKey();
            AuthorizeResult authorizeResult = AuditLogRouterJsonConfig.result(resultTopic.getKey());
            String topic = resultTopic.getValue();

            router.setRoute(category, authorizeResult, topic);
          }
        }
        crnRouters.setPattern(crnString, router);
      }
    } catch (CrnSyntaxException e) {
      throw new ConfigException("Invalid CRN in config", e);
    }
  }

  @Override
  public Optional<String> topic(CloudEvent<AttributesImpl, AuditLogEntry> event) {
    if (!event.getData().isPresent()) {
      log.warn("Tried to route invalid event. Data is missing {}", event);
      return Optional.empty();
    }

    AuditLogEntry auditLogEntry = event.getData().get();
    KafkaPrincipal eventPrincipal = SecurityUtils.parseKafkaPrincipal(
        auditLogEntry.getAuthenticationInfo().getPrincipal());

    if (excludedPrincipals.contains(eventPrincipal)) {
      return Optional.of(SUPPRESSED);  // suppress this message
    }

    if (!event.getAttributes().getSubject().isPresent()) {
      log.warn("Tried to route invalid event. No subject found. {}", event);
      return Optional.empty();
    }
    AuditLogCategoryResultRouter router = crnRouters
        .match(event.getAttributes().getSubject().get());
    if (router != null) {
      Optional<String> routedTopic = router.topic(event);
      if (routedTopic.isPresent()) {
        return routedTopic;
      }
    }

    return defaultTopicRouter.topic(event);
  }

  @Override
  public String toString() {
    return "AuditLogRouter(default=" + defaultTopicRouter + ",routes=" + crnRouters + ")";
  }
}
