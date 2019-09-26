package io.confluent.security.audit.router;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.CloudEvent;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTopicRouter implements EventTopicRouter {

  private static final Logger log = LoggerFactory.getLogger(DefaultTopicRouter.class);

  @JsonProperty("allowed")
  private final String allowedTopic;
  @JsonProperty("denied")
  private final String deniedTopic;

  @JsonCreator
  public DefaultTopicRouter(
      @JsonProperty("allowed") String allowedTopic,
      @JsonProperty("denied") String deniedTopic) {
    this.allowedTopic = allowedTopic;
    this.deniedTopic = deniedTopic;
  }

  @Override
  public Optional<String> topic(CloudEvent event) {
    try {
      AuditLogEntry auditLogEntry = event.getData().unpack(AuditLogEntry.class);
      if (auditLogEntry.getAuthorizationInfo().getGranted()) {
        return Optional.of(allowedTopic);
      }
      return Optional.of(deniedTopic);
    } catch (InvalidProtocolBufferException e) {
      log.debug("Attempted to route a invalid AuditLogEntry", e);
      return Optional.empty();
    }
  }

  public void validate(Set<String> destinationTopics) {
    if (this.allowedTopic == null || this.deniedTopic == null) {
      throw new IllegalArgumentException("Both allowed topic and denied topic must be set");
    }
    Set<String> missingTopics = Utils.mkSet(this.allowedTopic, this.deniedTopic);
    missingTopics.remove("");  // empty topic is permitted without appearing in destinations
    missingTopics.removeAll(destinationTopics);
    if (!missingTopics.isEmpty()) {
      throw new IllegalArgumentException(
          "Topics are not defined destinations: " + missingTopics.stream().collect(
              Collectors.joining(", ")));
    }
  }
}
