package io.confluent.security.audit.router;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.CloudEvent;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTopicRouter implements EventTopicRouter {

  private static final Logger log = LoggerFactory.getLogger(DefaultTopicRouter.class);

  private final String allowedTopic;
  private final String deniedTopic;

  @JsonCreator
  public DefaultTopicRouter(@JsonProperty("allowed") String allowedTopic,
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

  public void validate() {
    if (this.allowedTopic == null || this.deniedTopic == null) {
      throw new IllegalArgumentException("Both allowed topic and denied topic must be set");
    }
  }
}
