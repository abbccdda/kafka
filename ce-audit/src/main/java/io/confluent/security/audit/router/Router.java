package io.confluent.security.audit.router;

import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.security.audit.AuditLogEntry;
import java.util.Optional;

public interface Router {

  /**
   * Possibly return the name of a topic that this event should be routed to.
   * <p>
   * If the value is present, but "", the Router wants this message to be discarded.
   * <p>
   * If the value is an empty Optional, the Router has no opinion about what the routing should be.
   * In that case, the caller should consult a different Router or fall back to a default. This is
   * intended to allow a prioritized list of Routers to be tried, with the first one with a concrete
   * answer determining the routing.
   */
  Optional<String> topic(CloudEvent<AttributesImpl, AuditLogEntry> entry);
}
