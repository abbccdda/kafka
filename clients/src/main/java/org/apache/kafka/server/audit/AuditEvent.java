// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.audit;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public interface AuditEvent {

    UUID uuid();

    Instant timestamp();

    AuditEventType type();

    AuditEventStatus status();

    Map<String, Object> data();
}
