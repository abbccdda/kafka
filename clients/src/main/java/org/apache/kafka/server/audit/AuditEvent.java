// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.audit;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * The {@code AuditEvent} interface provides a mechanism for passing audit information to audit log providers.
 * This is the base interface that is extended by various components for specific audit event types.
 */
@InterfaceStability.Evolving
public interface AuditEvent {

    /**
     * Returns unique Id for this  evennt
     * @return the UUID
     */
    UUID uuid();

    /**
     * Returns the date/time that the event was logged.
     * @return the timestamp
     */
    Instant timestamp();

    /**
     * Returns the type of event.
     * @return the event type
     */
    AuditEventType type();

    /**
     * Returns the event status of event.
     * @return the event status
     */
    AuditEventStatus status();

    /**
     * Returns the event data.
     * @return the event data
     */
    Map<String, Object> data();
}
