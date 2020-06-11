// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.audit;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 *  The {@code AuditEventType} enum provides audit event types
 */
@InterfaceStability.Evolving
public enum AuditEventType {
    AUTHENTICATION,
    AUTHORIZATION
}
