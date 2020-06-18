// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.audit;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 *  The {@code AuditEventStatus} enum provides audit operation results.
 */
@InterfaceStability.Evolving
public enum AuditEventStatus {
    SUCCESS,
    FAILURE,
    UNAUTHENTICATED,
    UNKNOWN_USER_DENIED
}
