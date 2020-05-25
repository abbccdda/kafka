// (Copyright) [2018 - 2019] Confluent, Inc.

package org.apache.kafka.server.authorizer.internals;

import io.confluent.http.server.KafkaHttpServerBinder;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.server.audit.AuditLogProvider;
import org.apache.kafka.server.audit.NoOpAuditLogProvider;
import org.apache.kafka.server.http.MetadataServer;
import org.apache.kafka.server.http.MetadataServerFactory;

/**
 * Runtime broker configuration metadata provided to authorizers during start up.
 */
@InterfaceStability.Evolving
public interface ConfluentAuthorizerServerInfo extends AuthorizerServerInfo {
    /**
     * Returns the embedded {@link MetadataServer} installed in this broker.
     */
    default MetadataServer metadataServer() {
        return MetadataServerFactory.none();
    }

    default KafkaHttpServerBinder httpServerBinder() {
        return new KafkaHttpServerBinder();
    }

    /**
     * Returns the {@link org.apache.kafka.server.audit.AuditLogProvider} configured in this broker.
     */
    default AuditLogProvider auditLogProvider() {
        return NoOpAuditLogProvider.INSTANCE;
    }

    /**
     * Returns the instance of {@link Metrics}.
     */
    Metrics metrics();
}
