// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.audit;

import org.apache.kafka.common.config.ConfigException;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

public class NoOpAuditLogProvider implements AuditLogProvider {

    public static final NoOpAuditLogProvider INSTANCE = new NoOpAuditLogProvider();

    @Override
    public void logEvent(final AuditEvent auditEvent) {
    }

    @Override
    public boolean providerConfigured(final Map<String, ?> configs) {
        return true;
    }

    @Override
    public void setSanitizer(final UnaryOperator<AuditEvent> sanitizer) {
    }

    @Override
    public boolean usesMetadataFromThisKafkaCluster() {
        return false;
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return Collections.emptySet();
    }

    @Override
    public void validateReconfiguration(final Map<String, ?> configs) throws ConfigException {
    }

    @Override
    public void reconfigure(final Map<String, ?> configs) {
    }

    @Override
    public void configure(final Map<String, ?> configs) {
    }
}
