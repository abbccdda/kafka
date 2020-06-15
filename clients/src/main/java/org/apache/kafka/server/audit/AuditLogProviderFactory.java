// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.audit;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;

public final class AuditLogProviderFactory {

    protected static final Logger log = LoggerFactory.getLogger(AuditLogProviderFactory.class);

    /**
     * Creates a {@code AuditLogProvider}.
     */
    public static AuditLogProvider create(Map<String, ?> configs, String clusterId) {
        ServiceLoader<AuditLogProvider> providers = ServiceLoader.load(AuditLogProvider.class);
        for (AuditLogProvider provider : providers) {
            if (provider.providerConfigured(configs)) {
                provider.configure(configs);
                if (provider instanceof ClusterResourceListener) {
                    ((ClusterResourceListener) provider).onUpdate(new ClusterResource(clusterId));
                }
                return provider;
            }
        }

        log.warn("Could not find suitable AuditLogProvider implementation.");
        return NoOpAuditLogProvider.INSTANCE;
    }
}
