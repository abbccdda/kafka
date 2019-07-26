/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.client;

import kafka.tier.topic.TierTopicManagerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class TierTopicAdminClientSupplier implements Supplier<AdminClient> {
    private static final String SEPARATOR = "-";
    private static final String CLIENT_ID_PREFIX = "__TierAdminClient";

    private final TierTopicManagerConfig config;
    private final AtomicInteger instanceId = new AtomicInteger(0);

    public TierTopicAdminClientSupplier(TierTopicManagerConfig config) {
        this.config = config;
    }

    @Override
    public AdminClient get() {
        String clientId = clientId(config.clusterId, config.brokerId, instanceId.getAndIncrement());
        return AdminClient.create(properties(config, clientId));
    }

    private static Properties properties(TierTopicManagerConfig config, String clientId) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServersSupplier.get());
        return properties;
    }

    private static String clientId(String clusterId, int brokerId, long instanceId) {
        return CLIENT_ID_PREFIX + SEPARATOR +
                clusterId + SEPARATOR +
                brokerId + SEPARATOR +
                instanceId;
    }
}
