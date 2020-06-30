package io.confluent.kafka.multitenant;

import io.confluent.kafka.multitenant.quota.TenantPartitionAssignor;
import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;
import java.util.Map;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;

public class MultiTenantInterceptorConfig {

    private final TenantPartitionAssignor partitionAssignor;
    private final short defaultReplicationFactor;
    private final int defaultNumPartitions;
    private final boolean clusterPrefixForHostnameEnabled;

    public MultiTenantInterceptorConfig(Map<String, ?> configs) {
        this.partitionAssignor = TenantQuotaCallback.partitionAssignor(configs);
        this.defaultReplicationFactor = (short) intConfig(configs, KafkaConfig$.MODULE$.DefaultReplicationFactorProp());
        this.defaultNumPartitions = intConfig(configs, KafkaConfig$.MODULE$.NumPartitionsProp());
        this.clusterPrefixForHostnameEnabled = boolConfig(configs, ConfluentConfigs.MULTITENANT_LISTENER_PREFIX_ENABLE);
    }

    public TenantPartitionAssignor partitionAssignor() {
        return partitionAssignor;
    }

    public short defaultReplicationFactor() {
        return defaultReplicationFactor;
    }

    public int defaultNumPartitions() {
        return defaultNumPartitions;
    }

    public boolean isClusterPrefixForHostnameEnabled() {
        return clusterPrefixForHostnameEnabled;
    }

    public static int intConfig(Map<String, ?> configs, String configName) {
        Object configValue = configs.get(configName);
        if (configValue == null) {
            throw new ConfigException(configName + " is not set");
        }
        return Integer.parseInt(configValue.toString());
    }

    private static boolean boolConfig(Map<String, ?> configs, String configName) {
        Object configValue = configs.get(configName);
        if (configValue == null) {
            throw new ConfigException(configName + " is not set");
        }
        return Boolean.parseBoolean(configValue.toString());
    }
}
