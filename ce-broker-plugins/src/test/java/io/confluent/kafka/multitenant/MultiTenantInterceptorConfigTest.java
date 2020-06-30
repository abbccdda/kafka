package io.confluent.kafka.multitenant;

import java.util.HashMap;
import java.util.Map;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;


public class MultiTenantInterceptorConfigTest {

    @Test
    public void constructorCorrectlyParsedTheConfig() {
        Map<String, Object> configMap  = new HashMap<String, Object>() {{
            put(KafkaConfig.BrokerIdProp(), 1);
            put(KafkaConfig.DefaultReplicationFactorProp(), (short) 1);
            put(KafkaConfig.NumPartitionsProp(), 1);
            put(ConfluentConfigs.MULTITENANT_LISTENER_PREFIX_ENABLE, false);
        }};
        MultiTenantInterceptorConfig config = new MultiTenantInterceptorConfig(configMap);
        assertEquals(1, config.defaultNumPartitions());
        assertEquals(1, config.defaultReplicationFactor());
        assertNull(config.partitionAssignor());
        assertFalse(config.isClusterPrefixForHostnameEnabled());
    }
}
