/*
 Copyright 2020 Confluent Inc.
 */

package kafka.tier.tools;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TierRecoveryConfig {
    public static final String VALIDATE = "confluent.tier.recovery.validate";
    public static final String MATERIALIZE = "confluent.tier.recovery.materialize";
    public static final String WORKING_DIR = "confluent.tier.recovery.working.dir";
    public static final String DUMP_EVENTS = "confluent.tier.recovery.dump.events";
    public static final String BROKER_WORKDIR_LIST = "confluent.tier.recovery.broker.workdir.list";

    static Properties toMaterializerProperties(Properties props) {
        Properties newProperties = new Properties();
        final Map<Object, Object> mapping = new HashMap<>();
        mapping.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_CONFIG);
        mapping.put(TierRecoveryConfig.VALIDATE, TierTopicMaterializationToolConfig.TIER_STORAGE_VALIDATION);
        mapping.put(TierRecoveryConfig.MATERIALIZE, TierTopicMaterializationToolConfig.MATERIALIZE);
        mapping.put(TierRecoveryConfig.WORKING_DIR, TierTopicMaterializationToolConfig.WORKING_DIR);
        mapping.put(TierRecoveryConfig.DUMP_EVENTS, TierTopicMaterializationToolConfig.DUMP_EVENTS);
        for (Map.Entry<Object, Object> prop: props.entrySet()) {
            Object newKey = mapping.get(prop.getKey());
            if (newKey != null) {
                newProperties.put(newKey, prop.getValue());
            } else {
                newProperties.put(prop.getKey(), prop.getValue());
            }
        }
        return newProperties;
    }
}
