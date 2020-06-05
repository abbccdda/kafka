/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package org.apache.kafka.connect.integration;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfluentUnlicensedMonitorableSourceConnector extends MonitorableSourceConnector {

    private static final Logger log = LoggerFactory.getLogger(ConfluentUnlicensedMonitorableSourceConnector.class);

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("bar", ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "");

    private String connectorName;

    @Override
    public void start(Map<String, String> props) {
        super.start(props);
        // Validate all of the configuration
        new SimpleConfig(CONFIG_DEF, props);
        connectorName = props.get("name");
        if (props.keySet().stream().anyMatch(k -> k.startsWith("confluent."))) {
            throw new ConfigException("Did not expect license properties");
        }
    }

    @Override
    public ConfigDef config() {
        log.info("Configured {} connector {}", this.getClass().getSimpleName(), connectorName);
        return CONFIG_DEF;
    }
}
