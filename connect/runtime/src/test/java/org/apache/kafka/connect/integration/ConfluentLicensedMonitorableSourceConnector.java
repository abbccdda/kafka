/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package org.apache.kafka.connect.integration;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfluentLicensedMonitorableSourceConnector extends MonitorableSourceConnector {

    private static final Logger log = LoggerFactory.getLogger(ConfluentLicensedMonitorableSourceConnector.class);

    // Define the license properties as required and having no defaults so validation ensure they are set
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("foo", ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "")
            .define("confluent.license", ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "")
            .define("confluent.topic", ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "")
            .define("confluent.topic.bootstrap.servers", ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "")
            .define("confluent.topic.replication.factor", ConfigDef.Type.INT, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "");

    private String connectorName;

    @Override
    public void start(Map<String, String> props) {
        super.start(props);
        // Validate all of the configuration properties are provided
        new SimpleConfig(CONFIG_DEF, props);
        connectorName = props.get("name");
    }

    @Override
    public ConfigDef config() {
        log.info("Configured {} connector {}", this.getClass().getSimpleName(), connectorName);
        return CONFIG_DEF;
    }
}
