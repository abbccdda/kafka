/*
 * Copyright (C) 2020 Confluent, Inc.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The broker capacity config resolver implementation based on value stored directly in
 * the KafkaCruiseControlConfig.
 * KafkaCruiseControlConfig lets users specify a default value for a broker. Individual brokers
 * CANNOT have overrides in this case.
 *
 */
public class BrokerCapacityFromConfigResolver implements BrokerCapacityConfigResolver {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerCapacityFromConfigResolver.class);

    public static final int DEFAULT_CAPACITY_BROKER_ID = -1;

    // Configuration parameters that users can set to override.
    public static final String DEFAULT_DISK_CAPACITY_CONFIG = ConfluentConfigs.BALANCER_DISK_CAPACITY_BASE_CONFIG;
    public static final String DEFAULT_NETWORK_IN_CAPACITY_CONFIG = ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_BASE_CONFIG;
    public static final String DEFAULT_NETWORK_OUT_CAPACITY_CONFIG = ConfluentConfigs.BALANCER_NETWORK_OUT_CAPACITY_BASE_CONFIG;

    // Default CPU capacity is 100%
    private static final Double DEFAULT_CPU_CAPACITY = 100.0;

    private static final Map<Resource, String> BROKER_RESOURCE_CONFIGS = Stream.of(
            new AbstractMap.SimpleEntry<>(Resource.DISK, DEFAULT_DISK_CAPACITY_CONFIG),
            new AbstractMap.SimpleEntry<>(Resource.NW_IN, DEFAULT_NETWORK_IN_CAPACITY_CONFIG),
            new AbstractMap.SimpleEntry<>(Resource.NW_OUT, DEFAULT_NETWORK_OUT_CAPACITY_CONFIG))
    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

    private BrokerCapacityInfo defaultCapacity;
    @Override
    public BrokerCapacityInfo capacityForBroker(String rack, String host, int brokerId) {
        if (defaultCapacity == null) {
            throw new ConfigException("not configured");
        }
        if (brokerId >= 0) {
            // BrokerCapacityInfo returns an almost-immutable object (map
            // members are unmodifiableMap views) so it is safe to return
            // the defaultCapacity object without fear of modification.
            return defaultCapacity;
        } else {
            throw new IllegalArgumentException("The broker id(" + brokerId + ") should be non-negative.");
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        LOG.info("CruiseControl: Attempting to configure Broker Capacity from config properties");
        Map<Resource, Double> defaultBrokerCapacity = new HashMap<>();
        defaultBrokerCapacity.put(Resource.CPU, DEFAULT_CPU_CAPACITY);

        for (Map.Entry<Resource, String> resource : BROKER_RESOURCE_CONFIGS.entrySet()) {
            Double parsedValue;
            String configValue = KafkaCruiseControlUtils.getRequiredConfig(configs, resource.getValue());
            try {
                parsedValue = Double.parseDouble(configValue);
            } catch (NumberFormatException e) {
                throw new ConfigException("Invalid capacity (unparseable) " + configValue + " for capacity measure " + resource.getValue(),
                        e);
            }
            if (parsedValue < 0.0 || parsedValue == null) {
                throw new ConfigException("Negative capacity " + parsedValue + " (from " + configValue + ") is not allowed for " +
                        resource.getValue());
            }
            defaultBrokerCapacity.put(resource.getKey(), parsedValue);
        }
        // n.b. this constructor gives each broker a single CPU core, which may not be desirable.
        // As of SBK 1.0, CPU capacity is not considered for balancing so it's OK.
        defaultCapacity = new BrokerCapacityInfo(defaultBrokerCapacity, "default broker capacity");
    }

    @Override
    public void close()  {
        // Nothing to do, nothing ever opened
    }
}
