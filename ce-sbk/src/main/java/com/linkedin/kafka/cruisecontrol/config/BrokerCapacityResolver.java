/*
 * Copyright (C) 2020 Confluent, Inc.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import io.confluent.metrics.reporter.VolumeMetricsProvider;
import io.confluent.metrics.reporter.VolumeMetricsProvider.VolumeInfo;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The broker capacity config resolver implementation based on value stored directly in
 * the KafkaCruiseControlConfig.
 * KafkaCruiseControlConfig lets users specify a default value for a broker. Individual brokers can have
 * overrides for disk capacity, but NOT for CPU or network in/out.
 * The units of the definition are:
 *  DISK - MB
 *  CPU - Percentage (0 - 100)
 *  NW_IN - KB/s
 *  NW_OUT - KB/s
 */
public class BrokerCapacityResolver implements BrokerCapacityConfigResolver {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerCapacityResolver.class);

    public static final int DEFAULT_CAPACITY_BROKER_ID = -1;

    // Configuration parameters that users can set to override.
    public static final String LOG_DIRS_CONFIG = KafkaConfig$.MODULE$.LogDirsProp();

    // Visible for test cases
    static final double BYTES_PER_KB = 1024.0;

    // Default CPU capacity is 100%
    private static final Double DEFAULT_CPU_CAPACITY = 100.0;
    private static final double DISK_CAPACITY_UPDATE_THRESHOLD = 1d;
    private static final double DISK_CAPACITY_WARN_THRESHOLD = 1d;

    // Information on what config parameter stores a given resource value, and how to convert from
    // the units that config parameter is in to the units needed for the BrokerCapacityInfo
    private static class ResourceConfig {
         String configName;
         // Convert from how the value is stored/provided outside (bytes, bytes-per-sec, etc) to how the BrokerCapacityInfo stores it
         DoubleUnaryOperator conversionFunc;

         ResourceConfig(String configId, DoubleUnaryOperator converter) {
             configName = Objects.requireNonNull(configId);
             conversionFunc = Objects.requireNonNull(converter);
         }

         Double convertConfigValue(Double configValue) {
             return conversionFunc.applyAsDouble(configValue);
         }
    }

    // This tracks how config-specified resources are obtained (the config property name) and any unit conversions required
    // Config properties are specified in bytes but we store them as different units in the BrokerCapacityInfo (see above).
    private static final Map<Resource, ResourceConfig> BROKER_RESOURCE_CONFIGS = Stream.of(
            new AbstractMap.SimpleEntry<>(Resource.NW_IN,
                    new ResourceConfig(KafkaCruiseControlConfig.NETWORK_IN_CAPACITY_BYTES_CONFIG, bps -> bps / BYTES_PER_KB)),
            new AbstractMap.SimpleEntry<>(Resource.NW_OUT,
                    new ResourceConfig(KafkaCruiseControlConfig.NETWORK_OUT_CAPACITY_BYTES_CONFIG, bps -> bps / BYTES_PER_KB)))
    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

    private Map<Integer, BrokerCapacityInfo> capacitiesForBrokers = new HashMap<>();

    @Override
    public BrokerCapacityInfo capacityForBroker(String rack, String host, int brokerId) {
        if (capacitiesForBrokers.isEmpty()) {
            throw new ConfigException("not configured");
        }
        if (brokerId < 0) {
            throw new IllegalArgumentException("The broker id(" + brokerId + ") should be non-negative.");
        } else {
            BrokerCapacityInfo capacity = capacitiesForBrokers.get(brokerId);
            if (capacity != null) {
                return capacity;
            } else {
                LOG.debug("Returning default broker capacity for broker {}", brokerId);
                return capacitiesForBrokers.get(DEFAULT_CAPACITY_BROKER_ID);
            }
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        LOG.info("CruiseControl: Attempting to configure Broker Capacity from config properties");
        Map<Resource, Double> defaultBrokerCapacity = new HashMap<>();
        defaultBrokerCapacity.put(Resource.CPU, DEFAULT_CPU_CAPACITY);

        for (Map.Entry<Resource, ResourceConfig> resource : BROKER_RESOURCE_CONFIGS.entrySet()) {
            Double parsedValue;
            Long configValue = (Long) configs.get(resource.getValue().configName);

            try {
                parsedValue = resource.getValue().convertConfigValue(configValue.doubleValue());
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
        // use controller disk capacity as default for all brokers in cluster
        double diskCapacityInMiB = diskCapacityForCurrentBrokerInMiB(configs);
        if (diskCapacityInMiB < DISK_CAPACITY_WARN_THRESHOLD) {
            LOG.warn("Default broker disk capacity configured to {} MB", diskCapacityInMiB);
        }
        defaultBrokerCapacity.put(Resource.DISK, diskCapacityInMiB);
        // n.b. this constructor gives each broker a single CPU core, which may not be desirable.
        // As of SBK 1.0, CPU capacity is not considered for balancing so it's OK.
        capacitiesForBrokers.put(DEFAULT_CAPACITY_BROKER_ID, new BrokerCapacityInfo(defaultBrokerCapacity, "default broker capacity"));
    }

    @Override
    public void updateDiskCapacityForBroker(String rack, String host, int brokerId, double updateDiskCapacityInMiB) {
        if (brokerId < 0) {
            LOG.warn("Received disk capacity update for broker with negative id: {}, skipping update");
            return;
        }

        BrokerCapacityInfo brokerCapacity = capacityForBroker(rack, host, brokerId);
        double curDiskCapacity = brokerCapacity.capacity().getOrDefault(Resource.DISK, 0.0);
        // avoid floating point comparison, large epsilon because we don't care about small changes in disk capacity.
        if (Math.abs(curDiskCapacity - updateDiskCapacityInMiB) > DISK_CAPACITY_UPDATE_THRESHOLD) {
            LOG.info("Updating disk capacity for broker {} from {} to {} megabytes", brokerId, curDiskCapacity, updateDiskCapacityInMiB);
            Map<Resource, Double> updatedCapacity = new HashMap<>(brokerCapacity.capacity());
            updatedCapacity.put(Resource.DISK, updateDiskCapacityInMiB);
            capacitiesForBrokers.put(brokerId, new BrokerCapacityInfo(updatedCapacity));
        } else {
            LOG.debug("Skipping request to update disk capacity for broker {} from {} to {} megabytes",
                    brokerId, curDiskCapacity, updateDiskCapacityInMiB);
        }
    }

    private double diskCapacityForCurrentBrokerInMiB(Map<String, ?> configs) {
        String logDir = configs.get(LOG_DIRS_CONFIG) instanceof String ? (String) configs.get(LOG_DIRS_CONFIG) : null;
        if (logDir == null || logDir.length() == 0) {
            throw new ConfigException("Error calculating disk capacity, invalid log dir " + logDir);
        }
        VolumeMetricsProvider volumeMetricsProvider = new VolumeMetricsProvider(0, new String[]{logDir});
        Collection<VolumeInfo> volumeInfos = volumeMetricsProvider.getMetrics().values();
        if (volumeInfos.size() > 1) {
            throw new IllegalStateException("Static disk size estimation not supported for multiple volumes");
        }
        Optional<VolumeInfo> volume = volumeInfos.stream().findFirst();
        if (volume.isPresent()) {
            return (double) volume.get().totalBytes() / KafkaCruiseControlUtils.BYTES_IN_MB;
        } else {
            throw new ConfigException("Error calculating disk capacity, couldn't find volumes for log dir" + logDir);
        }
    }

    @Override
    public void close()  {
        // Nothing to do, nothing ever opened
    }
}
