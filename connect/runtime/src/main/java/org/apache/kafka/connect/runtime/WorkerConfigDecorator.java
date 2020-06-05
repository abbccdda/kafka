// (Copyright) [2020 - 2020] Confluent, Inc.
package org.apache.kafka.connect.runtime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The worker component that can decorate connector configurations and connector validation
 * results.
 *
 * <p>Currently this component looks for Confluent proprietary connectors that are defined
 * to use Confluent license properties, and injects Confluent license-related properties
 * from the worker config into the connector configurations and filters out license-related
 * configuration properties from the connectors' {@link ConfigDef} and validation results.
 *
 * @since 6.0
 */
public class WorkerConfigDecorator {

    private static final Logger log = LoggerFactory.getLogger(WorkerConfigDecorator.class);

    private static final String NO_DOC = "";

    /**
     * The name of the property used to define the Confluent license.
     * This reflects what is used by LicenseManager in each of the connectors.
     */
    protected static final String CONFLUENT_LICENSE_CONFIG = "confluent.license";
    private static final String CONFLUENT_LICENSE_DEFAULT = "";

    /**
     * The name of the property used to define the topic used for Confluent licenses.
     * This reflects what is used by LicenseManager in each of the connectors.
     */
    protected static final String CONFLUENT_TOPIC_CONFIG = "confluent.topic";
    private static final String CONFLUENT_TOPIC_DEFAULT = "_confluent-command";

    /**
     * The property prefixes used to define the producer and consumer properties used for Confluent licenses.
     * These reflects what is used by LicenseManager in each of the connectors.
     */
    protected static final String CONFLUENT_TOPIC_PREFIX = CONFLUENT_TOPIC_CONFIG + ".";
    protected static final String CONFLUENT_TOPIC_PRODUCER_PREFIX = CONFLUENT_TOPIC_PREFIX + "producer.";
    protected static final String CONFLUENT_TOPIC_CONSUMER_PREFIX = CONFLUENT_TOPIC_PREFIX + "consumer.";

    /**
     * The name of the property used to define the bootstrap servers where the Confluent licenses are stored.
     * This reflects what is used by LicenseManager in each of the connectors.
     */
    protected static final String CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG = CONFLUENT_TOPIC_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
    private static final List<?> CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_DEFAULT = Collections.emptyList();

    /**
     * The name of the property used to define the topic replication factor for the topic where the Confluent licenses are stored.
     * This reflects what is used by LicenseManager in each of the connectors.
     */
    protected static final String CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG = CONFLUENT_TOPIC_PREFIX + "replication.factor";
    private static final int CONFLUENT_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;

    /**
     * The feature flag that enables/disables this feature. This is enabled by default, and this
     * configuration is marked as "internal" so that it is not defined in documentation, though
     * it is still included in the logs.
     */
    protected static final String CONFLUENT_INJECT_INTO_CONNECTORS_CONFIG = CONFLUENT_LICENSE_CONFIG + ".inject.into.connectors";

    private static final List<DecorationPattern> PATTERNS = Collections.singletonList(
            new LicensePropertiesDecorator()
    );

    /**
     * Define any additional worker configuration properties, used to validate the worker properties
     * upon worker startup.
     *
     * <p>Note: the worker {@link ConfigDef} are only used for validation and therefore none of the
     * added config keys contain documentation, group name, or order in group.
     *
     * @param workerConfigDef the original {@link ConfigDef} for the worker; may not be null
     * @return the modified {@link ConfigDef} for the worker; never null, but may be
     *         {@code workerConfigDef} if no changes are to be made
     */
    static ConfigDef decorateWorkerConfig(ConfigDef workerConfigDef, Map<String, String> workerProps) {
        ConfigDef copy = new ConfigDef(workerConfigDef);
        if (PATTERNS.stream().map(p -> p.addToWorkerConfig(copy, workerProps)).anyMatch(r -> r)) {
            // At least one pattern modified the config def
            return copy;
        }
        // No patterns modified the config def, so return the original
        return workerConfigDef;
    }

    /**
     * Create an instance of the decorator using the supplied worker configuration properties.
     * The resulting decorator can use the supplied {@link WorkerConfigTransformer} to resolve
     * any externalized property variables in the worker configuration property values, either
     * at initialization time or every time properties are injected (preferred).
     *
     * @param workerProperties the worker's properties; may not be null
     * @param transformer      the transformer that can be used to replace externalized variables
     *                         in the worker properties with the resolved values; may not be null
     * @return the decorator instance; never null
     */
    protected static WorkerConfigDecorator initialize(
        WorkerConfig workerProperties,
        WorkerConfigTransformer transformer
    ) {
        return new WorkerConfigDecorator(workerProperties, transformer);
    }


    /**
     * Utility method to remove all of the specified keys from the given {@link ConfigDef}.
     *
     * @param configDef     the {@link ConfigDef} from which keys are to be removed
     * @param keysToExclude the names of the keys to be excluded; may not be null but may be
     *                      empty
     * @return the copy of the given {@link ConfigDef} but without the excluded keys; never null
     */
    private static ConfigDef without(ConfigDef configDef, String... keysToExclude) {
        Set<String> excludedKeys = new HashSet<>(Arrays.asList(keysToExclude));
        ConfigDef result = new ConfigDef();
        configDef.configKeys().forEach((key, value) -> {
            if (!excludedKeys.contains(key)) {
                result.define(value);
            }
        });
        return result;
    }

    /**
     * Get a new {@link ConfigDef} that contains all of the configuration keys that are in
     * both of the given {@link ConfigDef}.
     *
     * @param firstConfig  the first {@link ConfigDef}; may not be null
     * @param secondConfig the second {@link ConfigDef}; may not be null
     * @return a new {@link ConfigDef} that contains only the configuration keys that are
     * common to both of the supplied {@link ConfigDef} objects
     */
    private static ConfigDef commonConfigDef(ConfigDef firstConfig, ConfigDef secondConfig) {
        ConfigDef result = new ConfigDef();
        firstConfig.configKeys().forEach((key, value) -> {
            if (secondConfig.configKeys().keySet().contains(key)) {
                result.define(value);
            }
        });
        return result;
    }

    /**
     * Utility to add with the given prefix the keys in specified {@link ConfigDef} to the target
     * {@link ConfigDef}.
     *
     * @param configDef the target {@link ConfigDef} into which the prefixed keys should be added;
     *                  may not be null
     * @param prefix    the prefix; may not be null, but may be blank
     * @param toAdd     the {@link ConfigDef} whose keys should be added
     */
    protected static void addWithPrefix(ConfigDef configDef, String prefix, ConfigDef toAdd) {
        configDef.embed(
                Objects.requireNonNull(prefix),
                "",
                configDef.configKeys().size(),
                Objects.requireNonNull(toAdd)
        );
    }

    private final WorkerConfig workerConfig;
    private final WorkerConfigTransformer transformer;

    protected WorkerConfigDecorator(WorkerConfig workerConfig, WorkerConfigTransformer transformer) {
        this.workerConfig = Objects.requireNonNull(workerConfig);
        this.transformer = Objects.requireNonNull(transformer);
    }

    /**
     * Modify the connector configuration for the connector with the given name and {@link ConfigDef}
     * prior to being passed to the connector's {@link Connector#start(Map)} method.
     *
     * <p>This method returns the supplied connector properties if the name is null, since the
     * herder will catch the lack of a name after this method is called.
     *
     * @param connectorName the name of the connector
     * @param connector     the {@link Connector} instance; may not be null
     * @param configDef     the {@link ConfigDef} of the connector; may not be null
     * @param properties    the connector configuration properties; may not be null
     * @return the decorated connector configuration properties; never null, but may be
     *         {@code properties} if no changes are to be made
     */
    public Map<String, String> decorateConnectorConfig(
        String connectorName,
        Connector connector,
        ConfigDef configDef,
        Map<String, String> properties
    ) {
        if (connectorName == null) {
            // A connector name is required, but the herder catches null names after this method
            return properties;
        }
        Objects.requireNonNull(connector);
        Objects.requireNonNull(configDef);
        Objects.requireNonNull(properties);

        Map<String, String> decorated = new HashMap<>(properties);
        PATTERNS.forEach(pattern -> {
            Map<String, String> injected = pattern.injectedConnectorProperties(workerConfig, connectorName, configDef, properties);
            decorated.putAll(injected);
        });
        return decorated;
    }

    /**
     * Modify the validation result returned from the {@link Connector#validate} method. If the
     * decorator added or removed properties from the connector configuration, it should also
     * add/remove the corresponding config key(s) and config value(s).
     *
     * <p>This method returns the supplied connector properties if the name is null, since the
     * herder will catch the lack of a name after this method is called.
     *
     * @param connectorName  the name of the connector
     * @param connector      the {@link Connector} instance; may not be null
     * @param configDef      the {@link ConfigDef} of the connector; may not be null
     * @param original       the connector configuration properties before decoration; may not be null
     * @param validateResult the {@link ConfigInfos} returned by the connector's validate method;
     *                       may not be null
     * @return the decorated task configuration properties; never null, but may be
     *         {@code properties} if no changes are to be made
     */
    public ConfigInfos decorateValidationResult(
        String connectorName,
        Connector connector,
        ConfigDef configDef,
        Map<String, String> original,
        ConfigInfos validateResult
    ) {
        if (connectorName == null) {
            // A connector name is required, but the herder catches null names after this method
            return validateResult;
        }
        // If the connector name is not specified, the herder will catch it AFTER this call
        Objects.requireNonNull(connector);
        Objects.requireNonNull(configDef);
        Objects.requireNonNull(original);
        Objects.requireNonNull(validateResult);

        MutableConfigInfos infos = new MutableConfigInfos(validateResult);

        PATTERNS.forEach(pattern -> pattern.filterValidationResults(workerConfig, connectorName, original, infos));
        infos.forEachErrorInRemoved(info ->
            log.debug("Found {} error(s) in injected property {}: {}",
                    info.configValue().errors().size(), info.configKey().name(), info.configValue().errors())
        );

        return infos.asConfigInfos();
    }

    private static class MutableConfigInfos {

        private final ConfigInfos original;
        private final List<ConfigInfo> values;
        private final List<ConfigInfo> removed = new ArrayList<>();

        public MutableConfigInfos(ConfigInfos original) {
            this.original = Objects.requireNonNull(original);
            this.values = this.original.values();
        }

        public int removeAllWithName(Predicate<String> keyPredicate) {
            return removeAll(info -> {
                String name = info.configKey().name();
                return keyPredicate.test(name);
            });
        }

        public int removeAll(Predicate<ConfigInfo> predicate) {
            int removedCount = 0;
            Iterator<ConfigInfo> iter = values.iterator();
            while (iter.hasNext()) {
                ConfigInfo info = iter.next();
                if (predicate.test(info)) {
                    removed.add(info);
                    iter.remove();
                    ++removedCount;
                }
            }
            return removedCount;
        }

        public void forEachErrorInRemoved(Consumer<ConfigInfo> handler) {
            removed.stream().forEach(handler);
        }

        public ConfigInfos asConfigInfos() {
            if (removed.isEmpty()) {
                return original;
            }

            // Compute the errors and groups
            AtomicInteger errorCount = new AtomicInteger();
            Set<String> groups = new LinkedHashSet<>();
            values.forEach(configInfo -> {
                errorCount.addAndGet(configInfo.configValue().errors().size());
                groups.add(configInfo.configKey().group());
            });
            return new ConfigInfos(original.name(), errorCount.get(), new ArrayList<>(groups), values);
        }
    }

    /**
     * A connector configuration decorator pattern.
     *
     * <p>Each decorator pattern has the ability to:
     * <ul>
     *     <li>{@link #addToWorkerConfig(ConfigDef, Map)}  Add config keys} to the Connect worker's
     *     {@link ConfigDef}. By default, this method does nothing.</li>
     *     <li>{@link #injectedConnectorProperties(WorkerConfig, String, ConfigDef, Map)
     *     compute additional connector properties} that can be injected into connector
     *     configurations at runtime, just before they are started. By default, this class
     *     first determines {@link #shouldInjectIntoConnector(WorkerConfig, String, ConfigDef, Map) whether}
     *     the connector already has properties that {@link #matchesInjectedProperty(String) match}
     *     the injected properties, where the match logic must be supplied by subclasses.
     *     If no properties are found to match the properties that might be injected, then the
     *     {@link #computeInjectedProperties(WorkerConfig, String, ConfigDef, Map) computes}
     *     and then injects properties into the connector configuration.</li>
     *     <li>{@link #filterValidationResults(WorkerConfig, String, Map, MutableConfigInfos)}
     *     filter connector validation results} to remove any validation results or problems
     *     that apply to the connector properties injected into the connector at
     *     runtime. By default, all validation results are removed for any configuration
     *     that was injected.</li>
     * </ul>
     */
    protected static abstract class DecorationPattern {

        private final String patternName;

        protected DecorationPattern(String patternName) {
            this.patternName = Objects.requireNonNull(patternName);
        }

        /**
         * Get the name of this pattern.
         *
         * @return the decoration pattern name; never null
         */
        public String patternName() {
            return this.patternName;
        }

        /**
         * Add any configuration keys to the specified worker ConfigDef.
         *
         * @param configDef   the worker's {@link ConfigDef}; may not be null
         * @param workerProps the worker's configuration properties; may not be null
         * @return true if the worker config was modified, or false otherwise
         */
        public boolean addToWorkerConfig(ConfigDef configDef, Map<String, String> workerProps) {
            // do nothing
            return false;
        }

        /**
         * Compute the properties that should be injected into the connector configuration.
         *
         * <p>This method calls
         * {@link #shouldInjectIntoConnector(WorkerConfig, String, ConfigDef, Map)}
         * to determine whether the properties should be injected, and if so calls
         * {@link #computeInjectedProperties(WorkerConfig, String, ConfigDef, Map)}.
         * Otherwise, an empty map is returned.
         *
         * @param workerConfig       the worker configuration; may not be null
         * @param connectorName      the name of the connector; may not be null
         * @param connectorConfigDef the connector's {@link ConfigDef}; may not be null
         * @param connectorConfig    the connector's configuration; may not be null
         * @return the injected properties, or an empty map if no properties are to be injected
         */
        public Map<String, String> injectedConnectorProperties(WorkerConfig workerConfig,
                String connectorName, ConfigDef connectorConfigDef,
                Map<String, String> connectorConfig) {
            if (!isEnabled(workerConfig.originalsStrings())) {
                log.debug("{} injection is disabled; injecting no properties into connector '{}'", this, connectorName);
                return Collections.emptyMap();
            }
            if (shouldInjectIntoConnector(workerConfig, connectorName, connectorConfigDef, connectorConfig)) {
                log.info("Injecting {} properties into connector '{}'", patternName(), connectorName);
                return computeInjectedProperties(workerConfig, connectorName, connectorConfigDef, connectorConfig);
            }
            return Collections.emptyMap();
        }

        /**
         * Filter the validation results to remove any configuration properties that apply
         * to injected properties that the user doesn't know about.
         *
         * @param workerConfig            the worker configuration; may not be null
         * @param connectorName           the name of the connector; may not be null
         * @param originalConnectorConfig the connector's configuration; may not be null
         * @param validationResults       the mutable validation results; may not be null
         * @return true if the validation results were removed, or false otherwise
         */
        public boolean filterValidationResults(WorkerConfig workerConfig, String connectorName,
                Map<String, String> originalConnectorConfig, MutableConfigInfos validationResults) {
            if (!isEnabled(workerConfig.originalsStrings())) {
                log.debug("{} injection is disabled; no filtering of validation results for connector '{}'", this, connectorName);
                return false;
            }
            if (!alreadyHasInjectedConnectorProperties(originalConnectorConfig)
                && validationResults.removeAllWithName(this::matchesInjectedProperty) > 0) {
                log.debug("Removing injected {} properties from validation results for "
                          + "connector '{}'", patternName(), connectorName);
                return true;
            }
            log.debug("Found 0 injected {} properties from validation results for "
                      + "connector '{}'", patternName(), connectorName);
            return false;
        }

        /**
         * Determine whether this pattern is enabled.
         *
         * @param workerConfig the worker configuration; never null
         * @return true if the pattern is enabled, or false otherwise
         */
        public boolean isEnabled(Map<String, String> workerConfig) {
            return true;
        }

        @Override
        public String toString() {
            return patternName;
        }

        /**
         * Determine if this pattern should inject properties into the connector configuration.
         *
         * <p>This method returns true if {@link #requiresInjectedConnectorProperties(ConfigDef)}
         * is true <em>and</em> {@link #alreadyHasInjectedConnectorProperties(Map)} is false.
         *
         * @param config             the worker configuration; may not be null
         * @param connectorName      the name of the connector; may not be null
         * @param connectorConfigDef the connector's {@link ConfigDef}; may not be null
         * @param connectorConfig    the connector's configuration; may not be null
         * @return true if properties are to be injected, or false otherwise
         */
        protected boolean shouldInjectIntoConnector(WorkerConfig config, String connectorName,
                ConfigDef connectorConfigDef, Map<String, String> connectorConfig) {
            return requiresInjectedConnectorProperties(connectorConfigDef)
                   && !alreadyHasInjectedConnectorProperties(connectorConfig);
        }

        /**
         * Determine if the pattern applies to the given connector {@link ConfigDef}.
         *
         * <p>This method returns true if any of the connector configuration key names
         * {@link #matchesInjectedProperty(String) match the injected properties}.
         *
         * @param configDef the connector {@link ConfigDef}; may not be null
         * @return true if the connector requires decorated properties, or false otherwise
         */
        protected boolean requiresInjectedConnectorProperties(ConfigDef configDef) {
            return configDef.configKeys()
                            .keySet()
                            .stream()
                            .filter(Objects::nonNull)
                            .anyMatch(this::matchesInjectedProperty);
        }

        /**
         * Determine if the connector configuration already contains the properties matching this
         * decorator pattern.
         *
         * <p>This method returns true if any of the connector properties
         * {@link #matchesInjectedProperty(String) match the injected properties}.
         *
         * @param properties the connector configuration; may not be null
         * @return true if the connector already has the injected/decorated properties, or false
         *         otherwise
         */
        protected boolean alreadyHasInjectedConnectorProperties(Map<String, String> properties) {
            return properties.keySet()
                             .stream()
                             .anyMatch(this::matchesInjectedProperty);
        }

        /**
         * Determine if the specified property key matches one of the injected properties.
         * This may be used to determine whether a connector configuration already has properties
         * that would otherwise be injected.
         *
         * @param key the key of the property; never null
         * @return true if this pattern matches because of the given the given property,
         *         or false otherwise
         */
        protected abstract boolean matchesInjectedProperty(String key);

        /**
         * Compute the injected properties.
         *
         * @param config             the worker configuration; may not be null
         * @param connectorName      the name of the connector; may not be null
         * @param connectorConfigDef the connector's {@link ConfigDef}; may not be null
         * @param connectorConfig    the connector's configuration; may not be null
         * @return the injected properties; never null
         */
        protected abstract Map<String, String> computeInjectedProperties(WorkerConfig config,
                String connectorName,
                ConfigDef connectorConfigDef, Map<String, String> connectorConfig);
    }

    /**
     * Decorator that applies license properties from the worker config to Confluent-licensed
     * connector configurations.
     */
    protected static class LicensePropertiesDecorator extends DecorationPattern {

        public LicensePropertiesDecorator() {
            super("Confluent license");
        }

        @Override
        public boolean isEnabled(Map<String, String> workerConfig) {
            Object enabledValue = workerConfig.get(CONFLUENT_INJECT_INTO_CONNECTORS_CONFIG);
            return enabledValue == null ||
                   Boolean.TRUE.equals(enabledValue) ||
                   "true".equalsIgnoreCase(enabledValue.toString().trim());
        }

        @Override
        public boolean addToWorkerConfig(ConfigDef workerConfigDef, Map<String, String> workerProps) {
            if (!isEnabled(workerProps)) {
                log.info("Confluent license injection into licensed connectors is disabled. Connectors must configure license-related properties");
                return false;
            }

            log.debug("Injecting Confluent license properties into worker config");
            Objects.requireNonNull(workerConfigDef)
                   .defineInternal(CONFLUENT_INJECT_INTO_CONNECTORS_CONFIG, Type.BOOLEAN, Boolean.TRUE, Importance.LOW)
                   .define(CONFLUENT_LICENSE_CONFIG, Type.PASSWORD, CONFLUENT_LICENSE_DEFAULT, Importance.MEDIUM, NO_DOC)
                   .define(CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, Type.LIST,
                           CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_DEFAULT, Importance.MEDIUM, NO_DOC)
                   .define(CONFLUENT_TOPIC_CONFIG, Type.STRING, CONFLUENT_TOPIC_DEFAULT, Importance.LOW, NO_DOC)
                   .define(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG, Type.INT,
                           CONFLUENT_TOPIC_REPLICATION_FACTOR_DEFAULT, Range.atLeast(1), Importance.LOW, NO_DOC
                   );

            // We add most of the producer and consumer configs, except those that the license manager
            // will always set (and which don't have defaults). Note that we enforce that the
            // license manager's producer and consumer use the same bootstrap servers, so we
            // only allow setting 'confluent.topic.bootstrap.servers'.
            ConfigDef producerConfigs = without(ProducerConfig.configDef(),
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProducerConfig.RETRIES_CONFIG
            );
            ConfigDef consumerConfigs = without(ConsumerConfig.configDef(),
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
            );
            ConfigDef commonClientConfigs = commonConfigDef(producerConfigs, consumerConfigs);
            addWithPrefix(workerConfigDef, CONFLUENT_TOPIC_PREFIX, commonClientConfigs);
            addWithPrefix(workerConfigDef, CONFLUENT_TOPIC_PRODUCER_PREFIX, producerConfigs);
            addWithPrefix(workerConfigDef, CONFLUENT_TOPIC_CONSUMER_PREFIX, consumerConfigs);
            return true;
        }

        @Override
        protected boolean matchesInjectedProperty(String key) {
            return CONFLUENT_LICENSE_CONFIG.equals(key) || CONFLUENT_TOPIC_CONFIG.equals(key) || key.startsWith(CONFLUENT_TOPIC_PREFIX);
        }

        @Override
        public Map<String, String> computeInjectedProperties(
                WorkerConfig workerConfig,
                String connectorName,
                ConfigDef connectorConfigDef,
                Map<String, String> connectorConfig
        ) {
            log.debug("Injecting Confluent license properties into '{}' connector configuration", connectorName);
            Map<String, String> injectable = new HashMap<>();
            // Always include the (original) bootstrap servers even if the worker config uses the default
            String bootstrap = workerConfig.originalsStrings().get(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG);
            injectable.put(CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, bootstrap);

            // Always include the basic license properties that have defaults in the worker config
            injectable.put(CONFLUENT_LICENSE_CONFIG, workerConfig.getPassword(CONFLUENT_LICENSE_CONFIG).value());
            injectable.put(CONFLUENT_TOPIC_CONFIG, workerConfig.getString(CONFLUENT_TOPIC_CONFIG));
            injectable.put(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG,
                    Integer.toString(workerConfig.getInt(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG)));

            // Get the original worker properties and remove anything specific to the Connect cluster
            Map<String, ?> originals = workerConfig.originals();
            originals.remove(DistributedConfig.GROUP_ID_CONFIG);

            // Add the client properties at the top level
            addInherited(originals, injectable);
            // Add the client properties from `confluent.topic.*`, overwriting previously-inherited worker properties
            addInherited(workerConfig.originalsWithPrefix(CONFLUENT_TOPIC_PREFIX), injectable);
            // Add all license properties, overwriting anything we've seen so far
            workerConfig.originalsStrings()
                        .entrySet()
                        .stream()
                        .filter(entry -> matchesInjectedProperty(entry.getKey()))
                        .forEach(entry -> injectable.put(entry.getKey(), entry.getValue()));

            // Ensure that all license clients use the same Kafka server
            // (and besides we didn't define these config keys so they are not real properties)
            injectable.remove(CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
            injectable.remove(CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);

            // Remove any client properties NOT used by the license manager
            injectable.remove(CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
            injectable.remove(CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
            injectable.remove(CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.RETRIES_CONFIG);
            injectable.remove(CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
            injectable.remove(CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
            injectable.remove(CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
            injectable.remove(CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

            return injectable;
        }

        /**
         * Determine which of the original properties define {@link ProducerConfig producer} and/or
         * {@link ConsumerConfig consumer} properties, and add producer properties to the result
         * using the {@value CONFLUENT_TOPIC_PRODUCER_PREFIX} and consumer properties using the
         * {@value CONFLUENT_TOPIC_CONSUMER_PREFIX}.
         *
         * @param original the original properties to evaluate; may not be null
         * @param result   the properties to which the consumer and producer properties should be
         *                 added; may not be null
         */
        protected void addInherited(
                Map<String, ?> original,
                Map<String, String> result
        ) {
            original.entrySet().forEach(entry ->
                    addInherited(entry.getKey(), entry.getValue().toString(), result)
            );
        }

        /**
         * If the key is one of the {@link ProducerConfig producer} or {@link ConsumerConfig
         * consumer} properties, add producer properties to the result using the {@value
         * CONFLUENT_TOPIC_PRODUCER_PREFIX} and consumer properties using the {@value
         * CONFLUENT_TOPIC_CONSUMER_PREFIX}.
         *
         * @param key    the property key
         * @param value  the property value
         * @param result the map to which all client properties should be added; may not be null
         */
        protected void addInherited(
                String key,
                String value,
                Map<String, String> result
        ) {
            boolean producerProp = ProducerConfig.configNames().contains(key);
            boolean consumerProp = ConsumerConfig.configNames().contains(key);
            if (matchesInjectedProperty(key)) {
                result.put(key, value);
            } else if (producerProp && consumerProp) {
                result.put(CONFLUENT_TOPIC_PREFIX + key, value);
            } else if (producerProp) {
                result.put(CONFLUENT_TOPIC_PRODUCER_PREFIX + key, value);
            } else if (consumerProp) {
                result.put(CONFLUENT_TOPIC_CONSUMER_PREFIX + key, value);
            }
        }
    }
}
