/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.config.internals;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.requests.SamplingRequestLogFilter;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.interceptor.BrokerInterceptor;
import org.apache.kafka.server.interceptor.DefaultBrokerInterceptor;
import org.apache.kafka.server.license.LicenseValidator;
import org.apache.kafka.server.multitenant.MultiTenantMetadata;

public class ConfluentConfigs {
    private static final String CONFLUENT_PREFIX = "confluent.";

    public static final String BROKER_INTERCEPTOR_CLASS_CONFIG = "broker.interceptor.class";
    public static final Class<?> BROKER_INTERCEPTOR_CLASS_DEFAULT = DefaultBrokerInterceptor.class;
    public static final String MULTITENANT_METADATA_CLASS_CONFIG = "multitenant.metadata.class";
    public static final String MULTITENANT_METADATA_CLASS_DEFAULT = null;
    public static final String MULTITENANT_METADATA_DIR_CONFIG = "multitenant.metadata.dir";
    public static final String MULTITENANT_METADATA_DIR_DEFAULT = null;
    public static final String MULTITENANT_METADATA_SSL_CERTS_SPEC_CONFIG = "multitenant.metadata.ssl.certs.path";
    public static final String MULTITENANT_METADATA_SSL_CERTS_SPEC_DEFAULT = null;
    public static final String MULTITENANT_METADATA_RELOAD_DELAY_MS_CONFIG = "multitenant.metadata"
            + ".reload.delay.ms";
    public static final Long MULTITENANT_METADATA_RELOAD_DELAY_MS_DEFAULT = TimeUnit.MINUTES.toMillis(10);
    public static final String MULTITENANT_METADATA_RELOAD_DELAY_MS_DOC = "Interval (in ms) "
            + "between full reloads of logical cluster metadata. Defaults to 10 minutes.";
    public static final String MULTITENANT_TENANT_DELETE_DELAY_MS_CONFIG = "multitenant.tenant"
            + ".delete.delay";
    public static final Long MULTITENANT_TENANT_DELETE_DELAY_MS_DEFAULT = TimeUnit.DAYS.toMillis(7);
    public static final String MULTITENANT_TENANT_DELETE_DELAY_MS_DOC = "Delay between the time "
            + "the tenant is marked as deactivated in JSON file, until we actually start deleting"
            + " topics. This defaults to 7 days to allow plenty of times for operators and users "
            + "to regret their decisions and do something about it";
    public static final String MULTITENANT_TENANT_DELETE_BATCH_SIZE_CONFIG = "multitenant.tenant"
            + ".delete.batch.size";
    // This value is based on the idea that the controller is more efficient in deleting batches
    // But too many will block the controller for a while, so I'm erring on the low side
    public static final Integer MULTITENANT_TENANT_DELETE_BATCH_SIZE_DEFAULT = 10;
    public static final String MULTITENANT_TENANT_DELETE_BATCH_SIZE_DOC = "Batch size for topic "
            + "deletion of deactivated tenants. We wait for each batch to complete before sending"
            + " another";

    // TODO: for the above broker-level configs, we did not have the convention to add "confluent." to the configs;
    // for new configs added below, they should be added with CONFLUENT_PREFIX

    // the following are copied from AbstractKafkaAvroSerDeConfig, we duplicate these const strings here in order
    // to avoid introducing the dependency of schema-registry
    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String SCHEMA_REGISTRY_URL_CONFIG = CONFLUENT_PREFIX + SCHEMA_REGISTRY_URL;
    public static final String SCHEMA_REGISTRY_URL_DOC =
        "Comma-separated list of URLs for schema registry instances that can be used to "
            + "look up schemas.";

    public static final String MAX_CACHE_SIZE_CONFIG = CONFLUENT_PREFIX + "schema.registry.max.cache.size";
    public static final String MAX_CACHE_SIZE_DOC =
        "Maximum size of each LRU cache used to cache responses from the schema registry. "
            + "There is one cache to hold the ID to schema mappings and another to hold "
            + "the schemas that are registered to a subject.";
    public static final int MAX_CACHE_SIZE_DEFAULT = 10_000;

    public static final String MAX_RETRIES_CONFIG = CONFLUENT_PREFIX + "schema.registry.max.retries";
    public static final String MAX_RETRIES_DOC = "Maximum number of times to retry schema registry read operations.";
    public static final int MAX_RETRIES_DEFAULT = 1;

    public static final String RETRIES_WAIT_MS_CONFIG = CONFLUENT_PREFIX + "schema.registry.retries.wait.ms";
    public static final String RETRIES_WAIT_MS_DOC = "Time in milliseconds to wait before each retry.";
    public static final int RETRIES_WAIT_MS_DEFAULT = 0;

    public static final String MISSING_ID_QUERY_RANGE_CONFIG = CONFLUENT_PREFIX + "missing.id.query.range";
    public static final String MISSING_ID_QUERY_RANGE_DOC = "The range above max schema ID to make calls to Schema Registry";
    public static final int MISSING_ID_QUERY_RANGE_DEFAULT = 200;

    public static final String MISSING_ID_CACHE_TTL_CONFIG = CONFLUENT_PREFIX + "missing.id.cache.ttl.sec";
    public static final String MISSING_ID_CACHE_TTL_DOC = "The TTL in seconds for caching missing schema IDs";
    public static final long MISSING_ID_CACHE_TTL_DEFAULT  = 60;

    // for configs defined for both per-broker and per-topic, it should be defined in ConfluentTopicConfig instead.

    // used to check if the broker is configured for tenant-level quotas (by verifying that
    // "client.quota.callback.class" config is set to TenantQuotaCallback)
    public static final String TENANT_QUOTA_CALLBACK_CLASS = "io.confluent.kafka.multitenant.quota.TenantQuotaCallback";

    public static final String BACKPRESSURE_TYPES_CONFIG = CONFLUENT_PREFIX + "backpressure.types";
    public static final String BACKPRESSURE_TYPES_DEFAULT = null;
    public static final String BACKPRESSURE_TYPES_DOC =
        "Comma separated list of resource types for which broker back-pressure is enabled. "
        + "Backpressure is not enabled by default. Accepted values: 'request', 'produce', 'fetch'."
        + "Invalid values are ignored. This config is ignored if client.quota.callback.class is "
        + "not set, or set to class other than TenantQuotaCallback. In other words, broker"
        + " back-pressure can be enabled for multi-tenant clusters only.";

    // An enum is used over a boolean for balancer mode since it would have more than two modes later.
    public enum BalancerMode {
        ENABLED,
        DISABLED
    }

    public static final String CONFLUENT_BALANCER_PREFIX = CONFLUENT_PREFIX + "balancer.";
    public static final String BALANCER_MODE_CONFIG = CONFLUENT_BALANCER_PREFIX + "mode";
    public static final String BALANCER_MODE_DEFAULT = BalancerMode.ENABLED.toString();
    public static final String BALANCER_MODE_DOC = "The mode config is used to enable or disable the balancer.";

    public static final String BALANCER_RACK_AWARE_CONFIG = CONFLUENT_BALANCER_PREFIX + "rack.aware";
    public static final boolean BALANCER_RACK_AWARE_DEFAULT = false;
    public static final String BALANCER_RACK_AWARE_DOC = "This config controls whether the balancer takes broker racks " +
            "into account for replica placement decision (no more than one replica per rack).";

    public static final String BALANCER_THROTTLE_CONFIG = CONFLUENT_BALANCER_PREFIX + "throttle.bytes.per.second";
    public static final Long BALANCER_THROTTLE_DEFAULT = null;
    public static final String BALANCER_THROTTLE_DOC = "This config specifies the upper bound for bandwidth in bytes to " +
            "move replicas around for replica reassignment.";

    public static final String BALANCER_REPLICA_CAPACITY_CONFIG = CONFLUENT_BALANCER_PREFIX + "max.replicas";
    public static final Long BALANCER_REPLICA_CAPACITY_DEFAULT = null;
    public static final String BALANCER_REPLICA_CAPACITY_DOC = "The replica capacity is the maximum number of replicas " +
            "the balancer will place on a single broker.";

    public static final String BALANCER_DISK_CAPACITY_CONFIG = CONFLUENT_BALANCER_PREFIX + "disk.max.bytes";
    public static final Long BALANCER_DISK_CAPACITY_DEFAULT = null;
    public static final String BALANCER_DISK_CAPACITY_DOC = "This config specifies the upper bound for disk usage in " +
            "bytes per broker.";

    public static final String BALANCER_NETWORK_IN_CAPACITY_CONFIG = CONFLUENT_BALANCER_PREFIX + "network.in.bytes.per.second";
    public static final Long BALANCER_NETWORK_IN_CAPACITY_DEFAULT = null;
    public static final String BALANCER_NETWORK_IN_CAPACITY_DOC = "This config specifies the upper bound for network " +
            "incoming bytes per broker.";

    public static final String BALANCER_NETWORK_OUT_CAPACITY_CONFIG = CONFLUENT_BALANCER_PREFIX + "network.out.bytes.per.second";
    public static final Long BALANCER_NETWORK_OUT_CAPACITY_DEFAULT = null;
    public static final String BALANCER_NETWORK_OUT_CAPACITY_DOC = "This config specifies the upper bound for network " +
            "outgoing bytes per broker.";

    public static final String BALANCER_EXCLUDE_TOPIC_NAMES_CONFIG = CONFLUENT_BALANCER_PREFIX + "exclude.topic.names";
    public static final List BALANCER_EXCLUDE_TOPIC_NAMES_DEFAULT = Collections.EMPTY_LIST;
    public static final String BALANCER_EXCLUDE_TOPIC_NAMES_DOC = "This config accepts a list of topic names that " +
            "will be excluded from rebalancing. For example, 'balancer.exclude.topic.names=[topic1, topic2]' ";

    public static final String BALANCER_EXCLUDE_TOPIC_PREFIXES_CONFIG = CONFLUENT_BALANCER_PREFIX + "exclude.topic.prefixes";
    public static final List BALANCER_EXCLUDE_TOPIC_PREFIXES_DEFAULT = Collections.EMPTY_LIST;
    public static final String BALANCER_EXCLUDE_TOPIC_PREFIXES_DOC = "This config accepts a list of topic prefixes that " +
            "will be excluded from rebalancing. For example, 'balancer.exclude.topic.prefixes=[prefix1, prefix2]' would " +
            "exclude topics 'prefix1-suffix1', 'prefix1-suffix2', 'prefix2-suffix3', but not 'abc-prefix1-xyz'" +
            " and 'def-prefix2'";

    public static final String MULTITENANT_LISTENER_NAMES_CONFIG = CONFLUENT_PREFIX + "multitenant.listener.names";
    public static final String MULTITENANT_LISTENER_NAMES_DEFAULT = null;
    public static final String MULTITENANT_LISTENER_NAMES_DOC =
        "Comma separated list of listener names used for communications with tenants. If this is "
        + "unset, broker request (time on network and IO threads) backpressure will not be applied.";

    public static final String REQUEST_LOG_FILTER_CLASS_CONFIG = CONFLUENT_PREFIX + "request.log.filter.class";
    public static final String REQUEST_LOG_FILTER_DEFAULT = SamplingRequestLogFilter.class.getName();
    public static final String REQUEST_LOG_FILTER_CLASS_DOC = "Class of request log filter which can be " +
            "used to select a subset of requests for logging. Every request handler thread will get a separate " +
            "instance of this class and is only consulted if the request log level is set to INFO or higher.";

    public static final String APPLY_CREATE_TOPIC_POLICY_TO_CREATE_PARTITIONS =
        CONFLUENT_PREFIX + "apply.create.topic.policy.to.create.partitions";
    public static final boolean APPLY_CREATE_TOPIC_POLICY_TO_CREATE_PARTITIONS_DEFAULT = false;
    public static final String APPLY_CREATE_TOPIC_POLICY_TO_CREATE_PARTITIONS_DOC = "If this is set, " +
        "CreateTopicsPolicy will also apply to CreatePartitions.";

    public static final String VERIFY_GROUP_SUBSCRIPTION_PREFIX =
        CONFLUENT_PREFIX + "verify.group.subscription.prefix";
    public static final boolean VERIFY_GROUP_SUBSCRIPTION_PREFIX_DEFAULT = false;
    public static final String VERIFY_GROUP_SUBSCRIPTION_PREFIX_DOC = "If this is set, the group " +
        "coordinator will verify that the subscriptions are prefixed with the tenant.";

    public static final String STRAY_PARTITION_DELETION_ENABLE_CONFIG = CONFLUENT_PREFIX + "enable.stray.partition.deletion";
    public static final boolean STRAY_PARTITION_DELETION_ENABLE_DEFAULT = false;
    public static final String STRAY_PARTITION_DELETION_ENABLE_DOC = "Whether stray partition deletion is enabled";

    public static final String CRN_AUTHORITY_PREFIX = CONFLUENT_PREFIX + "authorizer.authority.";
    public static final String CRN_AUTHORITY_NAME_CONFIG = CRN_AUTHORITY_PREFIX + "name";
    public static final String CRN_AUTHORITY_NAME_DEFAULT = "";
    public static final String CRN_AUTHORITY_NAME_DOC = "The DNS name of the authority that this cluster"
        + "uses to authorize. This should be a name for the cluster hosting metadata topics.";

    public static final String EVENT_LOGGER_PREFIX = "event.logger.";
    public static final String AUDIT_PREFIX = "confluent.security.";
    public static final String AUDIT_EVENT_LOGGER_PREFIX = AUDIT_PREFIX + EVENT_LOGGER_PREFIX;
    public static final String AUDIT_EVENT_ROUTER_PREFIX = AUDIT_PREFIX + "event.router.";

    public static final String AUDIT_EVENT_ROUTER_CONFIG = AUDIT_EVENT_ROUTER_PREFIX + "config";
    public static final String AUDIT_EVENT_ROUTER_DEFAULT = "";
    public static final String AUDIT_EVENT_ROUTER_DOC = "JSON configuration for routing events to topics";

    public static final String AUDIT_LOGGER_ENABLE_CONFIG = AUDIT_EVENT_LOGGER_PREFIX + "enable";
    public static final String AUDIT_LOGGER_ENABLE_DEFAULT = "true";
    public static final String AUDIT_LOGGER_ENABLE_DOC = "Whether the event logger is enabled";


    public enum ClientType {
        PRODUCER("producer", ProducerConfig.configNames()),
        CONSUMER("consumer", ConsumerConfig.configNames()),
        ADMIN("admin", AdminClientConfig.configNames()),
        COORDINATOR("coordinator", ConsumerConfig.configNames());

        final String type;
        final Set<String> configNames;

        ClientType(String type, Set<String> configNames) {
            this.type = type;
            this.configNames = configNames;
        }
    }


    public static BrokerInterceptor buildBrokerInterceptor(Mode mode, Map<String, ?> configs) {
        if (mode == Mode.CLIENT)
            return null;

        BrokerInterceptor interceptor = new DefaultBrokerInterceptor();
        if (configs.containsKey(BROKER_INTERCEPTOR_CLASS_CONFIG)) {
            @SuppressWarnings("unchecked")
            Class<? extends BrokerInterceptor> interceptorClass =
                    (Class<? extends BrokerInterceptor>) configs.get(BROKER_INTERCEPTOR_CLASS_CONFIG);
            interceptor = Utils.newInstance(interceptorClass);
        }
        interceptor.configure(configs);
        return interceptor;
    }

    public static MultiTenantMetadata buildMultitenantMetadata(Map<String, ?> configs) {
        MultiTenantMetadata meta = null;
        if (configs.get(MULTITENANT_METADATA_CLASS_CONFIG) != null) {
            @SuppressWarnings("unchecked")
            Class<? extends MultiTenantMetadata> multitenantMetadataClass =
                (Class<? extends MultiTenantMetadata>) configs.get(MULTITENANT_METADATA_CLASS_CONFIG);
            meta = Utils.newInstance(multitenantMetadataClass);
            meta.configure(configs);
        }
        return meta;
    }

    public static LicenseValidator buildLicenseValidator(AbstractConfig config,
                                                         Endpoint interBrokerEndpoint) {
        LicenseValidator licenseValidator = null;
        ServiceLoader<LicenseValidator> validators = ServiceLoader.load(LicenseValidator.class);
        for (LicenseValidator validator : validators) {
            if (validator.enabled()) {
                licenseValidator = validator;
                break;
            }
        }
        if (licenseValidator == null) {
            throw new IllegalStateException("License validator not found");
        }
        licenseValidator.configure(interBrokerClientConfigs(config, interBrokerEndpoint));
        return licenseValidator;
    }

    /**
     * Returns inter-broker client configs that are used as default values for producers, consumers
     * and admin clients created by brokers. These can be overridden with prefixed configs if
     * required. The returned map also contains all other broker configs including any custom configs.
     */
    public static Map<String, Object> interBrokerClientConfigs(AbstractConfig brokerConfig,
                                                               Endpoint interBrokerEndpoint) {
        Map<String, Object> configs = brokerConfig.originals();
        Map<String, Object> clientConfigs = new HashMap<>(configs);

        // Remove broker configs that are not client configs. Using AdminClient config names for
        // filtering since they apply to producer/consumer as well.
        Set<String> brokerConfigNames = brokerConfig.values().keySet();
        clientConfigs.keySet().removeIf(n ->
            (brokerConfigNames.contains(n) && !AdminClientConfig.configNames().contains(n)) ||
                n.startsWith("listener.name."));

        ListenerName listenerName = new ListenerName(interBrokerEndpoint.listenerName().get());
        String listenerPrefix = listenerName.configPrefix();
        SecurityProtocol securityProtocol = interBrokerEndpoint.securityProtocol();
        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
            String saslMechanism = (String) brokerConfig.originals().get("sasl.mechanism.inter.broker.protocol");
            saslMechanism = saslMechanism != null ? saslMechanism : SaslConfigs.DEFAULT_SASL_MECHANISM;
            clientConfigs.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            String mechanismPrefix = listenerName.saslMechanismConfigPrefix(saslMechanism);
            updatePrefixedConfigs(configs, clientConfigs, mechanismPrefix);

            // If broker is configured with static JAAS config, set client sasl.jaas.config
            if (!clientConfigs.containsKey(SaslConfigs.SASL_JAAS_CONFIG)) {
                String jaasConfig = JaasContext.listenerSaslJaasConfig(listenerName, saslMechanism);
                clientConfigs.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
            }
        }
        updatePrefixedConfigs(configs, clientConfigs, listenerPrefix);
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, interBrokerEndpoint.host() + ":" + interBrokerEndpoint.port());
        clientConfigs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
        return clientConfigs;
    }

    /**
     * Client configs are derived from the provided `config` in the following order of precedence:
     * <ul>
     *   <li>configPrefix.clientPrefix.configName</li>
     *   <li>configPrefix.configName</li>
     *   <li>configName</li>
     * </ul>
     *
     * Metrics reporters are defined only if configured with a prefix to avoid broker's metrics
     * reporter being used when not required. Configs are not filtered out by client type since we
     * want to retain custom configs.
     */
    public static Map<String, Object> clientConfigs(AbstractConfig config,
                                                    String configPrefix,
                                                    ClientType clientType,
                                                    String topicPrefix,
                                                    String componentId) {
        // Process all configs from originals except the config names defined in `config`
        // since they are not client configs (e.g. license store/metadata store configs)
        Map<String, Object> srcConfigs = config.originals();
        srcConfigs.keySet().removeAll(config.values().keySet());

        Map<String, Object> clientConfigs = new HashMap<>(srcConfigs);
        clientConfigs.remove(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG);
        clientConfigs.put(CommonClientConfigs.CLIENT_ID_CONFIG,
            String.format("%s-%s-%s", topicPrefix, clientType.type, componentId));

        updatePrefixedConfigs(srcConfigs, clientConfigs, configPrefix + clientType.type + ".");
        updatePrefixedConfigs(srcConfigs, clientConfigs, configPrefix);
        return clientConfigs;
    }

    /**
     * Copy configs starting with `prefix` from `configs` to `dstConfigs` without the prefix.
     * Prefixed configs that are processed are removed from `configs` and `dstConfigs`.
     */
    private static void updatePrefixedConfigs(Map<String, Object> configs,
                                              Map<String, Object> dstConfigs,
                                              String prefix) {
        Set<String> prefixed = configs.keySet().stream()
            .filter(n -> n.startsWith(prefix))
            .collect(Collectors.toSet());

        prefixed.forEach(name -> {
            dstConfigs.remove(name);
            dstConfigs.put(name.substring(prefix.length()), configs.get(name));
        });
        configs.keySet().removeAll(prefixed);
    }
}
