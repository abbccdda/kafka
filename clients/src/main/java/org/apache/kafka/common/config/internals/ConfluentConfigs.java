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

import java.util.HashMap;
import java.util.ServiceLoader;
import org.apache.kafka.clients.CommonClientConfigs;
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
import org.apache.kafka.server.rest.RestServer;

import java.util.Map;
import java.util.concurrent.TimeUnit;


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
    private static final String KEY_SUBJECT_NAME_STRATEGY = "key.subject.name.strategy";
    private static final String VALUE_SUBJECT_NAME_STRATEGY = "value.subject.name.strategy";

    public static final String SCHEMA_REGISTRY_URL_CONFIG = CONFLUENT_PREFIX + SCHEMA_REGISTRY_URL;
    public static final String SCHEMA_REGISTRY_URL_DOC =
        "Comma-separated list of URLs for schema registry instances that can be used to "
            + "look up schemas.";

    // default TopicNameStrategy.class cannot be defined here, but should be in the plugin that can
    // depend on schema.registry
    public static final String KEY_SUBJECT_NAME_STRATEGY_CONFIG = CONFLUENT_PREFIX + KEY_SUBJECT_NAME_STRATEGY;
    public static final String KEY_SUBJECT_NAME_STRATEGY_DOC =
        "Determines how to construct the subject name under which the key schema is registered "
            + "with the schema registry. By default, <topic>-key is used as subject.";


    public static final String VALUE_SUBJECT_NAME_STRATEGY_CONFIG = CONFLUENT_PREFIX + VALUE_SUBJECT_NAME_STRATEGY;
    public static final String VALUE_SUBJECT_NAME_STRATEGY_DOC =
        "Determines how to construct the subject name under which the value schema is registered "
            + "with the schema registry. By default, <topic>-value is used as subject.";

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

    public static final String REST_SERVER_CLASS_CONFIG = CONFLUENT_PREFIX + "rest.server.class";
    public static final String REST_SERVER_CLASS_DOC = "The fully qualified name of a class that implements " + RestServer.class.getName()
        + " interface, which is used by the broker to start the Rest Server.";


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

    public static RestServer buildRestServer(AbstractConfig configs) {
        RestServer server = null;
        if (configs.getClass(REST_SERVER_CLASS_CONFIG) != null) {
            @SuppressWarnings("unchecked")
            Class<? extends RestServer> restServerClass =
                (Class<? extends RestServer>) configs.getClass(REST_SERVER_CLASS_CONFIG);
            server = Utils.newInstance(restServerClass);
            server.configure(configs.originals());
        }
        return server;
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

    public static Map<String, Object> interBrokerClientConfigs(AbstractConfig brokerConfig, Endpoint interBrokerEndpoint) {
        Map<String, Object>  clientConfigs = new HashMap<>();
        ListenerName listenerName = new ListenerName(interBrokerEndpoint.listenerName().get());
        String listenerPrefix = listenerName.configPrefix();
        Map<String, Object> configs = brokerConfig.originals();
        clientConfigs.putAll(configs);
        updatePrefixedConfigs(configs, clientConfigs, listenerPrefix);

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
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, interBrokerEndpoint.host() + ":" + interBrokerEndpoint.port());
        clientConfigs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
        // Broker id in client configs causes issues in metrics reporter, so don't include.
        clientConfigs.remove("broker.id");
        return clientConfigs;
    }

    private static void updatePrefixedConfigs(Map<String, Object> srcConfigs, Map<String, Object> dstConfigs, String prefix) {
        srcConfigs.entrySet().stream()
            .filter(e -> e.getKey().startsWith(prefix))
            .forEach(e -> {
                dstConfigs.remove(e.getKey());
                dstConfigs.put(e.getKey().substring(prefix.length()), e.getValue());
            });
    }
}
