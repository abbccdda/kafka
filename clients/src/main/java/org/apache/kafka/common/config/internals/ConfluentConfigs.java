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

import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.interceptor.BrokerInterceptor;
import org.apache.kafka.server.interceptor.DefaultBrokerInterceptor;
import org.apache.kafka.server.multitenant.MultiTenantMetadata;

import static org.apache.kafka.common.config.ConfluentTopicConfig.CONFLUENT_PREFIX;

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
}
