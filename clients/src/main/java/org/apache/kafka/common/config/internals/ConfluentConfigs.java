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

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ConfluentConfigs {
    public static final String BROKER_INTERCEPTOR_CLASS_CONFIG = "broker.interceptor.class";
    public static final Class<?> BROKER_INTERCEPTOR_CLASS_DEFAULT = DefaultBrokerInterceptor.class;
    public static final String MULTITENANT_METADATA_CLASS_CONFIG = "multitenant.metadata.class";
    public static final String MULTITENANT_METADATA_CLASS_DEFAULT = null;
    public static final String MULTITENANT_METADATA_DIR_CONFIG = "multitenant.metadata.dir";
    public static final String MULTITENANT_METADATA_DIR_DEFAULT = null;
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
