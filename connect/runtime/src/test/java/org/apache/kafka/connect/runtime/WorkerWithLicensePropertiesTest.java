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
package org.apache.kafka.connect.runtime;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.MockConnectMetrics.MockMetricsReporter;
import org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.Plugins.ClassLoaderUsage;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.ThreadedTest;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.api.easymock.annotation.MockStrict;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.easymock.EasyMock.anyObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Worker.class, Plugins.class, ConnectUtils.class})
@PowerMockIgnore("javax.management.*")
public class WorkerWithLicensePropertiesTest extends ThreadedTest {

    private static final String CONNECTOR_ID = "test-connector";
    private static final String WORKER_ID = "localhost:8083";
    private static final String PRODUCER_SSL_CONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${CCLOUD_API_KEY}\" password=\"${CCLOUD_API_SECRET}\"";

    private final ConnectorClientConfigOverridePolicy noneConnectorClientConfigOverridePolicy = new AllConnectorClientConfigOverridePolicy();

    private Map<String, String> workerProps = new HashMap<>();
    private WorkerConfig config;
    private Worker worker;

    @Mock private Plugins plugins;
    @Mock private PluginClassLoader pluginLoader;
    @Mock private DelegatingClassLoader delegatingLoader;
    @Mock private OffsetBackingStore offsetBackingStore;
    @Mock private StatusBackingStore statusBackingStore;
    @MockStrict private ConnectorStatus.Listener connectorStatusListener;

    @Mock private Herder herder;
    @Mock private ConnectorContext ctx;

    @Before
    public void setup() {
        super.setup();
        workerProps.put("bootstrap.servers", "localhost:8083");
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerProps.put("sasl.jaas.config", PRODUCER_SSL_CONFIG);
        workerProps.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        config = new StandaloneConfig(workerProps);

        PowerMock.mockStatic(Plugins.class);
    }

    @Test
    public void testWorkerInjectsLicensePropertiesIntoLicensedConnector() {
        LicensedTestConnector connector = new LicensedTestConnector();
        expectConverters();
        expectStartStorage();
        expectClusterId();

        // Create
        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader);
        EasyMock.expect(plugins.newConnector(LicensedTestConnector.class.getName())).andReturn(connector);

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, LicensedTestConnector.class.getName());

        EasyMock.expect(plugins.compareAndSwapLoaders(connector))
                .andReturn(delegatingLoader);
        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(pluginLoader);

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertEquals(Collections.emptySet(), worker.connectorNames());
        worker.startConnector(CONNECTOR_ID, props, ctx, connectorStatusListener, TargetState.STARTED);
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());

        // Ensure the connector saw the license properties
        assertTrue(connector.licenseCheckPassed());

        PowerMock.verifyAll();
    }

    @Test
    public void testStartLicenseConnectorWithInvalidInjectedProperties() {
        UnlicensedTestConnector connector = new UnlicensedTestConnector();
        expectConverters();
        expectStartStorage();
        expectClusterId();

        // Create
        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader);
        EasyMock.expect(plugins.newConnector(LicensedTestConnector.class.getName())).andReturn(connector);

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, LicensedTestConnector.class.getName());

        EasyMock.expect(plugins.compareAndSwapLoaders(connector))
                .andReturn(delegatingLoader);
        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(pluginLoader);

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertEquals(Collections.emptySet(), worker.connectorNames());
        worker.startConnector(CONNECTOR_ID, props, ctx, connectorStatusListener, TargetState.STARTED);
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());

        // Ensure the connector saw the license properties
        assertFalse(connector.foundLicenseProperties());

        PowerMock.verifyAll();
    }

    private void expectClusterId() {
        PowerMock.mockStaticPartial(ConnectUtils.class, "lookupKafkaClusterId");
        EasyMock.expect(ConnectUtils.lookupKafkaClusterId(EasyMock.anyObject())).andReturn("test-cluster").anyTimes();
    }

    private void expectStartStorage() {
        offsetBackingStore.configure(anyObject(WorkerConfig.class));
        EasyMock.expectLastCall();
        offsetBackingStore.start();
        EasyMock.expectLastCall();
        EasyMock.expect(herder.statusBackingStore())
                .andReturn(statusBackingStore).anyTimes();
    }

    private void expectConverters() {
        expectConverters(JsonConverter.class);
    }

    @SuppressWarnings("deprecation")
    private void expectConverters(Class<? extends Converter> converterClass) {
        //internal
        Converter internalKeyConverter = PowerMock.createMock(converterClass);
        Converter internalValueConverter = PowerMock.createMock(converterClass);

        // Instantiate and configure internal
        EasyMock.expect(
                plugins.newConverter(
                        config,
                        WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG,
                        ClassLoaderUsage.PLUGINS
                )
        ).andReturn(internalKeyConverter);
        EasyMock.expect(
                plugins.newConverter(
                        config,
                        WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG,
                        ClassLoaderUsage.PLUGINS
                )
        ).andReturn(internalValueConverter);
        EasyMock.expectLastCall();
    }

    /* Name here needs to be unique as we are testing the aliasing mechanism */
    public static class LicensedTestConnector extends SourceConnector {

        private static final ConfigDef CONFIG_DEF  = new ConfigDef()
                .define("confluent.topic.bootstrap.servers", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "")
                .define("confluent.topic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "");

        private boolean licenseCheckPassed = false;

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void start(Map<String, String> props) {
            licenseCheckPassed = props.containsKey("confluent.topic.bootstrap.servers") &&
                                 props.containsKey("confluent.topic") &&
                                 props.containsKey("confluent.topic.replication.factor") &&
                                 props.containsKey("confluent.topic.sasl.jaas.config");
        }

        @Override
        public Class<? extends Task> taskClass() {
            return null;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            return null;
        }

        @Override
        public void stop() {

        }

        @Override
        public ConfigDef config() {
            return CONFIG_DEF;
        }

        public boolean licenseCheckPassed() {
            return licenseCheckPassed;
        }
    }


    /* Name here needs to be unique as we are testing the aliasing mechanism */
    public static class UnlicensedTestConnector extends SourceConnector {

        private static final ConfigDef CONFIG_DEF  = new ConfigDef()
                .define("foot", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "");

        private boolean foundLicenseProperties = false;

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void start(Map<String, String> props) {
            foundLicenseProperties = props.keySet().stream().anyMatch(k -> k.startsWith("confluent."));
        }

        @Override
        public Class<? extends Task> taskClass() {
            return null;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            return null;
        }

        @Override
        public void stop() {

        }

        @Override
        public ConfigDef config() {
            return CONFIG_DEF;
        }

        public boolean foundLicenseProperties() {
            return foundLicenseProperties;
        }
    }
}
