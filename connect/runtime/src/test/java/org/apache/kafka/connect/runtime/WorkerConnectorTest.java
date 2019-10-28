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

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(EasyMockRunner.class)
public class WorkerConnectorTest extends EasyMockSupport {

    private static final String VERSION = "1.1";
    public static final String CONNECTOR = "connector";
    public static final Map<String, String> CONFIG = new HashMap<>();
    static {
        CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestConnector.class.getName());
        CONFIG.put(ConnectorConfig.NAME_CONFIG, CONNECTOR);
    }
    public ConnectorConfig connectorConfig;
    public MockConnectMetrics metrics;

    @Mock Plugins plugins;
    @Mock Connector connector;
    @Mock ConnectorContext ctx;
    @Mock ConnectorStatus.Listener listener;
    @Mock Herder herder;

    @Before
    public void setup() {
        connectorConfig = new ConnectorConfig(plugins, CONFIG);
        metrics = new MockConnectMetrics();
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }

    @Test
    public void testInitializeFailure() {
        RuntimeException exception = new RuntimeException();

        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall().andThrow(exception);

        listener.onFailure(CONNECTOR, exception);
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, metrics, listener);

        workerConnector.initialize(connectorConfig);
        assertFailedMetric(workerConnector);
        workerConnector.shutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testFailureIsFinalState() {
        RuntimeException exception = new RuntimeException();

        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall().andThrow(exception);

        listener.onFailure(CONNECTOR, exception);
        expectLastCall();

        // expect no call to onStartup() after failure

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, metrics, listener);

        workerConnector.initialize(connectorConfig);
        assertFailedMetric(workerConnector);
        workerConnector.transitionTo(TargetState.STARTED);
        assertFailedMetric(workerConnector);
        workerConnector.shutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testStartupAndShutdown() {
        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        listener.onStartup(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, metrics, listener);

        workerConnector.initialize(connectorConfig);
        assertInitializedMetric(workerConnector);
        workerConnector.transitionTo(TargetState.STARTED);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testStartupAndPause() {
        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        listener.onStartup(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall();

        listener.onPause(CONNECTOR);
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, metrics, listener);

        workerConnector.initialize(connectorConfig);
        assertInitializedMetric(workerConnector);
        workerConnector.transitionTo(TargetState.STARTED);
        assertRunningMetric(workerConnector);
        workerConnector.transitionTo(TargetState.PAUSED);
        assertPausedMetric(workerConnector);
        workerConnector.shutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testOnResume() {
        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        listener.onPause(CONNECTOR);
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        listener.onResume(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, metrics, listener);

        workerConnector.initialize(connectorConfig);
        assertInitializedMetric(workerConnector);
        workerConnector.transitionTo(TargetState.PAUSED);
        assertPausedMetric(workerConnector);
        workerConnector.transitionTo(TargetState.STARTED);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testStartupPaused() {
        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        // connector never gets started

        listener.onPause(CONNECTOR);
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, metrics, listener);

        workerConnector.initialize(connectorConfig);
        assertInitializedMetric(workerConnector);
        workerConnector.transitionTo(TargetState.PAUSED);
        assertPausedMetric(workerConnector);
        workerConnector.shutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testStartupFailure() {
        RuntimeException exception = new RuntimeException();

        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall().andThrow(exception);

        listener.onFailure(CONNECTOR, exception);
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, metrics, listener);

        workerConnector.initialize(connectorConfig);
        assertInitializedMetric(workerConnector);
        workerConnector.transitionTo(TargetState.STARTED);
        assertFailedMetric(workerConnector);
        workerConnector.shutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testShutdownFailure() {
        RuntimeException exception = new RuntimeException();

        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        listener.onStartup(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall().andThrow(exception);

        listener.onFailure(CONNECTOR, exception);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, metrics, listener);

        workerConnector.initialize(connectorConfig);
        assertInitializedMetric(workerConnector);
        workerConnector.transitionTo(TargetState.STARTED);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        assertFailedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testConnectorTaskMetrics() {
        Long expectedRunning = 1L;
        Long expectedPaused = 2L;
        Long expectedFailed = 3L;
        Long expectedUnassigned = 4L;
        Long expectedDestroyed = 5L;
        Long expectedTotal = 15L;
        List<ConnectorStateInfo.TaskState> tasks = new ArrayList<>();
        for (int i = 0; i < expectedRunning; i++) {
            tasks.add(new ConnectorStateInfo.TaskState(0, "RUNNING", "worker", "msg"));
        }
        for (int i = 0; i < expectedPaused; i++) {
            tasks.add(new ConnectorStateInfo.TaskState(0, "PAUSED", "worker", "msg"));
        }
        for (int i = 0; i < expectedFailed; i++) {
            tasks.add(new ConnectorStateInfo.TaskState(0, "FAILED", "worker", "msg"));
        }
        for (int i = 0; i < expectedUnassigned; i++) {
            tasks.add(new ConnectorStateInfo.TaskState(0, "UNASSIGNED", "worker", "msg"));
        }
        for (int i = 0; i < expectedDestroyed; i++) {
            tasks.add(new ConnectorStateInfo.TaskState(0, "DESTROYED", "worker", "msg"));
        }

        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        herder.connectorStatus(CONNECTOR);
        expectLastCall().andReturn(new ConnectorStateInfo(
            CONNECTOR,
            new ConnectorStateInfo.ConnectorState("RUNNING", "worker", "msg"),
            tasks,
            ConnectorType.SINK
        )).times(1);

        replayAll();

        WorkerConnector workerConnector =
            new WorkerConnector(CONNECTOR, connector, ctx, metrics, listener);
        workerConnector.metrics().addHerderMetrics(herder);

        workerConnector.initialize(connectorConfig);
        Long taskCount = (Long) metrics.currentMetricValue(workerConnector.metrics().metricGroup(),
            "connector-total-task-count");
        assertEquals(expectedTotal, taskCount);

        verifyAll();
    }

    @Test
    public void testTransitionStartedToStarted() {
        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        // expect only one call to onStartup()
        listener.onStartup(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, metrics, listener);

        workerConnector.initialize(connectorConfig);
        assertInitializedMetric(workerConnector);
        workerConnector.transitionTo(TargetState.STARTED);
        assertRunningMetric(workerConnector);
        workerConnector.transitionTo(TargetState.STARTED);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testTransitionPausedToPaused() {
        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        listener.onStartup(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall();

        listener.onPause(CONNECTOR);
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, metrics, listener);

        workerConnector.initialize(connectorConfig);
        assertInitializedMetric(workerConnector);
        workerConnector.transitionTo(TargetState.STARTED);
        assertRunningMetric(workerConnector);
        workerConnector.transitionTo(TargetState.PAUSED);
        assertPausedMetric(workerConnector);
        workerConnector.transitionTo(TargetState.PAUSED);
        assertPausedMetric(workerConnector);
        workerConnector.shutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    protected void assertFailedMetric(WorkerConnector workerConnector) {
        assertFalse(workerConnector.metrics().isUnassigned());
        assertTrue(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
    }

    protected void assertPausedMetric(WorkerConnector workerConnector) {
        assertFalse(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertTrue(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
    }

    protected void assertRunningMetric(WorkerConnector workerConnector) {
        assertFalse(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isPaused());
        assertTrue(workerConnector.metrics().isRunning());
    }

    protected void assertStoppedMetric(WorkerConnector workerConnector) {
        assertTrue(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
    }

    protected void assertInitializedMetric(WorkerConnector workerConnector) {
        assertTrue(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
        MetricGroup metricGroup = workerConnector.metrics().metricGroup();
        String status = metrics.currentMetricValueAsString(metricGroup, "status");
        String type = metrics.currentMetricValueAsString(metricGroup, "connector-type");
        String clazz = metrics.currentMetricValueAsString(metricGroup, "connector-class");
        String version = metrics.currentMetricValueAsString(metricGroup, "connector-version");
        assertEquals(type, "unknown");
        assertNotNull(clazz);
        assertEquals(VERSION, version);
    }

    private static abstract class TestConnector extends Connector {
    }

}
