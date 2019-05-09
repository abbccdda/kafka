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
package org.apache.kafka.connect.runtime.rest;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.easymock.EasyMock;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.MockStrict;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertNotNull;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.net.ssl.*", "javax.security.*"})
public class ConfluentRestServerTest {

  @MockStrict
  private Herder herder;
  @MockStrict
  private Plugins plugins;
  private RestServer server;

  protected static final String KAFKA_CLUSTER_ID = "Xbafgnagvar";

  @After
  public void tearDown() {
    server.stop();
  }

  @SuppressWarnings("deprecation")
  private Map<String, String> baseWorkerProps() {
    Map<String, String> workerProps = new HashMap<>();
    workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "status-topic");
    workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "config-topic");
    workerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    workerProps.put(DistributedConfig.GROUP_ID_CONFIG, "connect-test-group");
    workerProps.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
    workerProps.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
    workerProps.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
    workerProps.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
    workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");

    return workerProps;
  }

  @Test
  public void testRestServletInitializers() {
    TestServletInitializer.clearContext();

    Map<String, String> configMap = baseWorkerProps();
    configMap.put(
        WorkerConfig.REST_SERVLET_INITIALIZERS_CLASSES_CONFIG,
        TestServletInitializer.class.getName()
    );
    DistributedConfig config = new DistributedConfig(configMap);

    EasyMock.expect(herder.plugins()).andReturn(plugins);
    EasyMock.expect(herder.kafkaClusterId()).andReturn(KAFKA_CLUSTER_ID);
    EasyMock.expect(plugins.newPlugins(
        EasyMock.eq(Collections.emptyList()),
        EasyMock.eq(config),
        EasyMock.eq(ConnectRestExtension.class)
    )).andReturn(Collections.emptyList());

    EasyMock.expect(herder.plugins())
            .andReturn(plugins);

    PowerMock.replayAll();

    server = new RestServer(config);
    server.initializeServer();
    server.initializeResources(herder);

    assertNotNull(TestServletInitializer.context());
  }

  @Test(expected = ConfigException.class)
  public void testBadRestServletInitializer() {
    Map<String, String> configMap = baseWorkerProps();
    configMap.put(
        WorkerConfig.REST_SERVLET_INITIALIZERS_CLASSES_CONFIG,
        BadServletInitializer.class.getName()
    );
    DistributedConfig config = new DistributedConfig(configMap);

    EasyMock.expect(herder.plugins()).andReturn(plugins);
    EasyMock.expect(herder.kafkaClusterId()).andReturn(KAFKA_CLUSTER_ID);
    EasyMock.expect(plugins.newPlugins(
        EasyMock.eq(Collections.emptyList()),
        EasyMock.eq(config),
        EasyMock.eq(ConnectRestExtension.class)
    )).andReturn(Collections.emptyList());

    EasyMock.expect(herder.plugins())
            .andReturn(plugins);

    PowerMock.replayAll();

    server = new RestServer(config);
    server.initializeServer();
    server.initializeResources(herder);
  }

  public static class TestServletInitializer implements Consumer<ServletContextHandler> {
    private static ServletContextHandler context = null;

    @Override
    public void accept(ServletContextHandler context) {
      TestServletInitializer.context = context;
    }

    public static ServletContextHandler context() {
      return context;
    }

    public static void clearContext() {
      TestServletInitializer.context = null;
    }
  }

  public static class BadServletInitializer implements Consumer<ServletContextHandler> {

    @Override
    public void accept(ServletContextHandler context) {
      throw new RuntimeException();
    }
  }
}
