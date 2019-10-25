/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.security.audit.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import io.confluent.crn.CrnAuthorityConfig;
import io.confluent.events.EventLogger;
import io.confluent.events.exporter.LogExporter;
import io.confluent.events.exporter.kafka.KafkaExporter;
import io.confluent.security.audit.AuditLogConfig;
import io.confluent.security.audit.AuditLogRouterJsonConfigUtils;
import io.confluent.security.authorizer.provider.AuditLogProvider;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders;
import io.confluent.security.authorizer.provider.DefaultAuditLogProvider;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class ConfluentAuditLogProviderTest {

  private Map<String, Object> configs = Utils.mkMap(
      Utils.mkEntry(
          AuditLogConfig.BOOTSTRAP_SERVERS_CONFIG,
          "localhost:9092"),
      Utils.mkEntry(AuditLogConfig.TOPIC_CREATE_CONFIG, "false"),
      Utils.mkEntry(CrnAuthorityConfig.AUTHORITY_NAME_PROP, "mds.example.com"),
      Utils.mkEntry(
          AuditLogConfig.ROUTER_CONFIG,
          AuditLogRouterJsonConfigUtils.defaultConfig("localhost:9092", "", "")));

  @Before
  public void setUp() {
    TestExporter.clear();
  }

  @Test
  public void testAuditLogProviderConfig() throws Exception {
    AuditLogProvider provider = ConfluentBuiltInProviders.loadAuditLogProvider(configs);
    provider.configure(configs);
    assertEquals(ConfluentAuditLogProvider.class, provider.getClass());

    ConfluentAuditLogProvider confluentProvider = (ConfluentAuditLogProvider) provider;
    provider.start(configs).toCompletableFuture().get();
    verifyExecutorTerminated(confluentProvider);

    EventLogger eventLogger = ((ConfluentAuditLogProvider) provider).getEventLogger();
    assertNotNull(eventLogger);
    assertEquals(KafkaExporter.class, eventLogger.eventExporter().getClass());

    AuditLogProvider defaultProvider = ConfluentBuiltInProviders
        .loadAuditLogProvider(
            Collections.singletonMap(AuditLogConfig.AUDIT_LOGGER_ENABLED_CONFIG, "false"));
    assertEquals(DefaultAuditLogProvider.class, defaultProvider.getClass());
  }

  @Test
  public void testExporterConfiguration() throws Exception {
    ConfluentAuditLogProvider provider = createTestableProvider();

    CompletableFuture<Void> startFuture = startProvider(provider);
    TestExporter exporter = TestExporter.instance;
    assertNotNull(TestExporter.instance);
    assertEquals(TestExporter.State.STARTING, exporter.state);
    assertFalse(startFuture.isDone());
    assertFalse(provider.initExecutor().isTerminated());
    TestExporter.instance.semaphore.release();
    startFuture.get(10, TimeUnit.SECONDS);
    assertEquals(TestExporter.State.STARTED, exporter.state);
    verifyExecutorTerminated(provider);

    provider.close();
    assertEquals(TestExporter.State.CLOSED, exporter.state);
  }

  @Test
  public void testExporterConfigurationFailure() throws Exception {
    TestExporter.configureException = new RuntimeException("Test exception");
    ConfluentAuditLogProvider provider = createTestableProvider();

    CompletableFuture<Void> startFuture = startProvider(provider);
    ExecutionException exception = assertThrows(ExecutionException.class,
        () -> startFuture.get(10, TimeUnit.SECONDS));
    assertEquals(TestExporter.configureException, exception.getCause());

    verifyExecutorTerminated(provider);
  }

  @Test
  public void testCloseBeforeInitializationComplete() throws Exception {
    ConfluentAuditLogProvider provider = createTestableProvider();

    CompletableFuture<Void> startFuture = startProvider(provider);
    assertFalse(startFuture.isDone());
    assertFalse(provider.initExecutor().isTerminated());

    provider.close();
    verifyExecutorTerminated(provider);
  }

  private ConfluentAuditLogProvider createTestableProvider() {
    ConfluentAuditLogProvider provider = new ConfluentAuditLogProvider();
    configs.put(AuditLogConfig.EVENT_EXPORTER_CLASS_CONFIG, TestExporter.class.getName());

    provider.configure(configs);
    assertNull(TestExporter.instance);
    return provider;
  }

  private CompletableFuture<Void> startProvider(ConfluentAuditLogProvider provider)
      throws Exception {
    CompletableFuture<Void> startFuture = provider.start(configs).toCompletableFuture();
    TestUtils
        .waitForCondition(() -> TestExporter.instance != null, "Event exporter not created");
    return startFuture;
  }

  private void verifyExecutorTerminated(ConfluentAuditLogProvider provider) throws Exception {
    TestUtils
        .waitForCondition(() -> provider.initExecutor().isTerminated(), "Executor not terminated");
  }

  public static class TestExporter extends LogExporter {

    enum State {
      INITIALIZED,
      STARTING,
      STARTED,
      CLOSED
    }

    static TestExporter instance;
    static RuntimeException configureException;

    State state = State.INITIALIZED;
    Semaphore semaphore = new Semaphore(0);

    static void clear() {
      instance = null;
      configureException = null;
    }

    public TestExporter() {
      instance = this;
    }

    @Override
    public void configure(Map<String, ?> configs) {
      state = State.STARTING;
      if (configureException != null) {
        throw configureException;
      }
      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      super.configure(configs);
      state = State.STARTED;
    }

    @Override
    public void close() throws Exception {
      state = State.CLOSED;
      super.close();
    }
  }
}
