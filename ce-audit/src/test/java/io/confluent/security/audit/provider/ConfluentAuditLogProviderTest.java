/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.security.audit.provider;

import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.events.EventLogger;
import io.confluent.events.ProtobufEvent;
import io.confluent.events.exporter.LogExporter;
import io.confluent.events.exporter.kafka.KafkaExporter;
import io.confluent.security.audit.AuditLogConfig;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders;
import io.confluent.security.authorizer.provider.DefaultAuditLogProvider;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.apache.kafka.server.audit.AuditLogProvider;
import org.apache.kafka.server.audit.AuthenticationEventImpl;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import javax.security.sasl.SaslServer;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.internals.ConfluentConfigs.AUDIT_LOGGER_ENABLE_CONFIG;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.CRN_AUTHORITY_NAME_CONFIG;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.ENABLE_AUTHENTICATION_AUDIT_LOGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@SuppressWarnings("unchecked")
public class ConfluentAuditLogProviderTest {

  private Map<String, Object> configs = Utils.mkMap(
      Utils.mkEntry(
          AuditLogConfig.BOOTSTRAP_SERVERS_CONFIG,
          "localhost:9092"),
      Utils.mkEntry(AuditLogConfig.TOPIC_CREATE_CONFIG, "false"),
      Utils.mkEntry(CRN_AUTHORITY_NAME_CONFIG, "mds.example.com"));

  private ConfluentAuditLogProvider providerWithMockExporter(String clusterId,
                                                             Map<String, String> configOverrides) throws Exception {
    ConfluentAuditLogProvider provider = new ConfluentAuditLogProvider();
    configs.put(AuditLogConfig.EVENT_EXPORTER_CLASS_CONFIG, MockExporter.class.getName());
    configs.putAll(configOverrides);
    provider.configure(configs);
    provider.onUpdate(new ClusterResource(clusterId));
    CompletableFuture<Void> startFuture = provider
        .start(configs).toCompletableFuture();
    startFuture.get(10_000, TimeUnit.MILLISECONDS);
    return provider;
  }

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
            Collections.singletonMap(AUDIT_LOGGER_ENABLE_CONFIG, "false"));
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


  public static class MockMetricsReporterNeedsConfigs implements MetricsReporter {

    @Override
    public void init(List<KafkaMetric> metrics) {

    }

    @Override
    public void metricChange(KafkaMetric metric) {

    }

    @Override
    public void metricRemoval(KafkaMetric metric) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
      if (!configs.containsKey("confluent.metrics.reporter.bootstrap.servers")) {
        throw new ConfigException("no bootstrap servers");
      }
    }
  }

  @Test
  public void testAuditLogProviderConfigWithMetrics() throws Exception {
    Map<String, Object> metricsConfigs = new HashMap<>(configs);
    configs.put("metric.reporters", MockMetricsReporterNeedsConfigs.class.getName());
    configs.put("confluent.metrics.reporter.bootstrap.servers", "localhost:9092");
    configs.put("confluent.metrics.reporter.topic.replicas", "3");
    configs.put("confluent.support.metrics.enable", "true");

    AuditLogProvider provider = ConfluentBuiltInProviders.loadAuditLogProvider(metricsConfigs);
    provider.configure(metricsConfigs);
    assertEquals(ConfluentAuditLogProvider.class, provider.getClass());

    ConfluentAuditLogProvider confluentProvider = (ConfluentAuditLogProvider) provider;
    provider.start(configs).toCompletableFuture().get();
    verifyExecutorTerminated(confluentProvider);

    EventLogger eventLogger = ((ConfluentAuditLogProvider) provider).getEventLogger();
    assertNotNull(eventLogger);
    assertEquals(KafkaExporter.class, eventLogger.eventExporter().getClass());
  }

  @Test
  public void testAuthenticationEventWithDefaultDisabled() throws Throwable {
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap());

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
    SaslServer server = mock(SaslServer.class);
    AuthenticationContext authenticationContext = new SaslAuthenticationContext(server,
        SecurityProtocol.SASL_PLAINTEXT, InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_PLAINTEXT.name());

    provider.logEvent(new AuthenticationEventImpl(principal, authenticationContext, AuditEventStatus.SUCCESS));

    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();
    assertEquals(0, ma.events.size());

    provider.close();
  }

  @Test
  public void testAuthenticationEvent() throws Throwable {
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap(
            Utils.mkEntry(ENABLE_AUTHENTICATION_AUDIT_LOGS, "true")
        )
    );

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
    SaslServer server = mock(SaslServer.class);
    AuthenticationContext authenticationContext = new SaslAuthenticationContext(server,
        SecurityProtocol.SASL_PLAINTEXT, InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_PLAINTEXT.name());

    Map<String, Object> data = new HashMap<>();
    data.put("identifier", "id1");
    data.put("mechanism", "SASL");
    AuthenticationEventImpl authenticationEvent = new AuthenticationEventImpl(principal,
        authenticationContext, AuditEventStatus.SUCCESS);
    authenticationEvent.setData(data);
    provider.logEvent(authenticationEvent);

    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();
    assertEquals(1, ma.events.size());

    CloudEvent<AttributesImpl, AuditLogEntry> event = ma.events.get(0);

    // Attributes
    assertNotNull(event.getAttributes().getId());
    assertTrue(event.getAttributes().getTime().isPresent());
    assertEquals("crn://mds.example.com/kafka=63REM3VWREiYtMuVxZeplA",
        event.getAttributes().getSubject().get());
    assertEquals("crn://mds.example.com/kafka=63REM3VWREiYtMuVxZeplA",
        event.getAttributes().getSource().toString());
    assertEquals(ProtobufEvent.APPLICATION_JSON, event.getAttributes().getDatacontenttype().get());
    assertEquals("0.3", event.getAttributes().getSpecversion());
    assertEquals("io.confluent.kafka.server/authentication", event.getAttributes().getType());

    // Data
    assertTrue(event.getData().isPresent());
    AuditLogEntry ae = event.getData().get();
    assertEquals("crn://mds.example.com/kafka=63REM3VWREiYtMuVxZeplA", ae.getServiceName());
    assertEquals("crn://mds.example.com/kafka=63REM3VWREiYtMuVxZeplA", ae.getResourceName());
    assertEquals("kafka.Authentication", ae.getMethodName());
    assertEquals("User:user1", ae.getAuthenticationInfo().getPrincipal());
    assertEquals("id1", ae.getAuthenticationInfo().getMetadata().getIdentifier());
    assertEquals("SASL", ae.getAuthenticationInfo().getMetadata().getMechanism());
    assertEquals("SUCCESS", ae.getResult().getStatus());

    provider.close();
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
