package io.confluent.security.audit.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.audit.EventLogger;
import io.confluent.security.audit.appender.KafkaEventAppender;
import io.confluent.security.audit.appender.LogEventAppender;
import io.confluent.security.authorizer.provider.AuditLogProvider;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders;
import io.confluent.security.authorizer.provider.DefaultAuditLogProvider;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class ConfluentAuditLogProviderTest {

  private Map<String, Object> configs = Collections.singletonMap(
      EventLogConfig.EVENT_LOGGER_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
      "localhost:9092");

  @Before
  public void setUp() {
    TestEventAppender.clear();
  }

  @Test
  public void testAuditLogProviderConfig() throws Exception {
    AuditLogProvider provider = ConfluentBuiltInProviders.loadAuditLogProvider(configs);
    provider.configure(configs);
    assertEquals(ConfluentAuditLogProvider.class, provider.getClass());

    ConfluentAuditLogProvider confluentProvider = (ConfluentAuditLogProvider) provider;
    provider.start(configs).toCompletableFuture().get();
    verifyExecutorTerminated(confluentProvider);

    EventLogger fileLogger = confluentProvider.localFileLogger();
    assertNotNull(fileLogger);
    assertEquals(LogEventAppender.class, TestUtils.fieldValue(fileLogger, EventLogger.class, "eventAppender").getClass());
    EventLogger kafkaLogger = confluentProvider.kafkaLogger();
    assertNotNull(kafkaLogger);
    assertEquals(KafkaEventAppender.class, TestUtils.fieldValue(kafkaLogger, EventLogger.class, "eventAppender").getClass());

    AuditLogProvider defaultProvider = ConfluentBuiltInProviders.loadAuditLogProvider(Collections.emptyMap());
    assertEquals(DefaultAuditLogProvider.class, defaultProvider.getClass());
  }

  @Test
  public void testAppenderConfiguration() throws Exception {
    ConfluentAuditLogProvider provider = createTestableProvider();

    CompletableFuture<Void> startFuture = startProvider(provider);
    TestEventAppender appender = TestEventAppender.instance;
    assertNotNull(TestEventAppender.instance);
    assertEquals(TestEventAppender.State.STARTING, appender.state);
    assertFalse(startFuture.isDone());
    assertFalse(provider.initExecutor().isTerminated());
    TestEventAppender.instance.semaphore.release();
    startFuture.get(10, TimeUnit.SECONDS);
    assertEquals(TestEventAppender.State.STARTED, appender.state);
    verifyExecutorTerminated(provider);

    provider.close();
    assertEquals(TestEventAppender.State.CLOSED, appender.state);
  }

  @Test
  public void testAppenderConfigurationFailure() throws Exception {
    TestEventAppender.configureException = new RuntimeException("Test exception");
    ConfluentAuditLogProvider provider = createTestableProvider();

    CompletableFuture<Void> startFuture = startProvider(provider);
    ExecutionException exception = assertThrows(ExecutionException.class,
        () -> startFuture.get(10, TimeUnit.SECONDS));
    assertEquals(TestEventAppender.configureException, exception.getCause());

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
    ConfluentAuditLogProvider provider = new ConfluentAuditLogProvider() {
      protected EventLogger eventLogger(Map<String, Object> configs) {
        if (KafkaEventAppender.class.getName()
            .equals(configs.get(EventLogConfig.EVENT_LOGGER_CLASS_CONFIG)))
          configs.put(EventLogConfig.EVENT_LOGGER_CLASS_CONFIG, TestEventAppender.class.getName());
        return EventLogger.createLogger(configs);
      }
    };

    provider.configure(configs);
    assertNull(TestEventAppender.instance);
    return provider;
  }

  private CompletableFuture<Void> startProvider(ConfluentAuditLogProvider provider) throws Exception {
    CompletableFuture<Void> startFuture = provider.start(configs).toCompletableFuture();
    TestUtils.waitForCondition(() -> TestEventAppender.instance != null, "Event appender not created");
    return startFuture;
  }

  private void verifyExecutorTerminated(ConfluentAuditLogProvider provider) throws Exception {
    TestUtils.waitForCondition(() -> provider.initExecutor().isTerminated(), "Executor not terminated");
  }

  public static class TestEventAppender extends LogEventAppender {

    enum State {
      INITIALIZED,
      STARTING,
      STARTED,
      CLOSED
    }

    static TestEventAppender instance;
    static RuntimeException configureException;

    State state = State.INITIALIZED;
    Semaphore semaphore = new Semaphore(0);

    static void clear() {
      instance = null;
      configureException = null;
    }

    public TestEventAppender() {
      instance = this;
    }

    @Override
    public void configure(Map<String, ?> configs) {
      state = State.STARTING;
      if (configureException != null)
        throw configureException;
      try {
        semaphore.acquire();
      } catch (InterruptedException  e) {
        throw new RuntimeException(e);
      }
      super.configure(configs);
      state = State.STARTED;
    }

    @Override
    public void close() {
      state = State.CLOSED;
      super.close();
    }
  }
}
