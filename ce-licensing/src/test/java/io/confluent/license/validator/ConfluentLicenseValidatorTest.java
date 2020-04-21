// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.license.validator;

import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.license.ExpiredLicenseException;
import io.confluent.license.InvalidLicenseException;
import io.confluent.license.License;
import io.confluent.license.LicenseChanged;
import io.confluent.license.LicenseChanged.Type;
import io.confluent.license.LicenseManager;
import io.confluent.license.LicenseManagerTest;
import io.confluent.license.LicenseStore;
import io.confluent.license.test.utils.LicenseTestUtils;
import io.confluent.license.validator.ConfluentLicenseValidator.KafkaLicenseStore;
import io.confluent.license.validator.ConfluentLicenseValidator.LicenseStatus;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({License.class})
@PowerMockIgnore("javax.management.*")
public class ConfluentLicenseValidatorTest {

  private final MockTime time = new MockTime();
  private ConfluentLicenseValidator licenseValidator;
  private MockLicenseStore licenseStore;
  private int numberOfLicenseValidationCalls;

  @Before
  public void setUp() throws Exception {
    LicenseTestUtils.injectPublicKey();
    LicenseManagerTest.setLicenseManagerScheduler(TestExecutorService::new);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (licenseValidator != null)
        licenseValidator.close();
    } finally {
      LicenseManagerTest.setLicenseManagerScheduler(Executors::newSingleThreadScheduledExecutor);
      TestExecutorService.reset();
    }
  }

  @Test
  public void testLicense() throws Exception  {
    String license = LicenseTestUtils.generateLicense();
    licenseValidator = newConfluentLicenseValidator(license, "");
    licenseValidator.start("broker1");
    assertTrue("Invalid license", licenseValidator.isLicenseValid());
    LicenseTestUtils.verifyLicenseMetric(ConfluentLicenseValidator.METRIC_GROUP, LicenseStatus.LICENSE_ACTIVE);
    TestUtils.waitForCondition(() -> numberOfLicenseValidationCalls > 2,  "Periodic license validation is not working.");
    TestUtils.waitForCondition(() -> licenseStore.registeredLicense != null, "License not registered");
    assertEquals(license, licenseStore.registeredLicense);
    assertEquals(1, licenseStore.startCount);
    verifyClose();
  }

  @Test(expected = InvalidLicenseException.class)
  public void testInvalidLicense() {
    newConfluentLicenseValidator("invalid", "").start("broker1");
  }

  @Test(expected = ExpiredLicenseException.class)
  public void testExpiredLicense() {
    String license = createExpiredLicense();
    licenseValidator = newConfluentLicenseValidator(license, "");
    licenseValidator.start("broker1");
  }

  @Test
  public void testVerifyLicense() throws Exception {
    long licensePeriodMs =  60 * 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    licenseValidator = newConfluentLicenseValidator(license, "");
    licenseValidator.start("broker1");
    assertTrue(licenseValidator.isLicenseValid());
    time.sleep(licensePeriodMs + 1000);
    licenseValidator.accept(new LicenseEvent(Type.EXPIRED));
    assertFalse(licenseValidator.isLicenseValid());
    LicenseTestUtils.verifyLicenseMetric(ConfluentLicenseValidator.METRIC_GROUP, LicenseStatus.LICENSE_EXPIRED);
  }

  @Test
  public void testConfiguredLicense() throws Exception {
    long licensePeriodMs =  60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    licenseValidator = newConfluentLicenseValidator("", license);
    licenseValidator.start("broker1");
    assertEquals(1, licenseStore.startCount);
    verifyLicenseValidator(licensePeriodMs);
  }

  @Test
  public void testStoredLicense() throws Exception {
    long licensePeriodMs =  60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    licenseValidator = newConfluentLicenseValidator("", license);
    licenseValidator.start("broker1");
    LicenseTestUtils.verifyLicenseMetric(ConfluentLicenseValidator.METRIC_GROUP, LicenseStatus.LICENSE_ACTIVE);
    assertNull(licenseStore.registeredLicense);
    assertEquals(1, licenseStore.startCount);
    verifyLicenseValidator(licensePeriodMs);
    verifyClose();
  }


  @Test(expected = ExpiredLicenseException.class)
  public void testExpiredStoredLicense() {
    String license = createExpiredLicense();
    licenseValidator = newConfluentLicenseValidator("", license);
    licenseValidator.start("broker1");
  }

  @Test
  public void testExpiredStoredLicenseValidConfiguredLicense() throws Exception {
    String license = LicenseTestUtils.generateLicense();
    licenseValidator = newConfluentLicenseValidator(license, createExpiredLicense());
    licenseValidator.start("broker1");
    LicenseTestUtils.verifyLicenseMetric(ConfluentLicenseValidator.METRIC_GROUP, LicenseStatus.LICENSE_ACTIVE);
    TestUtils.waitForCondition(() -> licenseStore.registeredLicense != null, "License not registered");
    assertEquals(license, licenseStore.registeredLicense);
  }

  @Test
  public void testValidStoredLicenseExpiredConfiguredLicense() {
    String license = LicenseTestUtils.generateLicense();
    licenseValidator = newConfluentLicenseValidator(createExpiredLicense(), license);
    licenseValidator.start("broker1");
    LicenseTestUtils.verifyLicenseMetric(ConfluentLicenseValidator.METRIC_GROUP, LicenseStatus.LICENSE_ACTIVE);
    assertNull(licenseStore.registeredLicense);
  }

  @Test
  public void testConfiguredLicenseWithFailedStore() throws Exception {
    long licensePeriodMs = 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    licenseValidator = newConfluentLicenseValidator(license, "");
    licenseStore.startException = new RuntimeException("Test exception");
    licenseValidator.start("broker1");
    waitForStoreStart(false);
    assertNull(licenseStore.registeredLicense);
    verifyLicenseValidator(licensePeriodMs);
  }

  @Test
  public void testNoConfiguredLicenseWithFailedStore() throws Exception {
    licenseValidator = newConfluentLicenseValidator("", "");
    licenseStore.startException = new RuntimeException("Test exception");
    assertThrows(RuntimeException.class, () -> licenseValidator.start("broker1"));
  }

  @Test(timeout = 10000)
  public void testConfiguredLicenseWithDelayedStore() throws Exception {
    String license = LicenseTestUtils.generateLicense();
    Semaphore semaphore = new Semaphore(0);
    licenseValidator = newConfluentLicenseValidator(license, "");
    licenseStore.delayStart(semaphore);
    licenseValidator.start("broker1");
    waitForStoreStart(false);
    LicenseTestUtils.verifyLicenseMetric(ConfluentLicenseValidator.METRIC_GROUP, LicenseStatus.LICENSE_ACTIVE);
    semaphore.release();
    waitForStoreStart(true);
    TestUtils.waitForCondition(() -> licenseStore.registeredLicense != null, "License not registered");
    assertEquals(license, licenseStore.registeredLicense);
    LicenseTestUtils.verifyLicenseMetric(ConfluentLicenseValidator.METRIC_GROUP, LicenseStatus.LICENSE_ACTIVE);
  }

  @Test(timeout = 10000)
  public void testConfiguredLicenseExpiredWithDelayedStoreValidLicense() throws Exception {
    String configuredLicense = createExpiredLicense();
    String storedLicense = LicenseTestUtils.generateLicense();
    Semaphore semaphore = new Semaphore(0);
    licenseValidator = newConfluentLicenseValidator(configuredLicense, storedLicense);
    licenseStore.delayStart(semaphore);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> startFuture = executor.submit(() -> licenseValidator.start("broker1"));
      waitForStoreStart(false);
      assertFalse(startFuture.isDone());
      semaphore.release();
      startFuture.get(10, TimeUnit.SECONDS);
      LicenseTestUtils.verifyLicenseMetric(ConfluentLicenseValidator.METRIC_GROUP, LicenseStatus.LICENSE_ACTIVE);
      assertNull(licenseStore.registeredLicense);
    } finally {
       executor.shutdownNow();
    }
  }

  @Test(timeout = 10000)
  public void testLicenseManagerShutdownDuringStoreStart() throws Exception {
    String license = LicenseTestUtils.generateLicense();
    Semaphore semaphore = new Semaphore(0);
    licenseValidator = newConfluentLicenseValidator(license, "");
    licenseStore.delayStart(semaphore);
    licenseValidator.start("broker1");
    waitForStoreStart(false);
    verifyClose();
  }

  @Test
  public void testMetricsReporterIsDisabledForClients() {
    ConfluentLicenseValidator licenseValidator = new ConfluentLicenseValidator() {
      @Override
      protected LicenseManager createLicenseManager(LicenseConfig licenseConfig) {
        assertNull(licenseConfig.consumerConfigs().get(
            ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG));
        assertNull(licenseConfig.producerConfigs().get(
            ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG));
        assertNull(licenseConfig.topicAndAdminClientConfigs().get(
            AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG));
        LicenseStore store = EasyMock.niceMock(LicenseStore.class);
        replay(store);
        return new MockLicenseManager(store, time, licenseConfig.license);
      }
    };

    Map<String, String> configs = new HashMap<>();
    long licensePeriodMs =  60 * 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    configs.put(LicenseConfig.LICENSE_PROP, license);
    configs.put(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG, "SomeClassThatDoesNotExist");
    licenseValidator.configure(configs);

    licenseValidator.start("broker1");
  }

  private ConfluentLicenseValidator newConfluentLicenseValidator(String configuredLicense,
                                                                 String storedLicense) {
    licenseStore = new MockLicenseStore(LicenseConfig.TOPIC_DEFAULT,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Duration.ofSeconds(10), storedLicense);

    ConfluentLicenseValidator licenseValidator = new ConfluentLicenseValidator(Duration.ofMillis(10)) {
      @Override
      protected KafkaLicenseStore createLicenseStore(LicenseConfig licenseConfig) {
        return licenseStore;
      }

      @Override
      protected LicenseManager createLicenseManager(LicenseConfig licenseConfig) {
        try {
          return new MockLicenseManager(licenseStore, time, configuredLicense);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public boolean isLicenseValid() {
        numberOfLicenseValidationCalls++;
        return super.isLicenseValid();
      }

    };
    licenseValidator.configure(Collections.singletonMap(LicenseConfig.LICENSE_PROP, configuredLicense));
    return licenseValidator;
  }

  private String createExpiredLicense() {
    long licensePeriodMs =  60 * 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    time.sleep(licensePeriodMs + 1000);
    return license;
  }

  private void verifyLicenseValidator(long licensePeriodMs) throws Exception {
    LicenseTestUtils.verifyLicenseMetric(ConfluentLicenseValidator.METRIC_GROUP, LicenseStatus.LICENSE_ACTIVE);
    time.sleep(licensePeriodMs + 1000);
    TestUtils.waitForCondition(() -> numberOfLicenseValidationCalls > 2,  "Periodic license validation is not working.");
    TestUtils.waitForCondition(() -> licenseValidator.licenseStatus() == LicenseStatus.LICENSE_EXPIRED,
        "License status not updated");
  }

  private void waitForStoreStart(boolean expectActive) throws Exception {
    TestUtils.waitForCondition(() -> licenseStore.startCount > 0, "License store not started");
    assertEquals(1, licenseStore.startCount);
    TestUtils.waitForCondition(() -> licenseStore.active() == expectActive, "License store active!=" + expectActive);
  }

  private void verifyClose() {
    licenseValidator.close();
    assertEquals(1, licenseStore.stopCount);
    if (TestExecutorService.lastExecutor != null)
      assertTrue("Executor not shutdown", TestExecutorService.lastExecutor.isShutdown());
  }

  private static class MockLicenseManager extends LicenseManager {
    MockLicenseManager(LicenseStore store, Time time, String license) {
      super(() -> 1, store, time, license, false);
    }
  }

  private static class MockLicenseStore extends KafkaLicenseStore {
    volatile int startCount;
    volatile int stopCount;
    String storedLicense;
    String registeredLicense;
    Semaphore startSemaphore;
    RuntimeException startException;

    MockLicenseStore(String topic,
                     Map<String, Object> producerConfig,
                     Map<String, Object> consumerConfig,
                     Map<String, Object> topicConfig,
                     Duration topicCreateTimeout,
                     String storedLicense) {
      super(topic, producerConfig, consumerConfig, topicConfig, topicCreateTimeout);
      this.storedLicense = storedLicense;
    }

    @Override
    protected void startLogStore() {
      startCount++;
      try {
        if (startSemaphore != null)
          startSemaphore.tryAcquire(15, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      if (startException != null)
        throw startException;
    }

    @Override
    protected void stopLogStore() {
      stopCount++;
    }

    @Override
    public String licenseScan() {
      return active() ? storedLicense : super.licenseScan();
    }

    @Override
    public void registerLicense(String license) {
      if (active())
        registeredLicense = license;
      else
        super.registerLicense(license);
    }

    void delayStart(Semaphore semaphore) {
      this.startSemaphore = semaphore;
    }
  }

  private static class TestExecutorService extends AbstractExecutorService
      implements ScheduledExecutorService {

    volatile static TestExecutorService lastExecutor;
    private ScheduledExecutorService executor;

    TestExecutorService() {
      this.executor = Executors.newSingleThreadScheduledExecutor();
      lastExecutor = this;
    }

    @Override
    public void execute(Runnable command) {
      executor.execute(command);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      return executor.schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      return executor.schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      return executor.scheduleAtFixedRate(command, 10L, 10L, TimeUnit.MILLISECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
      return executor.scheduleWithFixedDelay(command, 10L, 10L, TimeUnit.MILLISECONDS);
    }

    @Override
    public List<Runnable> shutdownNow() {
      return executor.shutdownNow();
    }

    @Override
    public void shutdown() {
      executor.shutdown();
    }

    @Override
    public boolean isShutdown() {
      return executor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return executor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return executor.awaitTermination(timeout, unit);
    }

    static void reset() {
      lastExecutor = null;
    }
  }

  private class LicenseEvent implements LicenseChanged {

    private final Type eventType;
    private final License license;

    LicenseEvent(Type eventType) throws Exception {
      this.eventType = eventType;
      this.license = LicenseManagerTest.generateLicenseObject("ClientA", time, 1);
    }

    @Override
    public License license() {
      return license;
    }

    @Override
    public Type type() {
      return eventType;
    }

    @Override
    public String description() {
      return null;
    }
  }
}
