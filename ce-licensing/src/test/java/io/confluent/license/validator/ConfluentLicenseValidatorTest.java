// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.license.validator;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.license.InvalidLicenseException;
import io.confluent.license.License;
import io.confluent.license.LicenseChanged;
import io.confluent.license.LicenseChanged.Type;
import io.confluent.license.LicenseManager;
import io.confluent.license.LicenseManagerTest;
import io.confluent.license.LicenseStore;
import io.confluent.license.test.utils.LicenseTestUtils;
import io.confluent.license.validator.ConfluentLicenseValidator.LicenseStatus;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
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

  @Before
  public void setUp() throws Exception {
    LicenseTestUtils.injectPublicKey();
  }

  @After
  public void tearDown() {
    if (licenseValidator != null)
      licenseValidator.close();
  }

  @Test
  public void testLicense() {
    String license = LicenseTestUtils.generateLicense();
    licenseValidator = newConfluentLicenseValidator(license);
    licenseValidator.start("broker1");
    assertTrue("Invalid license", licenseValidator.isLicenseValid());
    LicenseTestUtils.verifyLicenseMetric(ConfluentLicenseValidator.METRIC_GROUP, LicenseStatus.LICENSE_ACTIVE);
  }

  @Test(expected = InvalidLicenseException.class)
  public void testInvalidLicense() {
    newConfluentLicenseValidator("invalid").start("broker1");
  }

  @Test(expected = InvalidLicenseException.class)
  public void testExpiredLicense() {
    long licensePeriodMs =  60 * 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    time.sleep(licensePeriodMs + 1000);
    licenseValidator = newConfluentLicenseValidator(license);
    licenseValidator.start("broker1");
  }

  @Test
  public void testVerifyLicense() throws Exception {
    long licensePeriodMs =  60 * 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    licenseValidator = newConfluentLicenseValidator(license);
    licenseValidator.start("broker1");
    assertTrue(licenseValidator.isLicenseValid());
    time.sleep(licensePeriodMs + 1000);
    licenseValidator.accept(new LicenseEvent(Type.EXPIRED));
    assertFalse(licenseValidator.isLicenseValid());
    LicenseTestUtils.verifyLicenseMetric(ConfluentLicenseValidator.METRIC_GROUP, LicenseStatus.LICENSE_EXPIRED);
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
        assertNull(licenseConfig.topicConfigs().get(
            AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG));
        LicenseStore store = EasyMock.niceMock(LicenseStore.class);
        replay(store);
        return new MockLicenseManager(store, time);
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

  private ConfluentLicenseValidator newConfluentLicenseValidator(String license) {
    ConfluentLicenseValidator licenseValidator = new ConfluentLicenseValidator() {
      @Override
      protected LicenseManager createLicenseManager(LicenseConfig licenseConfig) {
        try {
          LicenseStore store = EasyMock.niceMock(LicenseStore.class);
          expect(store.licenseScan()).andReturn("");
          replay(store);
          return new MockLicenseManager(store, time);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    licenseValidator.configure(Collections.singletonMap(LicenseConfig.LICENSE_PROP, license));
    return licenseValidator;
  }

  private static class MockLicenseManager extends LicenseManager {
    MockLicenseManager(LicenseStore store, Time time) {
      super(() -> 1, store, time);
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
