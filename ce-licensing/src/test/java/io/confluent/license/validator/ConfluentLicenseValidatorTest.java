// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.license.validator;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertFalse;
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
  public void testLicense() throws Exception {
    String license = LicenseTestUtils.generateLicense();
    licenseValidator = newConfluentLicenseValidator();
    licenseValidator.initializeAndVerify(license, "test", "broker1");
    licenseValidator.verifyLicense();
    LicenseTestUtils.verifyLicenseMetric("test", LicenseStatus.LICENSE_ACTIVE);
  }

  @Test(expected = InvalidLicenseException.class)
  public void testInvalidLicense() throws Exception {
    newConfluentLicenseValidator().initializeAndVerify("invalid", "test", "broker1");
  }

  @Test(expected = InvalidLicenseException.class)
  public void testExpiredLicense() throws Exception {
    long licensePeriodMs =  60 * 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    time.sleep(licensePeriodMs + 1000);
    licenseValidator = newConfluentLicenseValidator();
    licenseValidator.initializeAndVerify(license, "test", "broker1");
  }

  @Test
  public void testVerifyLicense() throws Exception {
    long licensePeriodMs =  60 * 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    licenseValidator = newConfluentLicenseValidator();
    licenseValidator.initializeAndVerify(license, "test", "broker1");
    assertTrue(licenseValidator.verifyLicense());
    time.sleep(licensePeriodMs + 1000);
    licenseValidator.accept(new LicenseEvent(Type.EXPIRED));
    assertFalse(licenseValidator.verifyLicense());
    LicenseTestUtils.verifyLicenseMetric("test", LicenseStatus.LICENSE_EXPIRED);
  }

  private ConfluentLicenseValidator newConfluentLicenseValidator() {
    ConfluentLicenseValidator licenseValidator = new ConfluentLicenseValidator(time) {
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
    licenseValidator.configure(Collections.emptyMap());
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