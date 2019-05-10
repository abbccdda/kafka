// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.license.validator;

import io.confluent.common.license.LicenseExpiredException;
import io.confluent.common.license.InvalidLicenseException;
import io.confluent.license.test.utils.LicenseTestUtils;
import io.confluent.license.validator.ConfluentLicenseValidator.LicenseStatus;
import org.apache.kafka.common.utils.MockTime;
import org.junit.After;
import org.junit.Test;

public class LicenseValidatorTest {

  private final MockTime time = new MockTime();
  private ConfluentLicenseValidator licenseValidator;

  @After
  public void tearDown() {
    if (licenseValidator != null)
      licenseValidator.close();
  }

  @Test
  public void testLicense() {
    String license = LicenseTestUtils.generateLicense();
    licenseValidator = new ConfluentLicenseValidator();
    licenseValidator.initializeAndVerify(license, null, time, "test");
    licenseValidator.verifyLicense(true);
    LicenseTestUtils.verifyLicenseMetric("test", LicenseStatus.LICENSE_ACTIVE);
  }

  @Test(expected = InvalidLicenseException.class)
  public void testInvalidLicense() {
    new ConfluentLicenseValidator().initializeAndVerify("invalid", null, time, "test");
  }

  @Test(expected = LicenseExpiredException.class)
  public void testExpiredLicense() {
    long licensePeriodMs =  60 * 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    time.sleep(licensePeriodMs + 1000);
    licenseValidator = new ConfluentLicenseValidator();
    licenseValidator.initializeAndVerify(license, null, time, "test");
  }

  @Test
  public void testExpiredLicenseWithoutFailOnExpiry() {
    long licensePeriodMs =  60 * 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    licenseValidator = new ConfluentLicenseValidator();
    licenseValidator.initializeAndVerify(license, null, time, "test");
    time.sleep(licensePeriodMs + 1000);
    licenseValidator.verifyLicense(false); // should not throw exception
    LicenseTestUtils.verifyLicenseMetric("test", LicenseStatus.LICENSE_EXPIRED);
  }

  @Test(expected = LicenseExpiredException.class)
  public void testExpiredLicenseWithFailOnExpiry() {
    long licensePeriodMs =  60 * 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    licenseValidator = new ConfluentLicenseValidator();
    licenseValidator.initializeAndVerify(license, null, time, "test");
    time.sleep(licensePeriodMs + 1000);
    licenseValidator.verifyLicense(true);
  }
}