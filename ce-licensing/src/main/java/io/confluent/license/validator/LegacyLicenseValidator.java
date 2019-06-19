// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.license.validator;

import io.confluent.license.InvalidLicenseException;
import io.confluent.license.License;
import io.confluent.license.trial.ZkTrialPeriod;
import java.security.PublicKey;
import java.util.Date;
import java.util.Map;
import org.apache.kafka.common.utils.Time;
import org.jose4j.jwt.JwtClaims;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Legacy license validator that stores license in-memory and uses ZooKeeper for trial license.
 * This validator is used by legacy broker features (LdapAuthorizer) that cannot rely on storing
 * license in Kafka topics.
 */
public class LegacyLicenseValidator extends ConfluentLicenseValidator {
  private static final Logger log = LoggerFactory.getLogger(
      ConfluentLicenseValidator.class);

  private static final String ZK_CONNECT_PROP = "zookeeper.connect";
  private static final PublicKey PUBLIC_KEY;

  private final Time time;
  private String zkConnect;
  private boolean licenseConfigured;
  private long validUntilMs;

  static {
    PublicKey publicKey = null;
    try {
      publicKey = License.loadPublicKey();
    } catch (Exception e) {
      log.error("Public key for license service could not be loaded", e);
    }
    PUBLIC_KEY = publicKey;
  }

  public LegacyLicenseValidator(Time time) {
    super(time);
    this.time = time;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.zkConnect = (String) configs.get(ZK_CONNECT_PROP);
  }

  @Override
  public void initializeAndVerify(String license, String metricGroup, String componentId)
      throws InvalidLicenseException {
    if (PUBLIC_KEY == null) {
      throw new InvalidLicenseException("Public key for license validator could not be loaded");
    }

    long now = time.milliseconds();
    licenseConfigured = license != null && !license.isEmpty();

    if (licenseConfigured) {
      try {
        JwtClaims claims = License.verify(PUBLIC_KEY, license);
        validUntilMs = claims.getExpirationTime().getValueInMillis();
        updateLicenseStatus(LicenseStatus.LICENSE_ACTIVE);
      } catch (Exception e) {
        String errorMessage = "Validation of configured license failed";
        log.error(errorMessage, e);
        throw new InvalidLicenseException(errorMessage, e);
      }
    } else {
      ZkTrialPeriod trialPeriod = new ZkTrialPeriod(zkConnect);
      validUntilMs = now + trialPeriod.startOrVerify(now);
      updateLicenseStatus(LicenseStatus.TRIAL);
    }

    if (verifyLicense()) {
      if (licenseConfigured) {
        updateLicenseStatus(LicenseStatus.LICENSE_ACTIVE);
      } else {
        updateLicenseStatus(LicenseStatus.TRIAL);
      }
      registerMetric(metricGroup);
    } else {
      throw new InvalidLicenseException("License not valid");
    }
  }

  @Override
  public boolean verifyLicense() {
    long now = time.milliseconds();
    if (now >= validUntilMs) {
      Date expiration = new Date(validUntilMs);
      if (licenseConfigured) {
        updateExpiredStatus(LicenseStatus.LICENSE_EXPIRED, expiration);
      } else {
        updateExpiredStatus(LicenseStatus.TRIAL_EXPIRED, expiration);
      }
    }
    return super.verifyLicense();
  }
}