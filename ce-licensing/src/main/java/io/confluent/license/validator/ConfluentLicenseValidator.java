// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.license.validator;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.AbstractConfig;
import io.confluent.license.InvalidLicenseException;
import io.confluent.license.License;
import io.confluent.license.LicenseChanged;
import io.confluent.license.LicenseChanged.Type;
import io.confluent.license.LicenseManager;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.server.license.LicenseValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * License validator used by Kafka brokers for proprietary features. This includes license
 * metrics and periodic error logging for expired license.
 *
 * Initial license verification throws {@link InvalidLicenseException} if the license is
 * invalid or has expired. The component using the license (e.g. broker) fails to start as
 * a result without a valid or free license. Thereafter, the component may continue to function
 * with proprietary features enabled even if license expires. In this case, an error is logged
 * periodically.
 *
 * License is stored in a Kafka topic in the MDS cluster if MDS is enabled. Otherwise it is stored
 * in the Kafka cluster associated with the component.
 */
public class ConfluentLicenseValidator implements LicenseValidator, Consumer<LicenseChanged> {
  private static final Logger log = LoggerFactory.getLogger(
      ConfluentLicenseValidator.class);

  private static final long EXPIRY_LOG_INTERVAL_MS = 10000;
  public static final String METRIC_GROUP = "confluent.license";
  public static final String METRIC_NAME = "licenseStatus";

  public enum LicenseStatus {
    TRIAL(true),
    TRIAL_EXPIRED(false),
    FREE_TIER(true),
    FREE_TIER_EXPIRED(false),
    LICENSE_ACTIVE(true),
    LICENSE_EXPIRED(false),
    INVALID_LICENSE(false),;

    final boolean active;
    LicenseStatus(boolean active) {
      this.active = active;
    }
  }

  private Map<String, ?> configs;
  private MetricName licenseStatusMetricName;
  private ScheduledExecutorService executorService;
  private LicenseManager licenseManager;
  private volatile LicenseStatus licenseStatus;
  private volatile String errorMessage;

  @Override
  public void configure(Map<String, ?> configs) {
    this.configs = configs;
  }

  @Override
  public void start(String componentId) {
    // Use MDS to store license if metadata server is configured.
    Map<String, Object> licenseConfigs = new HashMap<>(configs);
    LicenseConfig tmpConfig = new LicenseConfig(componentId, configs);
    replacePrefix(tmpConfig, licenseConfigs, "confluent.metadata.", LicenseConfig.PREFIX);
    replacePrefix(tmpConfig, licenseConfigs, "confluent.metadata.consumer.", LicenseConfig.CONSUMER_PREFIX);
    replacePrefix(tmpConfig, licenseConfigs, "confluent.metadata.producer.", LicenseConfig.PRODUCER_PREFIX);
    LicenseConfig licenseConfig = new LicenseConfig(componentId, configs);

    licenseManager = createLicenseManager(licenseConfig);
    licenseManager.addListener(this);
    License registeredLicense = licenseManager.registerOrValidateLicense(licenseConfig.license);
    updateLicenseStatus(registeredLicense);
    licenseManager.start();

    if (!isLicenseValid())
      throw new InvalidLicenseException("License validation failed: " + errorMessage);
    registerMetric(METRIC_GROUP);
    schedulePeriodicValidation();
  }

  @Override
  public boolean isLicenseValid() {
    return licenseStatus != null && licenseStatus.active;
  }

  @Override
  public void accept(LicenseChanged licenseChanged) {
    License license = licenseChanged.license();
    if (licenseChanged.type() == Type.EXPIRED) {
      Date expirationDate = license.expirationDate();
      if (license.isTrial()) {
        updateExpiredStatus(LicenseStatus.TRIAL_EXPIRED, expirationDate);
      } else if (license.isFreeTier()) {
        updateExpiredStatus(LicenseStatus.FREE_TIER_EXPIRED, expirationDate);
      } else {
        updateExpiredStatus(LicenseStatus.LICENSE_EXPIRED, expirationDate);
      }
    } else {
      updateLicenseStatus(license);
    }
  }

  public void close() {
    if (executorService != null) {
      executorService.shutdownNow();
      try {
        executorService.awaitTermination(60, TimeUnit.SECONDS);
      } catch (Exception e) {
        log.error("License executor did not terminate");
      }
    }
    if (licenseManager != null) {
      licenseManager.removeListener(this);
      licenseManager.stop();
    }
    try {
      Metrics.defaultRegistry().removeMetric(licenseStatusMetricName);
    } catch (Exception e) {
      log.debug("Metric not found", licenseStatusMetricName);
    }
  }

  // License manager may fail to create topic when brokers are starting up
  // if replication factor is greater than the number of available brokers.
  // So we need to retry. Caller will timeout after startup timeout.
  protected LicenseManager createLicenseManager(LicenseConfig licenseConfig) {
    while (true) {
      try {
        return new LicenseManager(licenseConfig.topic,
            licenseConfig.producerConfigs(),
            licenseConfig.consumerConfigs(),
            licenseConfig.topicConfigs());
      } catch (Exception e) {
        boolean retry = false;
        for (Throwable ex = e; ex != null; ex = ex.getCause()) {
          if (ex instanceof RetriableException || ex instanceof InvalidReplicationFactorException) {
            retry = true;
            break;
          }
        }
        if (!retry)
          throw e;
      }
    }
  }

  protected void updateExpiredStatus(LicenseStatus status, Date expirationDate) {
    switch (status) {
      case TRIAL_EXPIRED:
        errorMessage = "Your trial license has expired. "
            + "Please add a valid license to continue using the product";
        break;
      case FREE_TIER_EXPIRED:
        errorMessage = "Your free-tier license has expired. "
            + "Please add a valid license to continue using the product";
        break;
      case LICENSE_EXPIRED:
        errorMessage = String.format("Your license expired at %s. "
            + "Please add a valid license to continue using the product", expirationDate);
        break;
      default:
        throw new IllegalStateException("Unexpected expired license status " + status);
    }
    this.licenseStatus = status;
  }

  protected void updateLicenseStatus(LicenseStatus status) {
    this.errorMessage = null;
    this.licenseStatus = status;
  }

  private void updateLicenseStatus(License license) {
    if (license.isTrial()) {
      updateLicenseStatus(LicenseStatus.TRIAL);
    } else if (license.isFreeTier()) {
      updateLicenseStatus(LicenseStatus.FREE_TIER);
    } else {
      updateLicenseStatus(LicenseStatus.LICENSE_ACTIVE);
    }
  }

  // Registering yammer metric since we don't have access to the KafkaMetrics instance
  protected void registerMetric(String metricGroup) {
    String metricType = LicenseValidator.class.getSimpleName();
    MetricName metricName = new MetricName(metricGroup, metricType, METRIC_NAME, null,
        String.format("%s:type=%s,name=%s", metricGroup, metricType, METRIC_NAME));
    Metrics.defaultRegistry().newGauge(metricName, new Gauge<String>() {
      @Override
      public String value() {
        return licenseStatus.name().toLowerCase(Locale.ROOT);
      }
    });
    this.licenseStatusMetricName = metricName;
  }

  protected void schedulePeriodicValidation() {
    if (executorService != null)
      throw new IllegalStateException("License validation has already been started");

    executorService = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable, "confluent-license-manager");
      thread.setDaemon(true);
      return thread;
    });
    executorService.schedule(() -> {
      String error = this.errorMessage;
      if (!isLicenseValid() && error != null) {
        log.error(errorMessage);
      }
    }, EXPIRY_LOG_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }

  private void replacePrefix(AbstractConfig srcConfig, Map<String, Object> dstConfigs, String srcPrefix, String dstPrefix) {
    Map<String, Object> prefixedConfigs = srcConfig.originalsWithPrefix(srcPrefix);
    prefixedConfigs.forEach((k, v) -> {
      dstConfigs.remove(srcPrefix + k);
      dstConfigs.putIfAbsent(dstPrefix + k, v);
    });
  }
}