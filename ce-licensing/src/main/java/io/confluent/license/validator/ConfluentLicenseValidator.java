// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.license.validator;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import io.confluent.license.LicenseStore;
import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.KafkaException;
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
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.utils.Time;
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
 *
 * Since LicenseManager is used by several components, broker-specific startup sequence is
 * managed by ConfluentLicenseValidator. If a valid license is configured, license store is
 * started asynchronously. License is registered if/when license store start up completes.
 * Broker starts up successfully if a valid license is configured in server.properties even
 * if the license topic cannot be created or is unavailable. If a valid license is not configured,
 * license validator blocks on the license store during start up and broker start up fails if
 * the license topic is unavailable within the create timeout.
 */
public class ConfluentLicenseValidator implements LicenseValidator, Consumer<LicenseChanged> {
  private static final Logger log = LoggerFactory.getLogger(
      ConfluentLicenseValidator.class);

  private static final Duration EXPIRY_LOG_INTERVAL = Duration.ofSeconds(10);
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

  private final Duration expiryLogInterval;
  private Map<String, ?> configs;
  private MetricName licenseStatusMetricName;
  private ScheduledExecutorService executorService;
  private KafkaLicenseStore licenseStore;
  private LicenseManager licenseManager;
  private volatile LicenseStatus licenseStatus;
  private volatile String errorMessage;

  public ConfluentLicenseValidator() {
    this(EXPIRY_LOG_INTERVAL);
  }

  ConfluentLicenseValidator(Duration expiryLogInterval) {
    this.expiryLogInterval = expiryLogInterval;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.configs = configs;
  }

  @Override
  public final boolean enabled() {
    return true;
  }

  @Override
  public void start(String componentId) {
    // Use MDS to store license if metadata server is configured.
    Map<String, Object> licenseConfigs = new HashMap<>(configs);
    LicenseConfig tmpConfig = new LicenseConfig(componentId, configs);
    replacePrefix(tmpConfig, licenseConfigs, "confluent.metadata.", LicenseConfig.PREFIX);
    replacePrefix(tmpConfig, licenseConfigs, "confluent.metadata.consumer.", LicenseConfig.CONSUMER_PREFIX);
    replacePrefix(tmpConfig, licenseConfigs, "confluent.metadata.producer.", LicenseConfig.PRODUCER_PREFIX);
    replacePrefix(tmpConfig, licenseConfigs, "confluent.metadata.admin.", LicenseConfig.ADMIN_PREFIX);
    LicenseConfig licenseConfig = new LicenseConfig(componentId, configs);

    licenseStore = createLicenseStore(licenseConfig);
    licenseManager = createLicenseManager(licenseConfig);
    licenseManager.addListener(this);
    License configuredLicense = licenseManager.configuredLicense();
    if (configuredLicense != null)
      updateLicenseStatus(configuredLicense);

    String licenseStoreDesc = "license store with topic " + licenseConfig.topic;
    Future<?> startFuture = startLicenseStore(licenseConfig.license, configuredLicense == null, licenseStoreDesc);
    if (configuredLicense == null) {
      try {
        startFuture.get(licenseConfig.topicCreateTimeout.toMillis() + 15000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new InterruptException("Start up of " + licenseStoreDesc + " was interrupted", e);
      } catch (TimeoutException e) {
        throw new org.apache.kafka.common.errors.TimeoutException("Start up timed out for " + licenseStoreDesc, e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof InvalidLicenseException)
          throw (InvalidLicenseException) e.getCause();
        throw new KafkaException("Failed to start " + licenseStoreDesc +
            ". A valid license must be configured using '" + LicenseConfig.LICENSE_PROP + "' if topic is unavailable.",
            e.getCause());
      }
    }

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

  protected KafkaLicenseStore createLicenseStore(LicenseConfig licenseConfig) {
    return new KafkaLicenseStore(licenseConfig.topic,
        licenseConfig.producerConfigs(),
        licenseConfig.consumerConfigs(),
        licenseConfig.topicAndAdminClientConfigs(),
        licenseConfig.topicCreateTimeout,
        licenseConfig.retryBackoffMinMs,
        licenseConfig.retryBackoffMaxMs);
  }

  protected LicenseManager createLicenseManager(LicenseConfig licenseConfig) {
    return new LicenseManager(
        licenseConfig.topicAndAdminClientConfigs(),
        licenseStore,
        licenseConfig.license,
        false);
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

  // Visibility for testing
  LicenseStatus licenseStatus() {
    return licenseStatus;
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

  private Future<?> startLicenseStore(String configuredLicense, boolean failOnError, String storeDesc) {
    if (executorService != null)
      throw new IllegalStateException("License validation has already been started");
    executorService = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable, "confluent-license-manager");
      thread.setDaemon(true);
      return thread;
    });
    return executorService.submit(() -> {
      try {
        licenseStore.start();
        License license = licenseManager.registerOrValidateLicense(configuredLicense);
        if (license != null)
          updateLicenseStatus(license);
      } catch (Throwable t) {
        if (failOnError)
          throw t;
        else
          log.warn("Could not start " + storeDesc + ", configured license will be used without storing in license topic", t);
      }
      licenseManager.start();
    });
  }

  protected void schedulePeriodicValidation() {
    executorService.scheduleAtFixedRate(() -> {
      String error = this.errorMessage;
      if (!isLicenseValid() && error != null) {
        log.error(errorMessage);
      }
    }, expiryLogInterval.toMillis(), expiryLogInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void replacePrefix(AbstractConfig srcConfig, Map<String, Object> dstConfigs, String srcPrefix, String dstPrefix) {
    Map<String, Object> prefixedConfigs = srcConfig.originalsWithPrefix(srcPrefix);
    prefixedConfigs.forEach((k, v) -> {
      dstConfigs.remove(srcPrefix + k);
      dstConfigs.putIfAbsent(dstPrefix + k, v);
    });
  }

  static class KafkaLicenseStore extends LicenseStore {
    private volatile boolean active;

    KafkaLicenseStore(String topic,
                      Map<String, Object> producerConfig,
                      Map<String, Object> consumerConfig,
                      Map<String, Object> topicConfig,
                      Duration topicCreateTimeout,
                      Duration retryBackoffMinMs,
                      Duration retryBackoffMaxMs) {
      super(topic, producerConfig, consumerConfig, topicConfig, topicCreateTimeout, retryBackoffMinMs, retryBackoffMaxMs, Time.SYSTEM);
    }

    @Override
    protected void startLog() {
      startLogStore();
      active = true;
    }

    @Override
    protected void stopLog() {
      active = false;
      stopLogStore();
    }

    // Visibility to override for testing
    protected void startLogStore() {
      super.startLog();
    }

    // Visibility to override for testing
    protected void stopLogStore() {
      super.stopLog();
    }

    @Override
    public String licenseScan() {
      return active ? super.licenseScan() : null;
    }

    @Override
    public synchronized void registerLicense(String license, Callback callback) {
      if (active) {
        super.registerLicense(license, callback);
      } else
        log.debug("License store is not active, not registering license");
    }

    protected boolean active() {
      return active;
    }
  }
}