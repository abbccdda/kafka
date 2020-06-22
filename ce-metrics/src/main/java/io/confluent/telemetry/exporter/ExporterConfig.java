package io.confluent.telemetry.exporter;

import com.google.common.collect.ImmutableSet;

import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.RegexConfigDefValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public abstract class ExporterConfig extends AbstractConfig {

  public static enum ExporterType {
      http,
      kafka
  }

  public static final String ENABLED_CONFIG = "enabled";
  public static final String ENABLED_CONFIG_DOC = "Boolean value. True exporter should be enabled, false otherwise.";
  public static final boolean ENABLED_CONFIG_DEFAULT = true;

  public static final String TYPE_CONFIG = "type";
  public static final String TYPE_CONFIG_DOC =
      "The type of the exporter. Value must be on of " + Arrays.asList(ExporterType.values());

  public static final String WHITELIST_CONFIG = "whitelist";
  public static final String WHITELIST_DOC =
      "Regex matching the converted (snake_case) metric name to be published from this "
          + "particular exporter.\n\nBy default this includes all the metrics required by "
          + "Proactive Support and Confluent Auto Data Balancer. This should typically never "
          + "be modified unless requested by Confluent.";
  public static final String DEFAULT_WHITELIST = ConfluentTelemetryConfig.DEFAULT_WHITELIST;

  public static final Set<String> RECONFIGURABLES = ImmutableSet.of(ENABLED_CONFIG, WHITELIST_CONFIG);

  private static ConfigDef defineExporterConfigs(ConfigDef def) {
    return new ConfigDef(def)
      .define(
          ENABLED_CONFIG,
          ConfigDef.Type.BOOLEAN,
          ENABLED_CONFIG_DEFAULT,
          ConfigDef.Importance.LOW,
          ENABLED_CONFIG_DOC
      ).define(
          TYPE_CONFIG,
          ConfigDef.Type.STRING,
          ConfigDef.NO_DEFAULT_VALUE,
          ConfigDef.LambdaValidator.with(
            (name, value) -> parseType(value),
            () -> Arrays.asList(ExporterType.values()).toString()
          ),
          ConfigDef.Importance.LOW,
          TYPE_CONFIG_DOC
      ).define(
          WHITELIST_CONFIG,
          ConfigDef.Type.STRING,
          DEFAULT_WHITELIST,
          new RegexConfigDefValidator(),
          ConfigDef.Importance.LOW,
          WHITELIST_DOC
      );
  }

  public static ExporterType parseType(Object value) {
    if (value == null || value.toString().isEmpty()) {
      throw new ConfigException(
        "'" + TYPE_CONFIG + "' is a required config."
      );
    } else {
      try {
        return ExporterType.valueOf(value.toString());
      } catch (IllegalArgumentException e) {
        throw new ConfigException(
          "'" + TYPE_CONFIG + "' must be one of: "
          + Arrays.asList(ExporterType.values())
          + " however we found '"
          + value.toString()
          + "'"
        );
      }
    }
  }

  public ExporterConfig(ConfigDef definition, Map<?, ?> originals) {
    this(definition, originals, true);
  }

  public ExporterConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
    super(defineExporterConfigs(definition), originals, doLog);
  }

  public Predicate<MetricKey> buildMetricWhitelistFilter() {
    return ConfluentTelemetryConfig.buildMetricWhitelistFilter(getMetricsWhitelistRegex());
  }

  public ExporterType getType() {
    return parseType(getString(TYPE_CONFIG));
  }

  public boolean isEnabled() {
    return getBoolean(ENABLED_CONFIG);
  }

  public String getMetricsWhitelistRegex() {
    return getString(WHITELIST_CONFIG);
  }
}
