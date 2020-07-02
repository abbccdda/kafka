package io.confluent.telemetry.events.exporter;

import com.google.common.collect.ImmutableSet;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class ExporterConfig extends AbstractConfig {

  public enum ExporterType {
      http,
      kafka,
      test // Required for testing.
  }


  public static final String ENABLED_CONFIG = "enabled";
  public static final String ENABLED_CONFIG_DOC = "Boolean value. True exporter should be enabled, false otherwise.";
  public static final boolean ENABLED_CONFIG_DEFAULT = true;

  public static final String TYPE_CONFIG = "type";
  public static final String TYPE_CONFIG_DOC =
      "The type of the exporter. Value must be on of " + Arrays.asList(ExporterType.values());


  public static final Set<String> RECONFIGURABLES = ImmutableSet.of(ENABLED_CONFIG);

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

  public ExporterConfig(Map<String, ?> originals) {
    super(defineExporterConfigs(new ConfigDef()), originals);
  }

  public ExporterConfig(ConfigDef definition, Map<?, ?> originals) {
    this(definition, originals, true);
  }

  public ExporterConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
    super(defineExporterConfigs(definition), originals, doLog);
  }

  public ExporterType getType() {
    return parseType(getString(TYPE_CONFIG));
  }

  public boolean isEnabled() {
    return getBoolean(ENABLED_CONFIG);
  }
}
