package io.confluent.telemetry;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates map keys according to configured rules.
 * <p>
 * Use case is for handling deprecated config properties by translating them to their
 * non-deprecated equivalents.
 */
public class ConfigPropertyTranslater {

  private static final Logger log = LoggerFactory.getLogger(ConfigPropertyTranslater.class);

  private final Map<String, String> translations;
  private final Map<String, String> prefixTranslations;

  private ConfigPropertyTranslater(Builder builder) {
    translations = ImmutableMap.copyOf(builder.translations);
    prefixTranslations = ImmutableMap.copyOf(builder.prefixTranslations);
  }

  /**
   * Return a <b>new</b> Map by applying the configured translations to each key.
   * A warning message will be logged for each key that is translated.
   * <p>
   * Exact matches are checked before prefix matches.
   */
  public <T> Map<String, T> translate(Map<String, T> originals) {
    Map<String, T> result = new HashMap<>();
    originals.forEach((key, value) -> {
      String translatedKey = translateKey(key);
      if (translatedKey == null) {
        translatedKey = translateKeyViaPrefix(key);
      }

      if (translatedKey != null) {
        if (originals.containsKey(translatedKey)) {
          log.warn("Deprecated key '{}' and it's new counterpart '{}' have both been specified. "
              + "The value for the new key '{}' will be used. Please remove the deprecated key '{}' from your configuration.",
              key, translatedKey, translatedKey, key);
          result.put(translatedKey, originals.get(translatedKey));
        } else {
          log.warn("Configuration key '{}' is deprecated and may be removed in the future. "
              + "Please update your configuration to use '{}' instead.", key, translatedKey);
          result.put(translatedKey, value);
        }
      } else {
        result.put(key, value);
      }
    });
    return result;
  }

  private String translateKey(String key) {
    return translations.get(key);
  }

  private String translateKeyViaPrefix(String key) {
    return prefixTranslations.entrySet()
        .stream()
        .filter(entry -> key.startsWith(entry.getKey()))
        .findFirst()
        .map(entry -> {
          String oldPrefix = entry.getKey();
          String newPrefix = entry.getValue();
          return key.replace(oldPrefix, newPrefix);
        })
        .orElse(null);
  }

  public static class Builder {
    private final Map<String, String> translations = new HashMap<>();
    private final Map<String, String> prefixTranslations = new LinkedHashMap<>();

    /**
     * Add a property translation.
     */
    public Builder withTranslation(String deprecatedProperty, String newProperty) {
      translations.put(deprecatedProperty, newProperty);
      return this;
    }

    /**
     * Add a prefix translation.
     *
     * <p>
     * Any keys starting with <code>deprecatedPrefix</code> will be translated into a new key with
     * <code>newPrefix</code> substituted for <code>deprecatedPrefix</code>.
     *
     * <p>
     * Prefix translations are only checked in the absence of an exact match translations.
     */
    public Builder withPrefixTranslation(String deprecatedPrefix, String newPrefix) {
      prefixTranslations.put(deprecatedPrefix, newPrefix);
      return this;
    }

    public ConfigPropertyTranslater build() {
      return new ConfigPropertyTranslater(this);
    }
  }

}
