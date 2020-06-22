package io.confluent.telemetry;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class RegexConfigDefValidator implements ConfigDef.Validator {
  @Override
  public void ensureValid(String name, Object value) {
    String regexString = value.toString();
    try {
      Pattern.compile(regexString);
    } catch (PatternSyntaxException e) {
      throw new ConfigException(
          "Metrics filter for configuration "
              + name
              + " is not a valid regular expression"
      );
    }
  }
}
