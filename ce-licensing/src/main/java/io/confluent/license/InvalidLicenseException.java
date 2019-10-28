/*
 * Copyright [2017  - 2017] Confluent Inc.
 */

package io.confluent.license;

import org.apache.kafka.common.KafkaException;

public class InvalidLicenseException extends KafkaException {
  public InvalidLicenseException(String message) {
    super(message);
  }

  public InvalidLicenseException(String message, Throwable cause) {
    super(message, cause);
  }
}
