// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.common.license;

import org.apache.kafka.common.KafkaException;

public class LicenseExpiredException extends KafkaException {

  private static final long serialVersionUID = 1L;

  public LicenseExpiredException(String message) {
    super(message);
  }

  public LicenseExpiredException(String message, Throwable cause) {
    super(message, cause);
  }
}
