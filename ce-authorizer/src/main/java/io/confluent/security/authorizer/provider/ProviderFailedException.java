// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import org.apache.kafka.common.errors.ApiException;

public class ProviderFailedException extends ApiException {

  public ProviderFailedException(String message) {
    super(message);
  }

  public ProviderFailedException(Throwable cause) {
    super(cause);
  }
}
