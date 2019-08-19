/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.exceptions;

public class TierObjectStoreFatalException extends RuntimeException {

  public TierObjectStoreFatalException(String message, Throwable cause) {
    super(message, cause);
  }

  public TierObjectStoreFatalException(String message) {
    super(message);
  }
}
