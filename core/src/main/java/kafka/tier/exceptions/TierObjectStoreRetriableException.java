/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.exceptions;

import org.apache.kafka.common.errors.RetriableException;

public class TierObjectStoreRetriableException extends RetriableException {
  public TierObjectStoreRetriableException(String message, Throwable cause) {
    super(message, cause);
  }
}
