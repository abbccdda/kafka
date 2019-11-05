// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.store;

import org.apache.kafka.common.errors.NotCoordinatorException;

public class NotMasterWriterException extends NotCoordinatorException {

  private static final long serialVersionUID = 1L;

  public NotMasterWriterException(String message) {
    super(message);
  }
}
