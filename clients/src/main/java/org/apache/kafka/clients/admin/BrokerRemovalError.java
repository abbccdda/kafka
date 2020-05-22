package org.apache.kafka.clients.admin;

import org.apache.kafka.common.protocol.Errors;

/**
 * The error that caused a broker removal operation to fail.
 */
public final class BrokerRemovalError {
  private final short errorCode;
  private final String errorMessage;
  private final Exception exception;

  public BrokerRemovalError(Errors error, String errorMessage) {
    this.exception = error.exception(errorMessage);
    this.errorCode = error.code();
    this.errorMessage = errorMessage;
  }

  /**
   * @return the Exception class that corresponds to this type of error
   */
  public Exception exception() {
    return exception;
  }

  /**
   * @return the code of this error
   */
  public short errorCode() {
    return errorCode;
  }

  public String errorMessage() {
    return errorMessage;
  }
}
