/**
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.common;

import java.util.Objects;

public class AdminClientResult<T> {
  private final T result;
  private final Throwable exception;

  /**
   * @param result - the result of the admin client call, null if an exception was passed
   */
  public AdminClientResult(T result) {
    Objects.requireNonNull(result);
    this.result = result;
    this.exception = null;
  }

  /**
   * @param exception - the exception thrown by the admin client call, null if no exception occurred
   */
  public AdminClientResult(Throwable exception) {
    Objects.requireNonNull(exception);
    this.exception = exception;
    this.result = null;
  }

  /**
   * The result of the AdminClient-backed call. Null if the #{@link #hasException()}.
   */
  public T result() {
    return result;
  }

  /**
   * The exception of the AdminClient-backed call. Null if #{@link #hasException()} is false.
   */
  public Throwable exception() {
    return exception;
  }

  /**
   * @return boolean indicating whether this result had an exception
   */
  public boolean hasException() {
    return exception != null;
  }
}
