/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.http.server;

import java.time.Duration;
import java.util.Optional;

/**
 * An HTTP server that runs inside the Kafka broker.
 *
 * <p>See {@link KafkaHttpServerLoader} for details on how this class is loaded.
 *
 * <p>The life-cycle of the Kafka HTTP Server should follow the state machine below:
 *
 * <pre>
 *           NEW
 *            |
 *            v
 *  ------ STARTING ------
 *  |         |          |
 *  |         v          |
 *  ------ RUNNING -------
 *  |         |          |
 *  |         v          |
 *  |     STOPPING -------
 *  |         |          |
 *  |         v          v
 *  ---> TERMINATED    FAILED
 * </pre>
 *
 * <p>Implementations should be thread-safe.
 *
 * @since 6.0.0
 */
public interface KafkaHttpServer {

  /**
   * Returns whether this {@code KafkaHttpServer} is in state NEW.
   *
   * <p>A {@code KafkaHttpServer} is NEW if it has been created, but {@link #start()} has never been
   * called.
   *
   * @since 6.0.0
   */
  boolean isNew();

  /**
   * Returns whether this {@code KafkaHttpServer} is in state STARTING.
   *
   * <p>A {@code KafkaHttpServer} is STARTING if {@link #start()} has been called, but the server is
   * not ready to accept requests yet.
   *
   * @since 6.0.0
   */
  boolean isStarting();

  /**
   * Returns whether this {@code KafkaHttpServer} is in state RUNNING.
   *
   * <p>A {@code KafkaHttpServer} is RUNNING if it is ready to accept requests.
   *
   * @since 6.0.0
   */
  boolean isRunning();

  /**
   * Returns whether this {@code KafkaHttpServer} is in state STOPPING.
   *
   * <p>A {@code KafkaHttpServer} is STOPPING if {@link #stop()} has been called, but the server has
   * not completed shutdown yet (e.g. if it is in lame-duck mode).
   *
   * @since 6.0.0
   */
  boolean isStopping();

  /**
   * Returns whether this {@code KafkaHttpServer} is in state TERMINATED.
   *
   * <p>A {@code KafkaHttpServer} is TERMINATED if the server has completed shutdown after {@link
   * #stop()} being called.
   *
   * @since 6.0.0
   */
  boolean isTerminated();

  /**
   * Returns whether this {@code KafkaHttpServer} is in state FAILED.
   *
   * <p>A {@code KafkaHttpServer} is FAILED if it has finished execution due to an unrecoverable
   * error.
   *
   * @since 6.0.0
   */
  boolean isFailed();

  /**
   * Starts this {@code KafkaHttpServer}.
   *
   * <p>This method does not wait for the server to reach state RUNNING. Use {@link #awaitStarted()}
   * or {@link #awaitStarted(Duration)} after calling this method if you want to wait.
   *
   * @since 6.0.0
   */
  void start();

  /**
   * Stops this {@code KafkaHttpServer}.
   *
   * <p>This method does not wait for the server to reach state TERMINATED. Use {@link
   * #awaitStopped()} or {@link #awaitStopped(Duration)} after calling this method if you want
   * to wait.
   *
   * @since 6.0.0
   */
  void stop();

  /**
   * Returns the Throwable was raised transitioning the server to state FAILED.
   *
   * @since 6.0.0
   */
  Optional<Throwable> getError();

  /**
   * Waits until this {@code KafkaHttpServer} reaches either state RUNNING, or a state from which
   * RUNNING is unreachable.
   *
   * <p>This method will cause the current thread to become disabled for thread scheduling
   * purposes.
   *
   * @throws InterruptedException if the current thread is interrupted while waiting
   *
   * @since 6.0.0
   */
  void awaitStarted() throws InterruptedException;

  /**
   * Waits until this {@code KafkaHttpServer} either reaches state RUNNING, reaches a state from
   * which RUNNING is unreachable, or the timeout is reached.
   *
   * <p>This method will cause the current thread to become disabled for thread scheduling
   * purposes.
   *
   * @return true if the wait completed before the timeout, false otherwise
   * @throws InterruptedException if the current thread is interrupted while waiting
   *
   * @since 6.0.0
   */
  boolean awaitStarted(Duration timeout) throws InterruptedException;

  /**
   * Waits until this {@code KafkaHttpServer} reaches either state TERMINATED, or a state from which
   * TERMINATED is unreachable.
   *
   * <p>This method will cause the current thread to become disabled for thread scheduling
   * purposes.
   *
   * @throws InterruptedException if the current thread is interrupted while waiting
   *
   * @since 6.0.0
   */
  void awaitStopped() throws InterruptedException;

  /**
   * Waits until this {@code KafkaHttpServer} either reaches state TERMINATED, reaches a state from
   * which TERMINATED is unreachable, or the timeout is reached.
   *
   * <p>This method will cause the current thread to become disabled for thread scheduling
   * purposes.
   *
   * @return true if the wait completed before the timeout, false otherwise
   * @throws InterruptedException if the current thread is interrupted while waiting
   *
   * @since 6.0.0
   */
  boolean awaitStopped(Duration timeout) throws InterruptedException;
}
