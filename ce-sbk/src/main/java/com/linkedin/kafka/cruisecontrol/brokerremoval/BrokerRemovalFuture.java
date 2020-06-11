/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.brokerremoval;

import com.linkedin.kafka.cruisecontrol.executor.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A class that encapsulates the multiple futures and handles that compose a running broker removal operation.
 * This class provides a clean interface for executing and canceling a broker removal operation
 */
public class BrokerRemovalFuture {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerRemovalFuture.class);

  private AtomicReference<Executor.ReservationHandle> reservationHandle;
  private CompletableFuture<Void> initialFuture;
  private final CompletableFuture<Future<?>> chainedFutures;


  /**
   * @param chainedFutures a #{@link Future<Future>} consisting of the full broker removal operation -
   *                       the plan computation/shutdown operation and then the underlying reassignments execution
   */
  public BrokerRemovalFuture(AtomicReference<Executor.ReservationHandle> reservationHandle,
                             CompletableFuture<Void> initialFuture,
                             CompletableFuture<Future<?>> chainedFutures) {
    this.reservationHandle = reservationHandle;
    this.initialFuture = initialFuture;
    this.chainedFutures = chainedFutures;
  }

  /**
   * Executes the broker removal
   */
  public void execute(Duration duration) throws Throwable {
    try {
      initialFuture.complete(null);
      chainedFutures.get(duration.toMillis(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw e.getCause();
    } finally {
      Executor.ReservationHandle handle = reservationHandle.get();
      if (handle != null) {
        handle.close();
      }
    }
  }

  /**
   * Attempt to cancel the broker removal operation future. It is a nested future which consists of the
   *  outer-level removal operation (plan computation and broker shutdown)
   *  and the inner-level operation (plan reassignment execution)
   */
  public boolean cancel() {
    boolean wasCanceled = chainedFutures.cancel(true);
    if (wasCanceled) {
      LOG.info("Successfully canceled the broker removal shutdown and execution proposal submission task");
    } else if (chainedFutures.isDone()) {
      LOG.debug("The broker shutdown and execution proposal submission task is done. Cancelling the execution proposal task.");
      reservationHandle.get().stopExecution();
      LOG.info("Signalled Executor stop execution.");

      try {
        Future<?> executorFuture = chainedFutures.get();

        boolean executorFutureWasCanceled = executorFuture.cancel(true);
        if (executorFutureWasCanceled) {
          LOG.info("Successfully canceled the execution proposal task.");
        } else {
          LOG.warn("Could not cancel the execution proposal task, presumably because it was already done (isDone {})", executorFuture.isDone());
        }
        return executorFutureWasCanceled;
      } catch (InterruptedException | ExecutionException e) {
        // should not occur as the future is done
        LOG.error("Could not cancel the execution proposal future due to ", e);
      }
    } else {
      LOG.error("Did not manage to cancel the broker removal shutdown and execution proposal submission task");
    }

    return wasCanceled;
  }
}
