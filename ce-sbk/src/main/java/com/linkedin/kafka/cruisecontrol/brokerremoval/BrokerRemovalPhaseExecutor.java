/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.brokerremoval;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A #{@link BrokerRemovalPhaseExecutor} is a wrapper encapsulating the repeatable pattern of each broker removal phase execution.
 * Namely, the pattern consists of executing an action and depending on the result (exceptional or not), registering
 * a different #{@link BrokerRemovalCallback.BrokerRemovalEvent}
 * and, if exceptional, abandoning subsequent phase execution.
 *
 * For more information regarding the broker removal phases, see #{@link KafkaCruiseControl#removeBroker(int, Optional, BrokerRemovalCallback, String)}
 */
public class BrokerRemovalPhaseExecutor {
  private final BrokerRemovalCallback progressCallback;
  private BrokerRemovalOptions removalArgs;
  // nullable
  private final BrokerRemovalCallback.BrokerRemovalEvent successEvent;
  private final BrokerRemovalCallback.BrokerRemovalEvent failureEvent;
  private Function<Set<Integer>, String> errMsgSupplier;
  // nullable
  private Class<? extends Exception> exceptionWrapper;
  private static final Logger LOG = LoggerFactory.getLogger(BrokerRemovalPhaseExecutor.class);

  public static class Builder {
    private final BrokerRemovalCallback.BrokerRemovalEvent successEvent;
    private final BrokerRemovalCallback.BrokerRemovalEvent failureEvent;
    private final Function<Set<Integer>, String> errMsgSupplier;
    private final Class<? extends Exception> exceptionWrapper;

    public Builder(BrokerRemovalCallback.BrokerRemovalEvent successEvent,
                   BrokerRemovalCallback.BrokerRemovalEvent failureEvent,
                   Function<Set<Integer>, String> errMsgSupplier) {
      this(successEvent, failureEvent, errMsgSupplier, null);
    }

    /**
     * @param successEvent - nullable, the broker removal event that gets registered on successful phase completion
     * @param failureEvent - the broker removal event that gets registered on an exceptional phase completion
     * @param errMsgSupplier - a function that accepts a #{@code Set} of brokerIds as an argument
     *                       and returns a descriptive error message explaining what phase failed
     * @param exceptionWrapper - a nullable (optional) class to wrap any thrown exception as
     */
    public Builder(BrokerRemovalCallback.BrokerRemovalEvent successEvent,
                   BrokerRemovalCallback.BrokerRemovalEvent failureEvent,
                   Function<Set<Integer>, String> errMsgSupplier,
                   Class<? extends Exception> exceptionWrapper) {
      this.successEvent = successEvent;
      this.failureEvent = failureEvent;
      this.errMsgSupplier = errMsgSupplier;
      this.exceptionWrapper = exceptionWrapper;
    }

    public BrokerRemovalPhaseExecutor build(BrokerRemovalCallback progressCallback, BrokerRemovalOptions removalArgs) {
      return new BrokerRemovalPhaseExecutor(progressCallback, removalArgs,
          successEvent, failureEvent, errMsgSupplier, exceptionWrapper);
    }
  }

  private BrokerRemovalPhaseExecutor(BrokerRemovalCallback progressCallback,
                                    BrokerRemovalOptions removalArgs,
                                    BrokerRemovalCallback.BrokerRemovalEvent successEvent,
                                    BrokerRemovalCallback.BrokerRemovalEvent failureEvent,
                                    Function<Set<Integer>, String> errMsgSupplier,
                                    Class<? extends Exception> exceptionWrapper) {

    this.progressCallback = progressCallback;
    this.removalArgs = removalArgs;
    this.successEvent = successEvent;
    this.failureEvent = failureEvent;
    this.errMsgSupplier = errMsgSupplier;
    this.exceptionWrapper = exceptionWrapper;
  }

  /**
   * Executes the given #{@link BrokerRemovalPhase}, notifies the progress callback and completes the future.
   *
   * In case a #{@link InterruptedException} is thrown by the underlying code, the future will
   * immediately populated with the exception and returned.
   * Callers are expected to unwrap and handle the interruption appropriately.
   *
   * @return the completed #{@link CompletableFuture} of the removal step
   */
  public CompletableFuture<Void> execute(BrokerRemovalPhase phase) {
    CompletableFuture<Void> future = new CompletableFuture<>();

    try {
      phase.execute(removalArgs);
      if (successEvent != null) {
        progressCallback.registerEvent(successEvent);
      }
      future.complete(null);
    } catch (InterruptedException ie) {
      future.completeExceptionally(ie);
    } catch (Exception e) {
      String errMsg = errMsgSupplier.apply(removalArgs.brokersToRemove);
      LOG.error(errMsg, e);
      Exception exception = maybeWrapException(e, errMsg);
      try {
        progressCallback.registerEvent(failureEvent, exception);
      } catch (Exception registerException) {
        LOG.error("Caught exception while registering the {} event with failure {}. Cause: ", failureEvent, e, registerException);
      }

      future.completeExceptionally(exception);
    }

    return future;
  }

  private Exception maybeWrapException(Exception e, String errMsg) {
    if (exceptionWrapper != null) {
      try {
        Exception newException = exceptionWrapper.getDeclaredConstructor(String.class, Throwable.class).newInstance(errMsg, e);
        e = newException;
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
        // should be unreachable code due to the class inheriting from Exception
        LOG.error("Received an unexpected reflection exception when executing a broker removal phase.", e);
      }
    }
    return e;
  }
}
