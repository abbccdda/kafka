/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.brokerremoval;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlTest;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * A class to unit test the #{@link BrokerRemovalPhaseExecutor} directly.
 * Full coverage of the class is achieved through the #{@link KafkaCruiseControlTest}
 */
public class BrokerRemovalPhaseExecutorTest {

  @Mock
  private BrokerRemovalCallback mockRemovalCallback;

  @Mock
  private BrokerRemovalOptions mockOptions;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testExecute_throwsInterruptedExceptionWithoutAnyMoreAction() {
    BrokerRemovalPhaseExecutor executor = new BrokerRemovalPhaseExecutor.Builder(
        null, BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE,
        brokerIds -> String.format("Unexpected exception while submitting the broker removal plan for broker %s", brokerIds),
        KafkaCruiseControlException.class
    ).build(mockRemovalCallback, mockOptions);
    InterruptedException expectedException = new InterruptedException("Interrupted!");

    CompletionException thrownException = assertThrows(CompletionException.class, () ->
      executor.execute(args -> {
        throw expectedException;
      }).join()
    );

    assertEquals(expectedException, thrownException.getCause());
    verifyNoInteractions(mockRemovalCallback);
  }

  /**
   * If the #{@link BrokerRemovalPhase} throws an exception, the executor should catch the exception
   * and register the failure in the #{@link BrokerRemovalCallback},
   * then wraps it in the expected exception and completes the future exceptionally.
   */
  @Test
  public void testExecute_handlesExceptions() {
    Function<Set<Integer>, String> errMsgSupplier = brokerIds -> String.format("Unexpected exception while submitting the broker removal plan for broker %s", brokerIds);
    String expectedExceptionMessage = errMsgSupplier.apply(null);
    KafkaCruiseControlException expectedException = new KafkaCruiseControlException(expectedExceptionMessage);

    BrokerRemovalPhaseExecutor executor = new BrokerRemovalPhaseExecutor.Builder(
        null, BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE,
        errMsgSupplier,
        expectedException.getClass()
    ).build(mockRemovalCallback, mockOptions);

    CompletionException thrownException = assertThrows(CompletionException.class, () ->
        executor.execute(args -> {
          throw new Exception("a");
        }).join()
    );

    assertEquals(expectedException.getMessage(), thrownException.getCause().getMessage());
    assertEquals(expectedException.getClass(), thrownException.getCause().getClass());
    verify(mockRemovalCallback)
        .registerEvent(eq(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE), any(Exception.class));
  }

  /**
   * If the #{@link BrokerRemovalPhase} throws an exception, the executor should catch the exception
   * and register the failure in the #{@link BrokerRemovalCallback}.
   * If the callback throws an exception when we're registering said failure in it (i.e due to persistence),
   * we should handle it and still complete the future
   */
  @Test
  public void testExecute_handlesExceptionInCallback() {
    Function<Set<Integer>, String> errMsgSupplier = brokerIds -> String.format("Unexpected exception while submitting the broker removal plan for broker %s", brokerIds);
    BrokerRemovalPhaseExecutor executor = new BrokerRemovalPhaseExecutor.Builder(
        null, BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE,
        errMsgSupplier,
        null
    ).build(mockRemovalCallback, mockOptions);
    doAnswer(invocation -> {
      throw new InterruptedException("Interrupted while persisting progress!");
    }).when(mockRemovalCallback).registerEvent(any(), any());

    Exception expectedException = new Exception("a");

    ExecutionException thrownException = assertThrows(ExecutionException.class, () ->
        executor.execute(args -> {
          throw expectedException;
        }).get(100, TimeUnit.MILLISECONDS)
    );

    assertEquals(expectedException.getMessage(), thrownException.getCause().getMessage());
    assertEquals(expectedException.getClass(), thrownException.getCause().getClass());
    verify(mockRemovalCallback)
        .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE, expectedException);
  }
}
