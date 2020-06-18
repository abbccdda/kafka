/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.brokerremoval;


import com.linkedin.kafka.cruisecontrol.executor.Executor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BrokerRemovalFutureTest {

  AtomicReference<Executor.ReservationHandle> reservationHandle;

  @Mock
  Executor.ReservationHandle handle;

  @Mock
  CompletableFuture<Void> initialFuture;

  @Mock
  CompletableFuture<Future<?>> chainedFutures;

  private final Duration duration = Duration.ofSeconds(1);

  private BrokerRemovalFuture future;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    reservationHandle = new AtomicReference<>(handle);
    future = new BrokerRemovalFuture(reservationHandle, initialFuture, chainedFutures);
  }

  @Test
  public void testExecute_StartsExecution() throws Throwable {
    future.execute(duration);

    verify(handle).close();
    verify(initialFuture).complete(null);
    verify(chainedFutures).get(duration.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Test
  public void testExecute_ReThrowsCause() throws Throwable {
    Exception expectedException = new Exception("boom");
    ExecutionException executionException = new ExecutionException("", expectedException);
    when(chainedFutures.get(duration.toMillis(), TimeUnit.MILLISECONDS)).thenThrow(executionException);

    Exception receivedException = assertThrows(Exception.class, () -> future.execute(duration));

    assertEquals(expectedException, receivedException);
  }

  @Test
  public void testCancel_CancelsChainedFutures() {
    when(chainedFutures.cancel(true)).thenReturn(true);
    boolean futureCanceled = future.cancel();

    assertTrue("Expected the future to be canceled", futureCanceled);
  }

  /**
   * If the execution proposal submission future is done,
   * we should try to cancel the underlying execution proposal future
   */
  @Test
  public void testCancel_CancelsExecutionFutureAndStopsExecution() throws ExecutionException, InterruptedException {
    when(chainedFutures.cancel(true)).thenReturn(false);
    when(chainedFutures.isDone()).thenReturn(true);
    Future futureMock = mock(Future.class);
    when(chainedFutures.get()).thenAnswer((Answer<Object>) invocation -> futureMock);
    when(futureMock.cancel(true)).thenReturn(true);

    // act
    boolean futureCanceled = future.cancel();

    assertTrue("Expected the future to be canceled", futureCanceled);
    verify(handle).stopExecution();
    verify(chainedFutures).get();
    verify(futureMock).cancel(true);
  }

  @Test
  public void testCancel_DoesntThrowException() throws ExecutionException, InterruptedException {
    when(chainedFutures.cancel(true)).thenReturn(false);
    when(chainedFutures.isDone()).thenReturn(true);
    Future futureMock = mock(Future.class);
    when(chainedFutures.get()).thenThrow(new ExecutionException("boom", null));
    when(futureMock.cancel(true)).thenReturn(true);

    // act
    boolean futureCanceled = future.cancel();

    assertFalse("Expected the future to not be canceled", futureCanceled);
  }
}
