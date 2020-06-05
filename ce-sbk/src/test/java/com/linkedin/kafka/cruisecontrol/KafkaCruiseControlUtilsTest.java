/*
 * Copyright (c) 2020. Confluent, Inc.
 */

package com.linkedin.kafka.cruisecontrol;

import org.apache.kafka.common.utils.Time;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;

public class KafkaCruiseControlUtilsTest {
    private final int MAX_WAIT_MS = 1000;
    private final int INIT_WAIT_MS = 100;
    private final int MAX_RETRIES = 10;

    @SuppressWarnings("unchecked")
    @Test
    public void testBackoff_success() throws InterruptedException, TimeoutException {
        Time mockTime = Mockito.mock(Time.class);
        Supplier<Boolean> f = (Supplier<Boolean>) Mockito.mock(Supplier.class);
        Mockito.when(f.get()).thenReturn(true);

        KafkaCruiseControlUtils.backoff(f, MAX_RETRIES, INIT_WAIT_MS, MAX_WAIT_MS, mockTime);
        Mockito.verify(f, Mockito.times(1)).get();
        Mockito.verify(mockTime, Mockito.never()).sleep(anyLong());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBackoff_fails()  {
        Time mockTime = Mockito.mock(Time.class);
        Supplier<Boolean> f = (Supplier<Boolean>) Mockito.mock(Supplier.class);
        Mockito.when(f.get()).thenReturn(false);
        ArgumentCaptor<Long> sleepArgs = ArgumentCaptor.forClass(Long.class);

        assertThrows(TimeoutException.class,
                () -> KafkaCruiseControlUtils.backoff(f, MAX_RETRIES, INIT_WAIT_MS, MAX_WAIT_MS, mockTime));
        Mockito.verify(f, Mockito.times(MAX_RETRIES)).get();
        Mockito.verify(mockTime, Mockito.times(MAX_RETRIES - 1)).sleep(sleepArgs.capture());
        List<Long> sleepTimes = sleepArgs.getAllValues();
        long expected = INIT_WAIT_MS;
        for (long actual : sleepTimes) {
            assertEquals(Math.min(expected, MAX_WAIT_MS), actual);
            expected *= 2;
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBackoff_succeedEventually() throws InterruptedException, TimeoutException {
        Time mockTime = Mockito.mock(Time.class);
        Supplier<Boolean> f = (Supplier<Boolean>) Mockito.mock(Supplier.class);
        Mockito.when(f.get()).thenReturn(false, false, true);

        KafkaCruiseControlUtils.backoff(f, MAX_RETRIES, INIT_WAIT_MS, MAX_WAIT_MS, mockTime);
        Mockito.verify(f, Mockito.times(3)).get();
    }
}