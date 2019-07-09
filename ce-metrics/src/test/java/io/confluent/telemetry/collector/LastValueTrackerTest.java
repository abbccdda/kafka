package io.confluent.telemetry.collector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.collector.LastValueTracker.InstantAndValue;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import org.junit.Test;

public class LastValueTrackerTest {

  private static final MetricKey METRIC_NAME = new MetricKey("test-metric", Collections.emptyMap());


  Instant instant1 = Instant.now();
  Instant instant2 = instant1.plusMillis(1);

  @Test
  public void deltaDoubleOnePoint() {
    LastValueTracker<Double> lastValueTracker = new LastValueTracker<>();
    Optional<InstantAndValue<Double>> result = lastValueTracker.getAndSet(METRIC_NAME, instant1, 1d);
    assertFalse(result.isPresent());
  }
  @Test
  public void deltaDoubleTwoPoints() {
    LastValueTracker<Double> lastValueTracker = new LastValueTracker<>();
    lastValueTracker.getAndSet(METRIC_NAME, instant1, 1d);
    Optional<InstantAndValue<Double>> result = lastValueTracker
        .getAndSet(METRIC_NAME, instant2, 1000d);

    assertTrue(result.isPresent());
    assertEquals(instant1, result.get().getIntervalStart());
    assertEquals(1d, result.get().getValue(), 1e-6);
  }


  @Test
  public void deltaLongOnePoint() {
    LastValueTracker<Long> lastValueTracker = new LastValueTracker<>();
    Optional<InstantAndValue<Long>> result = lastValueTracker.getAndSet(METRIC_NAME, instant1, 1L);
    assertFalse(result.isPresent());
  }

  @Test
  public void deltaLongTwoPoints() {
    LastValueTracker<Long> lastValueTracker = new LastValueTracker<>();
    lastValueTracker.getAndSet(METRIC_NAME, instant1, 2L);
    Optional<InstantAndValue<Long>> result = lastValueTracker
        .getAndSet(METRIC_NAME, instant2, 10000L);

    assertTrue(result.isPresent());
    assertEquals(instant1, result.get().getIntervalStart());
    assertEquals(2L, result.get().getValue().longValue());
  }
}