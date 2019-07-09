package io.confluent.telemetry.collector;

import com.google.common.base.Preconditions;
import io.confluent.telemetry.MetricKey;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A LastValueTracker uses a ConcurrentMap to maintain historic values for a given key, and return
 * a previous value and an Instant for that value.
 *
 * @param <T> The type of the value.
 */
public class LastValueTracker<T> {
  private final ConcurrentMap<MetricKey, AtomicReference<InstantAndValue<T>>> counters = new ConcurrentHashMap<>();


  /**
   * Return the last instant/value for the given MetricKey, or Optional.empty if there isn't one.
   *
   * @param metricKey the key for which to calculate a getAndSet.
   * @param now the timestamp for the new value.
   * @param value the current value.
   * @return the timestamp of the previous entry and its value. If there
   *     isn't a previous entry, then this method returns {@link Optional#empty()}
   */
  public Optional<InstantAndValue<T>> getAndSet(MetricKey metricKey, Instant now, T value) {
    InstantAndValue<T> instantAndValue = new InstantAndValue<>(now, value);
    AtomicReference<InstantAndValue<T>> valueOrNull = counters
        .putIfAbsent(metricKey, new AtomicReference<>(instantAndValue));

    // there wasn't already an entry, so return empty.
    if (valueOrNull == null) {
      return Optional.empty();
    }

    // Update the atomic ref to point to our new InstantAndValue, but get the previous value
    InstantAndValue<T> previousValue = valueOrNull.getAndSet(instantAndValue);

    // Return the instance and the value.
    return Optional.of(new InstantAndValue<>(previousValue.getIntervalStart(), previousValue.getValue()));
  }

  public AtomicReference<InstantAndValue<T>> remove(MetricKey metricKey) {
    return counters.remove(metricKey);
  }

  public static class InstantAndValue<T> {

    private final Instant intervalStart;
    private final T value;

    public InstantAndValue(Instant intervalStart, T value) {
      Preconditions.checkNotNull(intervalStart, "intervalStart cannot be null");
      Preconditions.checkNotNull(value,  "value cannot be null");
      this.intervalStart = intervalStart;
      this.value = value;
    }

    public Instant getIntervalStart() {
      return intervalStart;
    }

    public T getValue() {
      return value;
    }
  }

}
