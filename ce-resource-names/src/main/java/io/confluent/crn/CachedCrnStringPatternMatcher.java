package io.confluent.crn;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * In practice, we will usually be routing messages based on the string CRN values in CloudEvent
 * subject fields. This cache avoids the expense of parsing those strings into
 * ConfluentResourceNames, and the expense of re-evaluating the CRN against a probably-unchanging
 * set of rules.
 * <p>
 * We assume that the number of unique subjects that we'll route based on is not unreasonably large.
 * There may be, eg. tens of thousands of topics, but not billions. We use an LRU to make sure we
 * don't expand the cache indefinitely.
 */
public class CachedCrnStringPatternMatcher<T> {

  private final CrnPatternMatcher<T> matcher;
  private final int capacity;
  private final Map<String, Optional<Entry<ConfluentResourceName, T>>> cache;

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  /**
   * Create a cache with the desired capacity. The capacity should be tuned to the number of unique
   * CRNs that will be matched, not the number of patterns they will be matched against. If you have
   * 1000 different topics, but 10 rules, the capacity should be > 1000.
   */
  public CachedCrnStringPatternMatcher(CrnPatternMatcher<T> matcher, int capacity) {
    this.matcher = matcher;
    this.capacity = capacity;
    this.cache = Collections.synchronizedMap(
        new LinkedHashMap<String, Optional<Entry<ConfluentResourceName, T>>>(
            this.capacity, 0.75f, true) {
          protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > capacity;
          }
        });
  }

  /**
   * Return both the pattern in the cache that matched (as the key) and the value (as the value).
   * <p>
   * This returns an empty optional, rather than throwing an exeception if the crnString is not
   * valid
   */
  @VisibleForTesting
  Optional<Entry<ConfluentResourceName, T>> matchEntry(String crnString) {
    return cache.computeIfAbsent(crnString, k -> {
      try {
        return Optional.ofNullable(matcher.matchEntry(ConfluentResourceName.fromString(crnString)));
      } catch (CrnSyntaxException e) {
        return Optional.empty();
      }
    });
  }

  /**
   * Return the stored value for this CRN, or null if there is no match
   */
  public T match(String crnString) {
    Optional<Entry<ConfluentResourceName, T>> entry = matchEntry(crnString);
    return entry.map(Entry::getValue).orElse(null);
  }

  public int size() {
    return cache.size();
  }

  @Override
  public String toString() {
    return "CachedCrnStringPatternMatcher(matcher=" + matcher + ")";
  }

  public static class Builder<T> {

    private Integer capacity = null;
    private Map<String, T> patterns = new HashMap<>();

    public Builder<T> capacity(int capacity) {
      this.capacity = capacity;
      return this;
    }

    public Builder<T> setPattern(String crnString, T value) {
      patterns.put(crnString, value);
      return this;
    }

    public CachedCrnStringPatternMatcher<T> build() throws CrnSyntaxException {
      CrnPatternMatcher.Builder<T> builder = CrnPatternMatcher.builder();
      for (Entry<String, T> entry : patterns.entrySet()) {
        builder.setPattern(ConfluentResourceName.fromString(entry.getKey()), entry.getValue());
      }
      return new CachedCrnStringPatternMatcher<>(builder.build(), capacity);
    }
  }
}
