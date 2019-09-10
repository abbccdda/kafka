package io.confluent.crn;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * In practice, we will usually be routing messages based on the string CRN values in CloudEvent
 * subject fields. This cache avoids the expense of parsing those strings into
 * ConfluentResourceNames, and the expense of re-evaluating the CRN against a probably-unchanging
 * set of rules.
 *
 * We assume that the number of unique subjects that we'll route based on is not unreasonably large.
 * There may be, eg. tens of thousands of topics, but not billions. We use an LRU to make sure we
 * don't expand the cache indefinitely.
 */
public class CachedCrnStringPatternMatcher<T> {

  private final CrnPatternMatcher<T> matcher = new CrnPatternMatcher<>();
  private final int capacity;
  private final Map<String, Entry<ConfluentResourceName, T>> cache;

  /**
   * Create a cache with the desired capacity. The capacity should be tuned to the number of unique
   * CRNs that will be matched, not the number of patterns they will be matched against. If you have
   * 1000 different topics, but 10 rules, the capacity should be > 1000.
   */
  public CachedCrnStringPatternMatcher(int capacity) {
    this.capacity = capacity;
    this.cache = Collections.synchronizedMap(
        new LinkedHashMap<String, Entry<ConfluentResourceName, T>>(
        this.capacity, 0.75f, true) {
      protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > capacity;
      }
    });
  }

  public void setPattern(String crnString, T value) throws CrnSyntaxException {
    matcher.setPattern(ConfluentResourceName.fromString(crnString), value);
    // this invalidates all existing cached values
    cache.clear();
  }

  /**
   * Return both the pattern in the cache that matched (as the key) and the value (as the value).
   *
   * This returns null, rather than throwing an exeception if the crnString is not valid
   */
  public Entry<ConfluentResourceName, T> matchEntry(String crnString) {
    return cache.computeIfAbsent(crnString, k -> {
      try {
        return matcher.matchEntry(ConfluentResourceName.fromString(crnString));
      } catch (CrnSyntaxException e) {
        return null;
      }
    });
  }

  /**
   * Return the stored value for this CRN, or null if there is no match
   */
  public T match(String crnString) {
    Entry<ConfluentResourceName, T> entry = matchEntry(crnString);
    return entry == null ? null : entry.getValue();
  }

  public int size() {
    return cache.size();
  }

}
