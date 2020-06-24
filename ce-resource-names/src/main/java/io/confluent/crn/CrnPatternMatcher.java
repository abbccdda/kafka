package io.confluent.crn;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

public class CrnPatternMatcher<T> {

  HashMap<String, NavigableMap<ConfluentResourceName, T>> patternsByResourceType;

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  private CrnPatternMatcher(Map<String, Map<ConfluentResourceName, T>> patternsByResourceType) {
    this.patternsByResourceType = new HashMap<>();
    patternsByResourceType.forEach((resourceType, patterns) -> {
      this.patternsByResourceType.put(resourceType, new TreeMap<>(patterns));
    });
  }

  public T match(ConfluentResourceName crn) {
    Entry<ConfluentResourceName, T> entry = matchEntry(crn);
    if (entry == null) {
      return null;
    }
    return entry.getValue();
  }

  /**
   * This returns both the pattern that matched and the value it matched
   */
  public Entry<ConfluentResourceName, T> matchEntry(ConfluentResourceName crn) {
    NavigableMap<ConfluentResourceName, T> skipListMap =
        patternsByResourceType.get(crn.resourceType());
    if (skipListMap == null) {
      return null;
    }
    // CRNs sort with wildcards < literals, so all possible matches fall between
    // the allWildcards version of a CRN and the CRN itself. We consider them in
    // descending order: most specific to most general
    NavigableMap<ConfluentResourceName, T> candidates = skipListMap
        .subMap(crn.allWildcards(), true, crn, true).descendingMap();

    for (Entry<ConfluentResourceName, T> candidate : candidates.entrySet()) {
      if (candidate.getKey().matches(crn)) {
        return candidate;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return "CrnPatternMatcher(" + patternsByResourceType + ")";
  }

  public static class Builder<T> {

    Map<String, Map<ConfluentResourceName, T>> patternsByResourceType = new HashMap<>();

    public Builder<T> setPattern(ConfluentResourceName crn, T value) {
      patternsByResourceType.computeIfAbsent(crn.resourceType(),
          k -> new HashMap<>()).put(crn, value);
      return this;
    }

    public CrnPatternMatcher<T> build() {
      return new CrnPatternMatcher<>(patternsByResourceType);
    }
  }
}
