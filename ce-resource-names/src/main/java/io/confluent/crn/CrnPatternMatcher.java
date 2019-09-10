package io.confluent.crn;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class CrnPatternMatcher<T> {

  HashMap<String, ConcurrentSkipListMap<ConfluentResourceName, T>> patternsByResourceType = new HashMap<>();

  public void setPattern(ConfluentResourceName crn, T value) {
    patternsByResourceType.computeIfAbsent(crn.resourceType(),
        k -> new ConcurrentSkipListMap<>()).put(crn, value);
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
    ConcurrentSkipListMap<ConfluentResourceName, T> skipListMap =
        patternsByResourceType.get(crn.resourceType());
    if (skipListMap == null) {
      return null;
    }
    // CRNs sort with wildcards < literals, so all possible matches fall between
    // the allWildcards version of a CRN and the CRN itself. We consider them in
    // descending order: most specific to most general
    ConcurrentNavigableMap<ConfluentResourceName, T> candidates = skipListMap
        .subMap(crn.allWildcards(), true, crn, true).descendingMap();

    for (Entry<ConfluentResourceName, T> candidate : candidates.entrySet()) {
      if (candidate.getKey().matches(crn)) {
        return candidate;
      }
    }
    return null;
  }

}
