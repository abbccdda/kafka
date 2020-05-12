// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.kafka.link;

import io.confluent.kafka.multitenant.schema.TransformContext;

public class LinkContext implements TransformContext {

  private final String destPrefix;
  private final int destPrefixLen;

  public LinkContext(String destPrefix) {
    this.destPrefix = destPrefix;
    this.destPrefixLen = destPrefix.length();
  }

  public String destPrefix() {
    return destPrefix;
  }

  public String destToSource(String name) {
    if (!name.startsWith(destPrefix))
      throw new IllegalStateException("Name does not start with prefix " + destPrefix + " : " + name);
    return name.substring(destPrefixLen);
  }

  public String sourceToDest(String name) {
    return destPrefix + name;
  }

  public int destToSourceDeltaBytes() {
    return -destPrefixLen;
  }
}
