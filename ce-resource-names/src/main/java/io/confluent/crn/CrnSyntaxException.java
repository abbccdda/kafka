/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.crn;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.stream.Collectors;

public class CrnSyntaxException extends URISyntaxException {

  public CrnSyntaxException(String input, String reason) {
    super(input, reason);
  }

  /**
   * If we encountered multiple problems trying to parse a CRN, produce an exception that captures
   * all of the reasons for failure
   */
  public CrnSyntaxException(String input, Collection<CrnSyntaxException> exceptions) {
    super(input,
        exceptions.stream()
            .map(URISyntaxException::getReason)
            .collect(Collectors.joining("; "))
    );
  }
}
