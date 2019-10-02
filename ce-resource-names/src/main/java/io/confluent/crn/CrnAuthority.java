/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.crn;

/**
 * A CrnAuthority is responsible for translating between resources and CRNs.
 */
public interface CrnAuthority {

  /**
   * The name of this Authority. This name should be unique and consistent across
   * all of the services controlled by this authority. It is recommended that this
   * be the DNS name for the Metadata Service that controls authorization for these
   * services.
   */
  String name();

  /**
   * Return the canonical CRN for the String
   * @throws CrnSyntaxException if there are misnamed elements in the scope
   */
  ConfluentResourceName canonicalCrn(String crnString) throws CrnSyntaxException;

  /**
   * Return the canonical CRN for the given CRN
   * @throws CrnSyntaxException if there are misnamed elements in the scope
   */
  ConfluentResourceName canonicalCrn(ConfluentResourceName crn) throws CrnSyntaxException;

  /**
   * Return true iff the two CRNs refer to the same resource(s)
   */
  boolean areEquivalent(ConfluentResourceName a, ConfluentResourceName b)
      throws CrnSyntaxException;
}
