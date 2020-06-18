// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.security.authorizer;

/**
 * ScopeType is used to identify what contexts an AccessPolicy should be applied
 */
public enum ScopeType {
    // These are ordered specific-to-general
    // The ordering is used to sort elements for multi-level
    // grants, so it must be maintained in order.
    // See Role.mostSpecificScopeType
    UNKNOWN,
    RESOURCE,
    CLUSTER,
    ENVIRONMENT,   // Specific to Confluent Cloud
    ORGANIZATION,  // Specific to Confluent Cloud
    ROOT           // Scope.ROOT_SCOPE: ancestor of all other scopes
}
