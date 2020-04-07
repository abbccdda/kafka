// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents an authorizable operation, e.g. Read, Write. This includes all Kafka operations
 * and additional operations may be added dynamically. Authorizers don't check the validity of
 * operations against resource types, authorization succeeds if an access rule matches.
 */
public class Operation {
  public static final Operation ALL = new Operation("All");
  private static final Map<Operation, Collection<Operation>> IMPLICIT_ALLOWED_OPS;

  private final String name;

  static {
    IMPLICIT_ALLOWED_OPS = new HashMap<>();
    IMPLICIT_ALLOWED_OPS.put(new Operation("Describe"),
        Stream.of("Read", "Write", "Delete", "Alter")
            .map(Operation::new).collect(Collectors.toSet()));
    IMPLICIT_ALLOWED_OPS.put(new Operation("DescribeConfigs"),
        Collections.singleton(new Operation("AlterConfigs")));
  }

  public Operation(String name) {
    this.name = name;
  }

  public String name() {
    return name;
  }

  public Collection<Operation> allowOps() {
    return IMPLICIT_ALLOWED_OPS.getOrDefault(this, Collections.emptySet());
  }

  /**
   * Returns true if this operation in an access rule can be used for authorizing the provided operation.
   * Allowing read, write, delete, or alter implies allowing describe.
   * See {@link org.apache.kafka.common.acl.AclOperation} for more details about ACL inheritance.
   *
   * @param requestedOp The operation being authorized
   * @param permissionType Permission type
   */
  public boolean matches(Operation requestedOp, PermissionType permissionType) {
    return this.equals(Operation.ALL) || this.equals(requestedOp) ||
        (permissionType == PermissionType.ALLOW && requestedOp.allowOps().contains(this));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Operation)) {
      return false;
    }

    Operation that = (Operation) o;
    return Objects.equals(this.name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return name;
  }
}
