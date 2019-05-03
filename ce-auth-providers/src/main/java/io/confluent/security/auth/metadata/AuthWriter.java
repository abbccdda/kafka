// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourcePatternFilter;
import io.confluent.security.authorizer.Scope;
import java.util.Collection;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Writer interface used by Metadata Server to update role bindings. All update methods are
 * asynchronous and the returned future completes when the update has been written to
 * log, acknowledged and has been consumed by the local reader. Update methods may block for
 * writer to be ready if a rebalance is in progress. Incremental update methods will also block
 * until local cache is up-to-date.
 */
public interface AuthWriter {

  /**
   * Adds a new cluster-level role binding without any resources.
   * <p>
   * Requestor should have Alter permission for SecurityMetadata to perform this operation.
   *
   * @param principal User or group principal to which role is assigned
   * @param role Name of role
   * @param scope Scope at which role is assigned
   * @return a stage that is completed when update completes
   * @throws IllegalArgumentException if the specified role has resource-level scope
   */
  CompletionStage<Void> addClusterRoleBinding(KafkaPrincipal principal, String role, Scope scope);

  /**
   * Adds resources to a role binding. If the role is not already assigned to the principal, an
   * binding will be added with the specified resources. If an binding exists, the provided
   * roles will be added to the list of resources. This method will block until the local cache is
   * up-to-date and the new binding is queued for update with the updated resources.
   * <p>
   * Requestor should have AlterAccess permission for the specified resources to perform this operation.
   *
   * @param principal User or group principal to which role is assigned
   * @param role Name of role
   * @param scope Scope at which role is assigned
   * @param resources Resources to add to role binding
   * @return a stage that is completed when update completes
   * @throws IllegalArgumentException if the specified role has cluster-level scope
   */
  CompletionStage<Void> addResourceRoleBinding(KafkaPrincipal principal, String role, Scope scope, Collection<ResourcePattern> resources);

  /**
   * Removes a role binding. If the specified role has resource-level scope, role
   * binding is removed for all assigned resources.
   * <p>
   * Requestor should have Alter permission for SecurityMetadata to perform this operation.
   *
   * @param principal User or group principal from which role is removed
   * @param role Name of role
   * @param scope Scope at which role is assigned
   * @return a stage that is completed when update completes
   */
  CompletionStage<Void> removeRoleBinding(KafkaPrincipal principal, String role, Scope scope);

  /**
   * Removes resources from an existing role binding. If the principal has no more resources for
   * the role at the specified scope, the role binding is deleted. This method will block until the
   * local cache is up-to-date and a new binding is queued with the updated resources.
   * <p>
   * Requestor should have AlterAccess permission for the specified resources to perform this operation.
   *
   * @param principal User or group principal from which role is removed
   * @param role Name of role
   * @param scope Scope at which role is assigned
   * @param resources Filter for resources being removed for the role binding
   * @return a stage that is completed when update completes
   * @throws IllegalArgumentException if the specified role has cluster-level scope
   */
  CompletionStage<Void> removeResourceRoleBinding(KafkaPrincipal principal, String role, Scope scope, Collection<ResourcePatternFilter> resources);

  /**
   * Sets resources for an existing role binding. If the role is not assigned to the principal,
   * a new role binding is created with the provided set of resources.
   * <p>
   * Requestor should have Alter permission for SecurityMetadata to perform this operation.
   *
   * @param principal User or group principal to which role is assigned
   * @param role Name of role
   * @param scope Scope at which role is assigned
   * @param resources Updated collection of resources for the role binding
   * @return a stage that is completed when update completes
   * @throws IllegalArgumentException if the specified role has cluster-level scope
   */
  CompletionStage<Void> replaceResourceRoleBinding(KafkaPrincipal principal, String role, Scope scope, Collection<ResourcePattern> resources);

}
