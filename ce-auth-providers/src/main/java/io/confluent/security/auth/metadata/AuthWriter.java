// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourcePatternFilter;
import io.confluent.security.authorizer.Scope;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;

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
   * @throws org.apache.kafka.common.errors.InvalidRequestException if the specified role has resource-level scope
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
   * @throws org.apache.kafka.common.errors.InvalidRequestException if the specified role has cluster-level scope
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
   * @throws org.apache.kafka.common.errors.InvalidRequestException if the specified role has cluster-level scope
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
   * @throws org.apache.kafka.common.errors.InvalidRequestException if the specified role has cluster-level scope
   */
  CompletionStage<Void> replaceResourceRoleBinding(KafkaPrincipal principal, String role, Scope scope, Collection<ResourcePattern> resources);

  /**
   * Creates ACL rules for a given AclBinding. This method will block until the local cache is
   * up-to-date and the new binding is queued for update with the updated rules.
   * <p>
   * Requestor should have AlterAccess permission for the specified resources to perform this operation.
   *
   * @param scope Scope at which ACL bindings are added
   * @param aclBinding  AclBinding to add
   * @return a stage that is completed when update completes
   */
  CompletionStage<Void> createAcls(Scope scope, AclBinding aclBinding);

  /**
   * Creates ACLs for the specified scope using the minimal number of batched updates.
   * This method should not block since it is invoked on the broker's request thread while
   * processing AdminClient requests to create ACLs.
   * <p>
   * Requestor should have AlterAccess permission for the specified resources to perform this operation.
   *
   * @param scope Scope at which ACL bindings are added
   * @param aclBindings List of ACLs to create
   * @return completion stages for each binding
   */
  Map<AclBinding, CompletionStage<AclCreateResult>> createAcls(Scope scope, List<AclBinding> aclBindings);

  /**
   * Deletes all ACL rules that match the provided filters. This method will block until the local cache is
   * up-to-date and the new binding is queued for update with the updated rules.
   * <p>
   * Requestor should have AlterAccess permission for the specified resources to perform this operation.
   *
   * @param scope Scope at which ACL bindings are deleted
   * @param aclBindingFilter AclBindingFilter to match the rules
   * @param resourceAccess predicate to check delete permission on resources
   * @return a stage that is completed when update completes
   */
  CompletionStage<Collection<AclBinding>> deleteAcls(Scope scope,
                                                     AclBindingFilter aclBindingFilter,
                                                     Predicate<ResourcePattern> resourceAccess);

  /**
   * Deletes ACLs that match any of the provided filters using batched update.
   * This method should not block since it is invoked on the broker's request thread while
   * processing AdminClient requests to delete ACLs.
   *
   * @param scope Scope at which ACL bindings are added
   * @param aclBindingFilters Filters whose matching ACLs are deleted
   * @param resourceAccess predicate to check delete permission on resources
   * @return completion stages for each matching binding
   */
  Map<AclBindingFilter, CompletionStage<AclDeleteResult>> deleteAcls(Scope scope,
      List<AclBindingFilter> aclBindingFilters,
      Predicate<ResourcePattern> resourceAccess);
}
