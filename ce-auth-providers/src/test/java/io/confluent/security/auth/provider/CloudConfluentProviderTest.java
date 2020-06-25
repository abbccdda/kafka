// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.security.auth.provider;

import io.confluent.security.authorizer.Scope;
import io.confluent.security.rbac.RbacRoles;
import java.util.Collections;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This test class exercises two broad things:
 *
 * - Are the role definitions for the cloud roles correct individually (test*AdminAccessRules).
 *   They check both positive and negative permissions on each of the items in our model hierarchy
 *
 * - Do the roles interact correctly (testInteraction*)
 *   We want to be sure that combinations of roles either laterally or hierarchically have
 *   the expected effects, particularly that they stay correct if some roles are deleted
 *
 *   These tests use a simple hierarchy:
 *
 *   - organization=123
 *       - environment=t55
 *           - cloud-cluster=lkc-a
 *           - cloud-cluster=lkc-b
 *       - environment=t66
 *           - cloud-cluster=lkc-c
 *   - organization=789
 *
 *   and they also test against "invalid" elements:
 *   - organization=789
 *       - environment=t55
 *           - cloud-cluster=lkc-b
 *
 *   In our model environment=t55 "actually" belongs to organization=123, but here we're
 *   simulating an error or attack in which the "wrong" scope path is supplied and verifying
 *   that erroneous access is not allowed
 */
public class CloudConfluentProviderTest extends CloudBaseProviderTest {

  @Before
  public void setUp() throws Exception {
    RbacRoles rbacRoles  = RbacRoles.loadDefaultPolicy(true);
    initializeRbacProvider("clusterA", Scope.ROOT_SCOPE,
        Collections.singletonMap(KafkaConfig.AuthorizerClassNameProp(),
            ConfluentConfigs.MULTITENANT_AUTHORIZER_CLASS_NAME),
        rbacRoles);
  }

  @After
  public void tearDown() {
    if (rbacProvider != null)
      rbacProvider.close();
  }

  @Test
  public void testRoleBindingAdminAccessRules() {
    updateRoleBinding(alice, "CCloudRoleBindingAdmin", Scope.ROOT_SCOPE, null);

    verifyRules(alice, groups, organization123, org, "DescribeAccess", "AlterAccess");
    verifyRules(alice, groups, organization123, user);
    verifyRules(alice, groups, organization789, org, "DescribeAccess", "AlterAccess");

    verifyRules(alice, groups, environmentT55, envT55);

    verifyRules(alice, groups, clusterA, clusterResource);
    verifyRules(alice, groups, clusterA, topic);

    verifyRules(alice, groups, clusterB, clusterResource);
    verifyRules(alice, groups, clusterB, topic);

    verifyRules(alice, groups, clusterC, clusterResource);
    verifyRules(alice, groups, clusterC, topic);

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, clusterResource);

    deleteRoleBinding(alice, "CCloudRoleBindingAdmin", Scope.ROOT_SCOPE);
  }

  private void verifyOrgAdmin() {
    verifyRules(alice, groups, organization123, org, "Describe", "Alter", "CreateEnvironment", "DescribeAccess", "AlterAccess");
    verifyRules(alice, groups, organization123, user, "Create", "Invite", "Describe", "Delete");
    verifyRules(alice, groups, organization789, org);

    verifyRules(alice, groups, environmentT55, envT55, "All");
    verifyRules(alice, groups, environmentT66, envT66, "All");

    verifyRules(alice, groups, clusterA, clusterResource, "All");
    verifyRules(alice, groups, clusterA, topic, "All");

    verifyRules(alice, groups, clusterB, clusterResource, "All");
    verifyRules(alice, groups, clusterB, topic, "All");

    verifyRules(alice, groups, clusterC, clusterResource, "All");
    verifyRules(alice, groups, clusterC, topic, "All");

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, clusterResource);
  }

  private void verifyNoPermissions() {
    verifyRules(alice, groups, organization123, org);
    verifyRules(alice, groups, organization123, user);
    verifyRules(alice, groups, organization789, org);

    verifyRules(alice, groups, environmentT55, envT55);
    verifyRules(alice, groups, environmentT66, envT66);

    verifyRules(alice, groups, clusterA, clusterResource);
    verifyRules(alice, groups, clusterA, topic);

    verifyRules(alice, groups, clusterB, clusterResource);
    verifyRules(alice, groups, clusterB, topic);

    verifyRules(alice, groups, clusterC, clusterResource);
    verifyRules(alice, groups, clusterC, topic);

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, clusterResource);
  }
  
  @Test
  public void testOrgAdminAccessRules() {
    updateRoleBinding(alice, "OrganizationAdmin", organization123, null);

    verifyOrgAdmin();

    deleteRoleBinding(alice, "OrganizationAdmin", organization123);

    verifyNoPermissions();
  }

  private void verifyEnvAdminT55() {
    verifyRules(alice, groups, organization123, org, "Describe");
    verifyRules(alice, groups, organization123, user, "Create", "Invite", "Describe");
    verifyRules(alice, groups, organization789, org);

    verifyRules(alice, groups, environmentT55, envT55,
            "Describe", "Alter", "Delete", "CreateKafkaCluster", "DescribeAccess", "AlterAccess");
    verifyRules(alice, groups, environmentT66, envT66);

    verifyRules(alice, groups, clusterA, clusterResource, "All");
    verifyRules(alice, groups, clusterA, topic, "All");

    verifyRules(alice, groups, clusterB, clusterResource, "All");
    verifyRules(alice, groups, clusterB, topic, "All");

    verifyRules(alice, groups, clusterC, clusterResource);
    verifyRules(alice, groups, clusterC, topic);

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, clusterResource);
  }

  @Test
  public void testEnvAdminAccessRules() {
    updateRoleBinding(alice, "EnvironmentAdmin", environmentT55, null);

    verifyEnvAdminT55();

    deleteRoleBinding(alice, "EnvironmentAdmin", environmentT55);

    verifyNoPermissions();
  }

  private void verifyCloudClusterAdminA() {
    verifyRules(alice, groups, organization123, org, "Describe");
    verifyRules(alice, groups, organization123, user, "Create", "Invite", "Describe");
    verifyRules(alice, groups, organization789, org);

    verifyRules(alice, groups, environmentT55, envT55, "Describe");
    verifyRules(alice, groups, environmentT55, envT66, "Describe");

    verifyRules(alice, groups, clusterA, cloudClusterResource, "Describe", "Alter", "Delete", "CreateKsqlCluster", "CreateKafkaCluster");
    verifyRules(alice, groups, kafkaClusterA, clusterResource, "Create", "IdempotentWrite", "Describe", "DescribeConfigs", "Alter", "AlterConfigs");
    verifyRules(alice, groups, kafkaClusterA, topic, "Create", "Delete", "Read", "Write", "Describe", "DescribeConfigs", "Alter", "AlterConfigs");

    verifyRules(alice, groups, clusterB, cloudClusterResource);
    verifyRules(alice, groups, kafkaClusterB, clusterResource);
    verifyRules(alice, groups, kafkaClusterB, topic);

    verifyRules(alice, groups, clusterC, clusterResource);
    verifyRules(alice, groups, kafkaClusterC, clusterResource);
    verifyRules(alice, groups, kafkaClusterC, topic);

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, cloudClusterResource);
  }

  @Test
  public void testCloudClusterAdminAccessRules() {
    updateRoleBinding(alice, "CloudClusterAdmin", clusterA, null);

    verifyCloudClusterAdminA();

    deleteRoleBinding(alice, "CloudClusterAdmin", clusterA);

    verifyNoPermissions();
  }

  @Test
  public void testInteractionMultipleEnvironments() {
    updateRoleBinding(alice, "EnvironmentAdmin", environmentT55, null);
    updateRoleBinding(alice, "EnvironmentAdmin", environmentT66, null);

    verifyRules(alice, groups, organization123, org, "Describe");
    verifyRules(alice, groups, organization123, user, "Create", "Invite", "Describe");
    verifyRules(alice, groups, organization789, org);

    verifyRules(alice, groups, environmentT55, envT55,
            "Describe", "Alter", "Delete", "CreateKafkaCluster", "DescribeAccess", "AlterAccess");
    verifyRules(alice, groups, environmentT66, envT66,
        "Describe", "Alter", "Delete", "CreateKafkaCluster", "DescribeAccess", "AlterAccess");

    verifyRules(alice, groups, clusterA, clusterResource, "All");
    verifyRules(alice, groups, clusterA, topic, "All");

    verifyRules(alice, groups, clusterB, clusterResource, "All");
    verifyRules(alice, groups, clusterB, topic, "All");

    verifyRules(alice, groups, clusterC, clusterResource, "All");
    verifyRules(alice, groups, clusterC, topic, "All");

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, clusterResource);

    deleteRoleBinding(alice, "EnvironmentAdmin", environmentT55);

    // still have our org permissions
    verifyRules(alice, groups, organization123, org, "Describe");
    verifyRules(alice, groups, organization123, user, "Create", "Invite", "Describe");

    verifyRules(alice, groups, environmentT55, envT55);
    verifyRules(alice, groups, environmentT66, envT66,
            "Describe", "Alter", "Delete", "CreateKafkaCluster", "DescribeAccess", "AlterAccess");

    verifyRules(alice, groups, clusterA, clusterResource);
    verifyRules(alice, groups, clusterA, topic);

    verifyRules(alice, groups, clusterB, clusterResource);
    verifyRules(alice, groups, clusterB, topic);

    verifyRules(alice, groups, clusterC, clusterResource, "All");
    verifyRules(alice, groups, clusterC, topic, "All");

    verifyRules(alice, groups, wrongEnvironmentT55, envT55);
    verifyRules(alice, groups, wrongClusterB, clusterResource);

    deleteRoleBinding(alice, "EnvironmentAdmin", environmentT66);

    verifyNoPermissions();
  }

  @Test
  public void testInteractionDifferentLevels() {
    updateRoleBinding(alice, "OrganizationAdmin", organization123, null);
    updateRoleBinding(alice, "EnvironmentAdmin", environmentT55, null);
    updateRoleBinding(alice, "CloudClusterAdmin", clusterA, null);

    verifyOrgAdmin();

    deleteRoleBinding(alice, "OrganizationAdmin", organization123);

    verifyEnvAdminT55();

    deleteRoleBinding(alice, "EnvironmentAdmin", environmentT55);

    verifyCloudClusterAdminA();
  }

  @Test
  public void testInteractionDifferentLevelsDeleteInner() {
    updateRoleBinding(alice, "OrganizationAdmin", organization123, null);
    updateRoleBinding(alice, "EnvironmentAdmin", environmentT55, null);
    updateRoleBinding(alice, "CloudClusterAdmin", clusterA, null);

    verifyOrgAdmin();

    deleteRoleBinding(alice, "CloudClusterAdmin", clusterA);

    verifyOrgAdmin();

    deleteRoleBinding(alice, "EnvironmentAdmin", environmentT55);

    verifyOrgAdmin();
  }

}
