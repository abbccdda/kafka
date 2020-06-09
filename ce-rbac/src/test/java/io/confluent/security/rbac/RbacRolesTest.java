// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.ScopeType;
import io.confluent.security.authorizer.utils.JsonMapper;
import io.confluent.security.authorizer.utils.JsonTestUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RbacRolesTest {

  private static final String ADMIN_ROLE = "{ \"name\" : \"admin\" , \"policy\" : " +
      "{ \"scopeType\" : \"Cluster\", \"allowedOperations\" : [{ \"resourceType\" : \"Topic\", \"operations\" : [\"All\"]}] }}";
  private static final String DEVELOPER_ROLE = "{ \"name\" : \"developer\" , \"policy\" : " +
      "{ \"scopeType\" : \"Resource\", \"allowedOperations\" : [{ \"resourceType\" : \"Metrics\", \"operations\" : [\"Monitor\"]}] }}";

  private RbacRoles rbacRoles = new RbacRoles(Collections.emptyList());

  @Test
  public void testRoleDefinitions() throws Exception {
    addRoles(ADMIN_ROLE);
    assertEquals(1, rbacRoles.roles().size());
    Role role = rbacRoles.roles().iterator().next();
    assertEquals("admin", role.name());
    AccessPolicy accessPolicy = role.accessPolicies().values().stream().findFirst().get();
    assertNotNull(accessPolicy);
    assertEquals(ScopeType.CLUSTER, accessPolicy.scopeType());
    verifyAccessPolicy(accessPolicy, "Topic", "All");

    addRoles(DEVELOPER_ROLE);
    assertEquals(2, rbacRoles.roles().size());
    Role role2 = rbacRoles.role("developer");
    AccessPolicy accessPolicy2 = role2.accessPolicies().values().stream().findFirst().get();
    assertNotNull(accessPolicy2);
    assertEquals(ScopeType.RESOURCE, accessPolicy2.scopeType());
    verifyAccessPolicy(accessPolicy2, "Metrics", "Monitor");

    assertEquals(accessPolicy, accessPolicy(JsonMapper.objectMapper().writeValueAsString(accessPolicy)));
    assertEquals(accessPolicy2, accessPolicy(JsonMapper.objectMapper().writeValueAsString(accessPolicy2)));
  }

  @Test(expected = InvalidRoleDefinitionException.class)
  public void testRoleWithUnknownScope() throws Exception {
    String json = "{ \"name\" : \"admin\" , \"policy\" : " +
        "{ \"scope\" : \"unknown\", \"allowedOperations\" : [{ \"resourceType\" : \"All\", \"operations\" : [\"All\"]}] }}";
    addRoles(json);
  }

  @Test
  public void testDefaultRoles() throws Exception {
    RbacRoles rbacRoles = RbacRoles.loadDefaultPolicy(false);

    assertEquals(ScopeType.CLUSTER, rbacRoles.role("SystemAdmin")
            .accessPolicies().values().stream().findFirst().get().scopeType());
    assertEquals(ScopeType.CLUSTER, rbacRoles.role("UserAdmin")
            .accessPolicies().values().stream().findFirst().get().scopeType());
    assertEquals(ScopeType.CLUSTER, rbacRoles.role("ClusterAdmin")
            .accessPolicies().values().stream().findFirst().get().scopeType());
    assertEquals(ScopeType.CLUSTER, rbacRoles.role("Operator")
            .accessPolicies().values().stream().findFirst().get().scopeType());
    assertEquals(ScopeType.CLUSTER, rbacRoles.role("SecurityAdmin")
            .accessPolicies().values().stream().findFirst().get().scopeType());
    assertEquals(ScopeType.RESOURCE, rbacRoles.role("ResourceOwner")
            .accessPolicies().values().stream().findFirst().get().scopeType());
    assertEquals(ScopeType.RESOURCE, rbacRoles.role("DeveloperRead")
            .accessPolicies().values().stream().findFirst().get().scopeType());
    assertEquals(ScopeType.RESOURCE, rbacRoles.role("DeveloperWrite")
            .accessPolicies().values().stream().findFirst().get().scopeType());
    assertEquals(ScopeType.RESOURCE, rbacRoles.role("DeveloperManage")
            .accessPolicies().values().stream().findFirst().get().scopeType());

    assertTrue(rbacRoles.role("UserAdmin")
            .accessPolicies().values().stream().findFirst().get()
        .allowedOperations(new ResourceType("Cluster")).contains(new Operation("Alter")));
    assertTrue(rbacRoles.role("ResourceOwner")
            .accessPolicies().values().stream().findFirst().get()
        .allowedOperations(new ResourceType("Group")).contains(new Operation("Read")));
  }


  @Test
  public void testCloudRoles() throws Exception {
    RbacRoles rbacRoles = RbacRoles.loadDefaultPolicy(true);

    Role bindingAdmin = rbacRoles.role("CCloudRoleBindingAdmin");
    assertEquals(Arrays.asList(ScopeType.ROOT),
            bindingAdmin.accessPolicies().keySet()
                    .stream().sorted().collect(Collectors.toList()));
    assertEquals(ScopeType.ROOT, bindingAdmin.accessPolicies().get(ScopeType.ROOT).scopeType());


    Role orgAdmin = rbacRoles.role("OrganizationAdmin");
    assertEquals(Arrays.asList(ScopeType.ORGANIZATION),
            orgAdmin.accessPolicies().keySet()
                    .stream().sorted().collect(Collectors.toList()));
    assertEquals(ScopeType.ORGANIZATION, orgAdmin.accessPolicies().get(ScopeType.ORGANIZATION).scopeType());

    Role envAdmin = rbacRoles.role("EnvironmentAdmin");
    assertEquals(Arrays.asList(ScopeType.ENVIRONMENT, ScopeType.ORGANIZATION),
            envAdmin.accessPolicies().keySet()
                    .stream().sorted().collect(Collectors.toList()));
    assertEquals(ScopeType.ENVIRONMENT, envAdmin.accessPolicies().get(ScopeType.ENVIRONMENT).scopeType());
    assertEquals(ScopeType.ORGANIZATION, envAdmin.accessPolicies().get(ScopeType.ORGANIZATION).scopeType());

    Role clusterAdmin = rbacRoles.role("CloudClusterAdmin");
    assertEquals(Arrays.asList(ScopeType.CLUSTER, ScopeType.ORGANIZATION),
            clusterAdmin.accessPolicies().keySet()
                    .stream().sorted().collect(Collectors.toList()));
    assertEquals(ScopeType.CLUSTER, clusterAdmin.accessPolicies().get(ScopeType.CLUSTER).scopeType());
    assertEquals(ScopeType.ORGANIZATION, clusterAdmin.accessPolicies().get(ScopeType.ORGANIZATION).scopeType());

  }

  private void addRoles(String rolesJson) {
    for (Role role : JsonTestUtils.jsonArray(Role[].class, rolesJson)) {
      rbacRoles.addRole(role);
    }
  }

  private void verifyAccessPolicy(AccessPolicy accessPolicy, String expectedResourceType, String... expectedOps) {
    assertEquals(1, accessPolicy.allowedOperations().size());
    assertEquals(expectedResourceType, accessPolicy.allowedOperations().iterator().next().resourceType());
    Collection<Operation> ops = accessPolicy.allowedOperations(new ResourceType(expectedResourceType));
    assertEquals(Utils.mkSet(expectedOps), ops.stream().map(Operation::name).collect(Collectors.toSet()));
  }

  private AccessPolicy accessPolicy(String json) {
    return JsonTestUtils.jsonObject(AccessPolicy.class, json);
  }
}