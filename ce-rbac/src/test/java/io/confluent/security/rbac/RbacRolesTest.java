// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.utils.JsonMapper;
import io.confluent.security.authorizer.utils.JsonTestUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

public class RbacRolesTest {

  private static final String ADMIN_ROLE = "{ \"name\" : \"admin\" , \"policy\" : " +
      "{ \"bindingScope\" : \"cluster\", \"bindWithResource\": false, \"allowedOperations\" : [{ \"resourceType\" : \"Topic\", \"operations\" : [\"All\"]}] }}";
  private static final String DEVELOPER_ROLE = "{ \"name\" : \"developer\" , \"policy\" : " +
      "{ \"bindingScope\" : \"cluster\", \"bindWithResource\": true, \"allowedOperations\" : [{ \"resourceType\" : \"Metrics\", \"operations\" : [\"Monitor\"]}] }}";

  private RbacRoles rbacRoles = new RbacRoles(Collections.emptyList(),
          Arrays.asList("cluster", "environment", "organization", "root"));

  @Test
  public void testRoleDefinitions() throws Exception {
    addRoles(ADMIN_ROLE);
    assertEquals(1, rbacRoles.roles().size());
    Role role = rbacRoles.roles().iterator().next();
    assertEquals("admin", role.name());
    AccessPolicy accessPolicy = role.accessPolicies().values().stream().findFirst().get();
    assertNotNull(accessPolicy);
    assertFalse(accessPolicy.bindWithResource());
    assertEquals(Scope.CLUSTER_BINDING_SCOPE, accessPolicy.bindingScope());
    verifyAccessPolicy(accessPolicy, "Topic", "All");

    addRoles(DEVELOPER_ROLE);
    assertEquals(2, rbacRoles.roles().size());
    Role role2 = rbacRoles.role("developer");
    AccessPolicy accessPolicy2 = role2.accessPolicies().values().stream().findFirst().get();
    assertNotNull(accessPolicy2);
    assertTrue(accessPolicy2.bindWithResource());
    assertEquals(Scope.CLUSTER_BINDING_SCOPE, accessPolicy2.bindingScope());
    verifyAccessPolicy(accessPolicy2, "Metrics", "Monitor");

    assertEquals(accessPolicy, accessPolicy(JsonMapper.objectMapper().writeValueAsString(accessPolicy)));
    assertEquals(accessPolicy2, accessPolicy(JsonMapper.objectMapper().writeValueAsString(accessPolicy2)));
  }

  @Test(expected = InvalidRoleDefinitionException.class)
  public void testRoleWithUnknownScope() throws Exception {
    String json = "{ \"name\" : \"admin\" , \"policy\" : " +
        "{ \"bindingScope\" : \"unknown\", \"bindWithResource\" : false, \"allowedOperations\" : [{ \"resourceType\" : \"All\", \"operations\" : [\"All\"]}] }}";
    addRoles(json);
  }
  
  private AccessPolicy onlyPolicy(Role role) {
    Collection<AccessPolicy> policies = role.accessPolicies().values();
    assertEquals(1, policies.size());
    return policies.iterator().next();
  }

  @Test
  public void testDefaultRoles() throws Exception {
    RbacRoles rbacRoles = RbacRoles.loadDefaultPolicy(false);

    assertEquals(Scope.CLUSTER_BINDING_SCOPE, 
            onlyPolicy(rbacRoles.role("SystemAdmin")).bindingScope());
    assertFalse(
            onlyPolicy(rbacRoles.role("SystemAdmin")).bindWithResource());

    assertEquals(Scope.CLUSTER_BINDING_SCOPE,  
            onlyPolicy(rbacRoles.role("UserAdmin")).bindingScope());
    assertFalse(
            onlyPolicy(rbacRoles.role("UserAdmin")).bindWithResource());

    assertEquals(Scope.CLUSTER_BINDING_SCOPE,  
            onlyPolicy(rbacRoles.role("ClusterAdmin")).bindingScope());
    assertFalse(
            onlyPolicy(rbacRoles.role("ClusterAdmin")).bindWithResource());

    assertEquals(Scope.CLUSTER_BINDING_SCOPE,  
            onlyPolicy(rbacRoles.role("Operator")).bindingScope());
    assertFalse(
            onlyPolicy(rbacRoles.role("Operator")).bindWithResource());

    assertEquals(Scope.CLUSTER_BINDING_SCOPE,  
            onlyPolicy(rbacRoles.role("SecurityAdmin")).bindingScope());
    assertFalse(
            onlyPolicy(rbacRoles.role("SecurityAdmin")).bindWithResource());

    assertEquals(Scope.CLUSTER_BINDING_SCOPE,
            onlyPolicy(rbacRoles.role("ResourceOwner")).bindingScope());
    assertTrue(
            onlyPolicy(rbacRoles.role("ResourceOwner")).bindWithResource());

    assertEquals(Scope.CLUSTER_BINDING_SCOPE,
            onlyPolicy(rbacRoles.role("DeveloperRead")).bindingScope());
    assertTrue(
            onlyPolicy(rbacRoles.role("DeveloperRead")).bindWithResource());

    assertEquals(Scope.CLUSTER_BINDING_SCOPE,
            onlyPolicy(rbacRoles.role("DeveloperWrite")).bindingScope());
    assertTrue(
            onlyPolicy(rbacRoles.role("DeveloperWrite")).bindWithResource());

    assertEquals(Scope.CLUSTER_BINDING_SCOPE,
            onlyPolicy(rbacRoles.role("DeveloperManage")).bindingScope());
    assertTrue(onlyPolicy(rbacRoles.role("DeveloperManage")).bindWithResource());

    assertTrue(onlyPolicy(rbacRoles.role("UserAdmin"))
        .allowedOperations(new ResourceType("Cluster")).contains(new Operation("Alter")));
    assertTrue(onlyPolicy(rbacRoles.role("ResourceOwner"))
        .allowedOperations(new ResourceType("Group")).contains(new Operation("Read")));
  }


  @Test
  public void testCloudRoles() throws Exception {
    RbacRoles rbacRoles = RbacRoles.loadDefaultPolicy(true);

    Role bindingAdmin = rbacRoles.role("CCloudRoleBindingAdmin");
    assertEquals(Arrays.asList(Scope.ROOT_BINDING_SCOPE),
            bindingAdmin.accessPolicies().keySet()
                    .stream().sorted().collect(Collectors.toList()));
    assertEquals(Scope.ROOT_BINDING_SCOPE, bindingAdmin.accessPolicies().get(Scope.ROOT_BINDING_SCOPE).bindingScope());


    Role orgAdmin = rbacRoles.role("OrganizationAdmin");
    assertEquals(Arrays.asList("organization"),
            orgAdmin.accessPolicies().keySet()
                    .stream().sorted().collect(Collectors.toList()));
    assertEquals("organization", orgAdmin.accessPolicies().get("organization").bindingScope());

    Role envAdmin = rbacRoles.role("EnvironmentAdmin");
    assertEquals(Arrays.asList("environment", "organization"),
            envAdmin.accessPolicies().keySet()
                    .stream().sorted().collect(Collectors.toList()));
    assertEquals("environment", envAdmin.accessPolicies().get("environment").bindingScope());
    assertEquals("organization", envAdmin.accessPolicies().get("organization").bindingScope());

    Role clusterAdmin = rbacRoles.role("CloudClusterAdmin");
    assertEquals(Arrays.asList(Scope.CLUSTER_BINDING_SCOPE, "organization"),
            clusterAdmin.accessPolicies().keySet()
                    .stream().sorted().collect(Collectors.toList()));
    assertEquals(Scope.CLUSTER_BINDING_SCOPE, clusterAdmin.accessPolicies().get(Scope.CLUSTER_BINDING_SCOPE).bindingScope());
    assertEquals("organization", clusterAdmin.accessPolicies().get("organization").bindingScope());
  }

  @Test
  public void testInvalidBindingScopes() {
    String[][] tests = {
            {"root", "first binding scope must be 'cluster'"},
            {"cluster, root, org", "binding scope 'root' must be last"},
            {"cluster, org=, root", "bindingScopes may only contain letters and '-': 'org='"},
            {"cluster, org, cluster, root", "bindingScopes may not be repeated: 'cluster'"},
            {"cluster, org, org, root", "bindingScopes may not be repeated: 'org'"},
            {"cluster, org, root, root", "binding scope 'root' must be last"},
    };

    for (String[] test : tests) {
      try {
        new RbacRoles(Collections.emptyList(), Arrays.asList(test[0].split(", *")));
        fail("Didn't throw");
      } catch (InvalidRoleDefinitionException e) {
        assertEquals(test[1], e.getMessage());
      }
    }
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