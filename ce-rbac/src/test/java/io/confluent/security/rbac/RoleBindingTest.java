// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.utils.JsonMapper;
import io.confluent.security.authorizer.utils.JsonTestUtils;
import java.util.Collection;
import java.util.Collections;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RoleBindingTest {

  @Test
  public void testAssignment() throws Exception {
    RoleBinding alice = roleBinding(
        "{ \"principal\": \"User:Alice\", \"role\": \"ClusterAdmin\", \"scope\": {\"clusters\":{\"kafka-cluster\" : \"ClusterA\"}} }");
    assertEquals(new KafkaPrincipal("User", "Alice"), alice.principal());
    assertEquals("ClusterAdmin", alice.role());
    assertEquals(Collections.singletonMap("kafka-cluster", "ClusterA"), alice.scope().clusters());
    assertEquals(Collections.emptyList(), alice.scope().path());
    assertTrue(alice.resources().isEmpty());
    verifyEquals(alice, roleBinding(JsonMapper.objectMapper().writeValueAsString(alice)));

    RoleBinding bob = roleBinding(
        "{ \"principal\": \"User:Bob\", \"role\": \"Developer\", \"scope\": {\"clusters\": {\"kafka-cluster\" : \"ClusterB\" }}, " +
            "\"resources\" : [ {\"resourceType\": \"Topic\", \"patternType\": \"PREFIXED\", \"name\": \"Finance\"}," +
            "{\"resourceType\": \"Group\", \"patternType\": \"LITERAL\", \"name\": \"*\"}," +
            "{\"resourceType\": \"App\", \"patternType\": \"LITERAL\", \"name\": \"FinanceAppA\"} ] }");
    assertEquals(new KafkaPrincipal("User", "Bob"), bob.principal());
    assertEquals("Developer", bob.role());
    assertEquals(Collections.singletonMap("kafka-cluster", "ClusterB"), bob.scope().clusters());
    Collection<ResourcePattern> resources = bob.resources();
    assertEquals(3, resources.size());
    verifyEquals(bob, roleBinding(JsonMapper.objectMapper().writeValueAsString(bob)));
  }

  private RoleBinding roleBinding(String json) {
    return JsonTestUtils.jsonObject(RoleBinding.class, json);
  }

  private void verifyEquals(RoleBinding assignment1, RoleBinding assignment2) {
    assertEquals(assignment1.principal(), assignment2.principal());
    assertEquals(assignment1.role(), assignment2.role());
    assertEquals(assignment1.scope(), assignment2.scope());
    assertEquals(assignment1.resources(), assignment2.resources());
    assertEquals(assignment1, assignment2);
  }
}