// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.authorizer.utils.JsonMapper;
import io.confluent.security.authorizer.utils.JsonTestUtils;
import org.junit.Test;

public class ScopeTest {

  @Test
  public void testScopes() throws Exception {
    Scope root = scope("{ \"path\": [] }");
    Scope abc = scope("{ \"path\": [\"a=1\", \"b=2\"], \"clusters\": {\"c\" : \"123\"}  }");
    Scope ab = scope("{ \"path\": [\"a=1\", \"b=2\"] }");
    Scope a = scope("{ \"path\": [\"a=1\"] }");
    Scope c = scope("{ \"path\": [\"c=3\"] }");

    assertEquals(root, scope(JsonMapper.objectMapper().writeValueAsString(root)));
    assertEquals(abc, scope(JsonMapper.objectMapper().writeValueAsString(abc)));
    assertEquals(ab, scope(JsonMapper.objectMapper().writeValueAsString(ab)));
    assertEquals(a, scope(JsonMapper.objectMapper().writeValueAsString(a)));

    assertEquals(ab, abc.parent());
    assertEquals(a, ab.parent());
    assertEquals(root, a.parent());
    assertNull(root.parent());

    assertTrue(abc.containsScope(abc));
    assertTrue(a.containsScope(ab));
    assertTrue(a.containsScope(abc));
    assertTrue(ab.containsScope(abc));
    assertFalse(abc.containsScope(ab));
    assertFalse(abc.containsScope(a));
    assertFalse(abc.containsScope(c));
    assertFalse(ab.containsScope(c));
    assertFalse(a.containsScope(c));

    assertTrue(root.containsScope(abc));
    assertTrue(root.containsScope(ab));
    assertTrue(root.containsScope(a));
    assertTrue(root.containsScope(c));
    assertFalse(abc.containsScope(root));
    assertFalse(ab.containsScope(root));
    assertFalse(c.containsScope(root));
  }

  private Scope scope(String json) {
    return JsonTestUtils.jsonObject(Scope.class, json);
  }

  @Test
  public void testValidateScope() {
    // happy path
    scope("{\"path\": [\"business-unit=finance\"], " +
            "\"clusters\": {\"kafka-cluster\": \"clusterId\"}}").validate();

    assertThrows(InvalidScopeException.class, () ->
            scope("{\"clusters\": {\"kafka-cluster\": \"\"}}").validate());
    assertThrows(InvalidScopeException.class, () ->
            scope("{\"clusters\": {\"kafka-cluster\": null}}").validate());
    assertThrows(InvalidScopeException.class, () ->
            scope("{\"clusters\": {\"\": \"clusterId\"}}").validate());

    assertThrows(InvalidScopeException.class, () ->
            scope("{\"path\": [null]}").validate());
    assertThrows(InvalidScopeException.class, () ->
            scope("{\"path\": [\"\"]}").validate());

    assertThrows(InvalidScopeException.class, () ->
            scope("{\"path\": [\"root=foo\"]}").validate());
    assertThrows(InvalidScopeException.class, () ->
            scope("{\"path\": [\"cluster=foo\"]}").validate());
  }

  @Test
  public void testBindingScope() {
    assertEquals(Scope.ROOT_BINDING_SCOPE, Scope.ROOT_SCOPE.bindingScope());

    assertEquals(Scope.CLUSTER_BINDING_SCOPE, Scope.kafkaClusterScope("kafka_id").bindingScope());
    assertEquals(Scope.CLUSTER_BINDING_SCOPE, new Scope.Builder()
            .withKafkaCluster("kafka_id")
            .withCluster("ksql-cluster", "ksql_id").build().bindingScope());
    assertEquals(Scope.CLUSTER_BINDING_SCOPE, new Scope.Builder()
            .addPath("division=intermediate")
            .withKafkaCluster("kafka_id").build().bindingScope());
    assertEquals(Scope.CLUSTER_BINDING_SCOPE, new Scope.Builder()
            .addPath("organization=org_id")
            .withKafkaCluster("kafka_id").build().bindingScope());
    assertEquals(Scope.CLUSTER_BINDING_SCOPE, new Scope.Builder()
            .addPath("organization=org_id")
            .addPath("environment=env_id")
            .addPath("cloud-cluster=kafka_id")
            .withKafkaCluster("kafka_id").build().bindingScope());

    // Testing arbitrary nested scopes

    assertEquals("region",
            Scope.intermediateScope("region=env_id").bindingScope());
    assertEquals("region",
            Scope.intermediateScope("division=org_id", "region=env_id").bindingScope());

    assertEquals("division",
            Scope.intermediateScope("division=org_id").bindingScope());
  }

  @Test
  public void testEnclosingScope() {
    Scope clusterScope = new Scope.Builder("division=org_id", "region=env_id")
            .withKafkaCluster("kafka_id").build();
    assertEquals(clusterScope,
            clusterScope.ancestorWithBindingScope(Scope.CLUSTER_BINDING_SCOPE));
    assertEquals(clusterScope.parent(),
            clusterScope.ancestorWithBindingScope("region"));
    assertEquals(clusterScope.parent().parent(),
            clusterScope.ancestorWithBindingScope("division"));
    assertEquals(Scope.ROOT_SCOPE,
            clusterScope.ancestorWithBindingScope(Scope.ROOT_BINDING_SCOPE));
  }

}