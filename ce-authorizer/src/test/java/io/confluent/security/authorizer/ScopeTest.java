// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.security.authorizer.utils.JsonMapper;
import io.confluent.security.authorizer.utils.JsonTestUtils;
import org.junit.Test;

public class ScopeTest {

  @Test
  public void testScopes() throws Exception {
    Scope root = scope("{ \"path\": [] }");
    Scope abc = scope("{ \"path\": [\"a\", \"b\"], \"clusters\": {\"c\" : \"123\"}  }");
    Scope ab = scope("{ \"path\": [\"a\", \"b\"] }");
    Scope a = scope("{ \"path\": [\"a\"] }");
    Scope c = scope("{ \"path\": [\"c\"] }");

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
  public void testScopeType() {
    assertEquals(ScopeType.ROOT, Scope.ROOT_SCOPE.scopeType());

    assertEquals(ScopeType.CLUSTER, Scope.kafkaClusterScope("kafka_id").scopeType());
    assertEquals(ScopeType.CLUSTER, new Scope.Builder()
            .withKafkaCluster("kafka_id")
            .withCluster("ksql-cluster", "ksql_id").build().scopeType());
    assertEquals(ScopeType.CLUSTER, new Scope.Builder()
            .addPath("intermediate")
            .withKafkaCluster("kafka_id").build().scopeType());
    assertEquals(ScopeType.CLUSTER, new Scope.Builder()
            .addPath("organization=org_id")
            .withKafkaCluster("kafka_id").build().scopeType());
    assertEquals(ScopeType.CLUSTER, new Scope.Builder()
            .addPath("organization=org_id")
            .addPath("environment=org_id")
            .withKafkaCluster("kafka_id").build().scopeType());

    assertEquals(ScopeType.UNKNOWN, Scope.intermediateScope("intermediate").scopeType());

    // Confluent Cloud specific

    assertEquals(ScopeType.ENVIRONMENT,
            Scope.intermediateScope("environment=env_id").scopeType());
    assertEquals(ScopeType.ENVIRONMENT,
            Scope.intermediateScope("organization=org_id", "environment=env_id").scopeType());

    assertEquals(ScopeType.ORGANIZATION,
            Scope.intermediateScope("organization=org_id").scopeType());
  }

  @Test
  public void testEnclosingScope() {
    Scope clusterScope = new Scope.Builder("organization=org_id", "environment=env_id")
            .withKafkaCluster("kafka_id").build();
    assertEquals(clusterScope,
            clusterScope.enclosingScope(ScopeType.CLUSTER));
    assertEquals(clusterScope.parent(),
            clusterScope.enclosingScope(ScopeType.ENVIRONMENT));
    assertEquals(clusterScope.parent().parent(),
            clusterScope.enclosingScope(ScopeType.ORGANIZATION));
    assertEquals(Scope.ROOT_SCOPE,
            clusterScope.enclosingScope(ScopeType.ROOT));
    assertNull(
            clusterScope.enclosingScope(ScopeType.UNKNOWN));
    assertEquals(clusterScope,
            clusterScope.enclosingScope(ScopeType.RESOURCE));
  }

}