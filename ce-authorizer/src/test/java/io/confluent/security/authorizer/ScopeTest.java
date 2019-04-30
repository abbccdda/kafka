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
}