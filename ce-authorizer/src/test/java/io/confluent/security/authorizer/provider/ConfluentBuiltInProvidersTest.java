// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

public class ConfluentBuiltInProvidersTest {

  @Test
  public void testOldProviderException() throws Exception {
    ConfigException e = assertThrows(ConfigException.class,
        () -> ConfluentBuiltInProviders.loadAccessRuleProviders(Collections.singletonList("ACL")));
    assertTrue("Unexpected message: " + e.getMessage(),
        e.getMessage().contains("confluent.authorizer.access.rule.providers=ZK_ACL"));

    e = assertThrows(ConfigException.class,
        () -> ConfluentBuiltInProviders.loadAccessRuleProviders(Collections.singletonList("RBAC")));
    assertTrue("Unexpected message: " + e.getMessage(),
        e.getMessage().contains("confluent.authorizer.access.rule.providers=CONFLUENT"));

    e = assertThrows(ConfigException.class,
        () -> ConfluentBuiltInProviders.loadAccessRuleProviders(Arrays.asList("ACL", "RBAC")));
    assertTrue("Unexpected message: " + e.getMessage(),
        e.getMessage().contains("confluent.authorizer.access.rule.providers=ZK_ACL,CONFLUENT") ||
        e.getMessage().contains("confluent.authorizer.access.rule.providers=CONFLUENT,ZK_ACL")
    );

    e = assertThrows(ConfigException.class,
        () -> ConfluentBuiltInProviders.loadAccessRuleProviders(Collections.singletonList("SOME_ACL")));
    assertFalse("Unexpected message: " + e.getMessage(),
        e.getMessage().contains("confluent.authorizer.access.rule.providers=ZK_ACL"));
  }
}