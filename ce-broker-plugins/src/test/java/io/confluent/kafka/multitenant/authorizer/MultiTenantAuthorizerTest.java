// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.kafka.multitenant.authorizer;

import static org.junit.Assert.assertEquals;

import io.confluent.security.authorizer.provider.DefaultAuditLogProvider;
import java.util.Collections;
import org.junit.Test;

public class MultiTenantAuthorizerTest {

  @Test
  public void testAuditEventsDisabled() {
    MultiTenantAuthorizer authorizer = new MultiTenantAuthorizer();
    authorizer.configureProviders(Collections.emptyList(),
        null,
        null,
        new MockAuditLogProvider());
    assertEquals(DefaultAuditLogProvider.class, authorizer.auditLogProvider().getClass());
  }

  private static class MockAuditLogProvider extends DefaultAuditLogProvider {
  }
}
