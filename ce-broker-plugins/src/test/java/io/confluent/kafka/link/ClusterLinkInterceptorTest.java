// (Copyright) [2020 - 2020] Confluent, Inc.
package io.confluent.kafka.link;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Map;
import kafka.server.link.ClusterLinkManager$;
import org.apache.kafka.clients.ClientInterceptor;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

public class ClusterLinkInterceptorTest {

  private final String destPrefix = "dest_tenant_";
  private final Map<String, String> configs = Collections.singletonMap(
      ClusterLinkManager$.MODULE$.DestinationTenantPrefixProp(), destPrefix);

  @Test
  public void testConfigure() {
    ClusterLinkInterceptor interceptor = new ClusterLinkInterceptor();
    assertThrows(ConfigException.class, () -> interceptor.configure(Collections.emptyMap()));
    interceptor.configure(configs);
  }

  @Test
  public void testServiceLoader() {
    ClientInterceptor interceptor = ClusterLinkManager$.MODULE$.tenantInterceptor(destPrefix);
    assertNotNull(interceptor);
    assertTrue(interceptor instanceof ClusterLinkInterceptor);
  }

}