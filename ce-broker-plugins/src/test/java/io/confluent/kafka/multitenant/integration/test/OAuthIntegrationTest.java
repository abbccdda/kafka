package io.confluent.kafka.multitenant.integration.test;


import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.network.CertStores;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.kafka.test.IntegrationTest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.multitenant.Utils;
import io.confluent.kafka.server.plugins.auth.oauth.OAuthUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;

import static io.confluent.kafka.multitenant.Utils.LC_META_ABC;

@Category(IntegrationTest.class)
public class OAuthIntegrationTest {

  private IntegrationTestHarness testHarness;
  private OAuthUtils.JwsContainer jwsContainer;
  private Map<String, Object> saslClientConfigs;
  private Map<String, Object> saslServerConfigs;

  private final String allowedCluster = LC_META_ABC.logicalClusterId();
  private final String[] allowedClusters = new String[] {allowedCluster};

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
    CertStores serverCertStores = new CertStores(true, "localhost");
    CertStores clientCertStores = new CertStores(false, "localhost");
    saslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
    saslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);

    testHarness = new IntegrationTestHarness();
  }

  /**
   *  Ensures that the way we've structured the metadata initialization works with our OAuth plugin which depends on it being initialized
   */
  @Test
  public void testOAuthPluginInitializesCleanlyOnServerStartup() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(100000, "Confluent", "Confluent", allowedClusters);
    configureMechanisms("OAUTHBEARER", Collections.singletonList("OAUTHBEARER"));

    testHarness.start(brokerProps());
    testHarness.shutdown();
  }

  private Properties brokerProps() throws IOException {
    Properties props = new Properties();
    props.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
              tempFolder.getRoot().getCanonicalPath());
    props.put(ConfluentConfigs.MULTITENANT_METADATA_CLASS_CONFIG,
              "io.confluent.kafka.multitenant.PhysicalClusterMetadata");

    Map<String, Object> serverProps = new HashMap<>();
    SecurityTestUtils.attachServerOAuthConfigs(serverProps,
                                               Collections.singletonList("OAUTHBEARER"),
                                               "listener.name.external",
                                               jwsContainer);
    props.putAll(serverProps);
    return props;
  }

  private void configureMechanisms(String clientMechanism, List<String> serverMechanisms) {
    SecurityTestUtils.attachMechanisms(this.saslClientConfigs, clientMechanism, jwsContainer, allowedCluster);

    SecurityTestUtils.attachServerOAuthConfigs(this.saslServerConfigs, serverMechanisms,
                                               "listener.name.sasl_ssl", jwsContainer);
    TestJaasConfig.createConfiguration(clientMechanism, serverMechanisms);
  }
}
