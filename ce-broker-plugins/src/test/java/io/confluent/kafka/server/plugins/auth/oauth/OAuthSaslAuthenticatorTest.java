// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth.oauth;


import io.confluent.kafka.multitenant.PhysicalClusterMetadata;
import io.confluent.kafka.multitenant.Utils;
import io.confluent.kafka.test.utils.SecurityTestUtils;

import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.CertStores;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.NioEchoServer;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.NetworkTestUtils;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.kafka.multitenant.Utils.LC_META_ABC;
import static io.confluent.kafka.multitenant.Utils.initiatePhysicalClusterMetadata;


public class OAuthSaslAuthenticatorTest {
  private Selector selector;
  private NioEchoServer server;
  private OAuthUtils.JwsContainer jwsContainer;
  private Map<String, Object> saslClientConfigs;
  private Map<String, Object> saslServerConfigs;
  private String allowedCluster = LC_META_ABC.logicalClusterId();
  private PhysicalClusterMetadata metadata;
  private Map<String, Object> configs;
  private String brokerUUID;
  private String[] allowedClusters = new String[] {allowedCluster};
  private static Time time = Time.SYSTEM;

  private CredentialCache credentialCache;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    LoginManager.closeAll();
    CertStores serverCertStores = new CertStores(true, "localhost");
    CertStores clientCertStores = new CertStores(false, "localhost");
    this.saslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
    this.saslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
    this.credentialCache = new CredentialCache();
    setUpMetadata();
  }

  private void setUpMetadata() throws IOException, InterruptedException {
    brokerUUID = "uuid";
    configs = new HashMap<>();
    configs.put("broker.session.uuid", brokerUUID);
    saslServerConfigs.put("broker.session.uuid", brokerUUID);
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
        tempFolder.getRoot().getCanonicalPath());

    metadata = initiatePhysicalClusterMetadata(configs);

    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
    TestUtils.waitForCondition(
        () -> metadata.metadata(LC_META_ABC.logicalClusterId()) != null,
        "Expected metadata of new logical cluster to be present in metadata cache");
  }

  @After
  public void tearDown() throws Exception {
    if (this.server != null) {
      this.server.close();
    }

    if (this.selector != null) {
      this.selector.close();
    }

    metadata.close(brokerUUID);
  }

  @Test
  public void testValidTokenAuthorizes() throws Exception {
    String node = "0";
    SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
    jwsContainer = OAuthUtils.setUpJws(100000, "Confluent", "Confluent", allowedClusters);
    this.configureMechanisms("OAUTHBEARER", Collections.singletonList("OAUTHBEARER"));
    this.server = this.createEchoServer(securityProtocol);

    this.createAndCheckClientConnection(securityProtocol, node);
    this.server.verifyAuthenticationMetrics(1, 0);
  }

  @Test
  public void testInvalidIssuerFailsAuthorization() throws Exception {
    String node = "0";
    SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
    jwsContainer = OAuthUtils.setUpJws(100000, "SomebodyElse", "Confluent", allowedClusters);
    this.configureMechanisms("OAUTHBEARER", Collections.singletonList("OAUTHBEARER"));
    this.server = this.createEchoServer(securityProtocol);

    this.createAndCheckClientConnectionFailure(securityProtocol, node);
    this.server.verifyAuthenticationMetrics(0, 1);
  }

  @Test
  public void testPublicKeyFailsAuthorization() throws Exception {
    String node = "0";
    SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
    jwsContainer = OAuthUtils.setUpJws(100000, "SomebodyElse", "Confluent", allowedClusters);
    OAuthUtils.writePemFile(jwsContainer.getPublicKeyFile(), OAuthUtils.generateKeyPair().getPublic());
    this.configureMechanisms("OAUTHBEARER", Collections.singletonList("OAUTHBEARER"));
    this.server = this.createEchoServer(securityProtocol);

    this.createAndCheckClientConnectionFailure(securityProtocol, node);
    this.server.verifyAuthenticationMetrics(0, 1);
  }

  private void configureMechanisms(String clientMechanism, List<String> serverMechanisms) {
    SecurityTestUtils.attachMechanisms(this.saslClientConfigs, clientMechanism, jwsContainer, allowedCluster);

    SecurityTestUtils.attachServerOAuthConfigs(this.saslServerConfigs, serverMechanisms,
                                               "listener.name.sasl_ssl", jwsContainer);
    TestJaasConfig.createConfiguration(clientMechanism, serverMechanisms);
  }

  private void createAndCheckClientConnection(SecurityProtocol securityProtocol, String node) throws Exception {
    this.createClientConnection(securityProtocol, node);
    NetworkTestUtils.checkClientConnection(this.selector, node, 100, 10);
    this.selector.close();
    this.selector = null;
  }

  private void createAndCheckClientConnectionFailure(SecurityProtocol securityProtocol, String node)
          throws Exception {
    createClientConnection(securityProtocol, node);
    NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.State.AUTHENTICATION_FAILED);
    selector.close();
    selector = null;
  }

  private void createClientConnection(SecurityProtocol securityProtocol, String node) throws Exception {
    this.createSelector(securityProtocol, this.saslClientConfigs);
    InetSocketAddress addr = new InetSocketAddress("localhost", this.server.port());
    this.selector.connect(node, addr, 4096, 4096);
  }

  private void createSelector(SecurityProtocol securityProtocol, Map<String, Object> clientConfigs) {
    if (this.selector != null) {
      this.selector.close();
      this.selector = null;
    }

    String saslMechanism = (String) this.saslClientConfigs.get("sasl.mechanism");
    ChannelBuilder channelBuilder = ChannelBuilders.clientChannelBuilder(securityProtocol, JaasContext.Type.CLIENT,
            new TestSecurityConfig(clientConfigs), (ListenerName) null, saslMechanism, time, true);
    // Create the selector manually instead of using NetworkTestUtils so we can use a longer timeout
    this.selector = new Selector(25000L, new Metrics(), time, "MetricGroup",
        channelBuilder, new LogContext());
  }

  private NioEchoServer createEchoServer(SecurityProtocol securityProtocol) throws Exception {
    return NetworkTestUtils.createEchoServer(
            ListenerName.forSecurityProtocol(securityProtocol), securityProtocol,
            new TestSecurityConfig(this.saslServerConfigs), this.credentialCache, time
    );
  }
}
