// (Copyright) [2019 - 2019] Confluent, Inc.
package io.confluent.kafka.multitenant.integration.test;

import io.confluent.kafka.clients.plugins.auth.oauth.OAuthBearerLoginCallbackHandler;
import io.confluent.kafka.multitenant.PhysicalClusterMetadata;
import io.confluent.kafka.multitenant.Utils;
import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;
import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;
import io.confluent.kafka.server.plugins.auth.oauth.OAuthBearerServerLoginCallbackHandler;
import io.confluent.kafka.server.plugins.auth.oauth.OAuthBearerValidatorCallbackHandler;
import io.confluent.kafka.server.plugins.auth.oauth.OAuthUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.confluent.kafka.multitenant.Utils.LC_META_ABC;
import static io.confluent.kafka.multitenant.Utils.initiatePhysicalClusterMetadata;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

/**
 * An integration test testing OAUTHBEARER with an AdminClient
 */
@Category(IntegrationTest.class)
public class AdminClientIntegrationTest {
  private static final Logger log = LoggerFactory.getLogger(AdminClientIntegrationTest.class);

  private final String logicalCluster = "lkc-11111";
  private final int adminUserId = 100;
  private final Properties adminProperties = new Properties();
  {
    adminProperties.put("sasl.login.callback.handler.class", OAuthBearerLoginCallbackHandler.class.getName());
  }
  private IntegrationTestHarness testHarness;
  private OAuthUtils.JwsContainer jwsContainer;
  private String allowedCluster = LC_META_ABC.logicalClusterId();
  private String[] allowedClusters = new String[] {allowedCluster};
  private String brokerUUID;
  private PhysicalClusterMetadata metadata;
  private Map<String, Object> configs;
  private List<NewTopic> sampleTopics = Collections.singletonList(new NewTopic("abcd", 3, (short) 1));

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private void setUp() throws Exception {
    setUp(allowedClusters);
  }

  private void setUp(String[] allowedClusters) throws Exception {
    testHarness = new IntegrationTestHarness();

    String subject = logicalCluster + "_APIKEY" + adminUserId;
    jwsContainer = OAuthUtils.setUpJws(100000, "Confluent", subject, allowedClusters);
    PhysicalCluster physicalCluster = testHarness.start(setUpMetadata(brokerProps()));


    physicalCluster.createLogicalCluster(logicalCluster, adminUserId, 1);
  }

  @After
  public void tearDown() throws Exception {
    testHarness.shutdown();
    metadata.close(brokerUUID);
  }

  private Properties setUpMetadata(Properties brokerProps) throws IOException, InterruptedException {
    brokerUUID = "uuid";
    configs = new HashMap<>();
    configs.put("broker.session.uuid", brokerUUID);
    brokerProps.put("broker.session.uuid", brokerUUID);
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
            tempFolder.getRoot().getCanonicalPath());

    metadata = initiatePhysicalClusterMetadata(configs);

    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
    TestUtils.waitForCondition(
            () -> metadata.metadata(LC_META_ABC.logicalClusterId()) != null,
            "Expected metadata of new logical cluster to be present in metadata cache");

    return brokerProps;
  }

  private Properties brokerProps() {
    Properties props = new Properties();
    props.put("sasl.enabled.mechanisms", Collections.singletonList("OAUTHBEARER"));
    props.put("listener.name.external.oauthbearer.sasl.login.callback.handler." +
            "class", OAuthBearerServerLoginCallbackHandler.class.getName());
    props.put("listener.name.external.oauthbearer.sasl.server.callback" +
            ".handler.class", OAuthBearerValidatorCallbackHandler.class.getName());
    props.put("listener.name.external.oauthbearer.sasl.jaas.config",
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                    "publicKeyPath=\"" +
                    jwsContainer.getPublicKeyFile().toPath() +  "\";");
    props.put("client.quota.callback.class", TenantQuotaCallback.class.getName());
    return props;
  }

  private String clientJaasConfig(String jwsToken, String allowedCluster) {
    return "org.apache.kafka.common.security." +
            "oauthbearer.OAuthBearerLoginModule Required token=\"" + jwsToken +
            "\" cluster=\"" + allowedCluster + "\";";
  }

  @Test
  public void testCorrectConfigurationAuthenticatesSuccessfully() throws Exception {
    setUp();
    AdminClient client = testHarness.createOAuthAdminClient(clientJaasConfig(jwsContainer.getJwsToken(), allowedCluster), adminProperties);
    client.createTopics(sampleTopics).all().get();

    List<String> expectedTopics = sampleTopics.stream().map(NewTopic::name)
            .collect(Collectors.toList());
    assertTrue(client.listTopics().names().get().containsAll(expectedTopics));
  }

  @Test
  public void testAllowedClusterExtensionNotInTokenThrowsException() throws Exception {
    setUp();
    Class expectedException = SaslAuthenticationException.class;
    try {
      AdminClient client = testHarness.createOAuthAdminClient(clientJaasConfig(jwsContainer.getJwsToken(), "wrong"), adminProperties);
      client.createTopics(sampleTopics).all().get();
      fail(String.format("Expected admin command to throw a %s", expectedException));
    } catch (Exception e) {
      if (e.getCause().getClass() != expectedException)
        fail(String.format("Expected admin command to throw a %s but it threw a %s", expectedException, e.getCause().getClass()));
      log.info("Expected exception message: {}", e.getCause().getMessage());
    }
  }

  @Test
  public void testAllowedClusterNotHostedOnBrokerThrowsException() throws Exception {
    setUp(new String[] {"other-cluster"}); // different cluster than hosted in metadata
    Class expectedException = SaslAuthenticationException.class;
    try {
      AdminClient client = testHarness.createOAuthAdminClient(clientJaasConfig(jwsContainer.getJwsToken(), "other-cluster"), adminProperties);
      client.createTopics(sampleTopics).all().get();
      fail(String.format("Expected admin command to throw a %s", expectedException));
    } catch (Exception e) {
      if (e.getCause().getClass() != expectedException)
        fail(String.format("Expected admin command to throw a %s but it threw a %s", expectedException, e.getCause().getClass()));
      log.info("Expected exception message: {}", e.getCause().getMessage());
    }
  }

  @Test
  public void testInvalidTokenThrowsException() throws Exception {
    setUp(new String[] {}); // an empty clusters claim in the token is considered invalid
    Class expectedException = SaslAuthenticationException.class;
    try {
      testHarness.createOAuthAdminClient(clientJaasConfig(jwsContainer.getJwsToken(), allowedCluster), adminProperties).createTopics(sampleTopics).all().get();
      fail(String.format("Expected admin command to throw a %s", expectedException));
    } catch (Exception e) {
      if (e.getCause().getClass() != expectedException)
        fail(String.format("Expected admin command to throw a %s but it threw a %s", expectedException, e.getCause().getClass()));
      log.info("Expected exception message: {}", e.getCause().getMessage());
    }
  }

  @Test
  public void testIllegalStateExceptionOnBrokerMetadataThrowsException() throws Exception {
    setUp();
    Class expectedException = SaslAuthenticationException.class;
    try {
      metadata.close(brokerUUID);
      AdminClient client = testHarness.createOAuthAdminClient(clientJaasConfig(jwsContainer.getJwsToken(), allowedCluster), adminProperties);
      client.createTopics(sampleTopics).all().get();
      fail(String.format("Expected admin command to throw a %s", expectedException));
    } catch (Exception e) {
      if (e.getCause().getClass() != expectedException)
        fail(String.format("Expected admin command to throw a %s but it threw a %s", expectedException, e.getCause().getClass()));
      log.info("Expected exception message: {}", e.getCause().getMessage());
    }
  }
}
