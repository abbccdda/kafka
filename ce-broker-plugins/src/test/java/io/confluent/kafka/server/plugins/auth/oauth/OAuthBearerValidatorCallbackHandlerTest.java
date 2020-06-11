// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth.oauth;

import io.confluent.kafka.common.multitenant.oauth.OAuthBearerJwsToken;
import io.confluent.kafka.multitenant.PhysicalClusterMetadata;
import io.confluent.kafka.multitenant.Utils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.security.auth.callback.Callback;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.confluent.kafka.common.multitenant.oauth.OAuthBearerJwsToken.OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY;
import static io.confluent.kafka.multitenant.Utils.LC_META_ABC;
import static io.confluent.kafka.multitenant.Utils.initiatePhysicalClusterMetadata;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;


public class OAuthBearerValidatorCallbackHandlerTest {
  private OAuthUtils.JwsContainer jwsContainer;
  private String defaultIssuer = "Confluent";
  private String defaultSubject = "Customer";
  private PhysicalClusterMetadata metadata;
  private Map<String, Object> configs;
  private String brokerUUID;
  private String[] defaultAllowedClusters = new String[] {LC_META_ABC.logicalClusterId()};

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    brokerUUID = "uuid";
    configs = new HashMap<>();
    configs.put("broker.session.uuid", brokerUUID);
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
            tempFolder.getRoot().getCanonicalPath());

    metadata = initiatePhysicalClusterMetadata(configs);

    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
    TestUtils.waitForCondition(
            () -> metadata.metadata(LC_META_ABC.logicalClusterId()) != null,
            "Expected metadata of new logical cluster to be present in metadata cache");
  }

  @After
  public void tearDown() {
    metadata.close(brokerUUID);
  }

  @Test
  public void testAttachesJws() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(36000, defaultIssuer, defaultSubject, defaultAllowedClusters);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(
            baseOptions());

    OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(jwsContainer.getJwsToken());
    callbackHandler.handle(new Callback[]{callback});

    assertNotNull(callback.token());
    assertEquals(jwsContainer.getJwsToken(), callback.token().value());
    assertNull(callback.errorStatus());
  }

  @Test
  public void testPopulatesInvalidExtensionsWhenNoLogicalClusterMetadata() throws Exception {
    deleteLogicalClusterMetadata();

    List<String> allowedLogicalClusters = Collections.singletonList(LC_META_ABC.logicalClusterId());
    OAuthBearerToken tokenMock = new OAuthBearerJwsToken("", new HashSet<>(), 0L, "", 0L, "", allowedLogicalClusters);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());
    Map<String, String> extensions = new HashMap<>();
    extensions.put(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY, LC_META_ABC.logicalClusterId());

    OAuthBearerExtensionsValidatorCallback callback = new OAuthBearerExtensionsValidatorCallback(tokenMock, new SaslExtensions(extensions));
    callbackHandler.handle(new Callback[]{callback});

    assertFalse(callback.invalidExtensions().isEmpty());
    assertNotNull(callback.invalidExtensions().get(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY));
    assertFalse(callback.errorMessage().isEmpty());
  }

  @Test
  public void testLogicalClusterExtensionsValidatedWhenTheyMatchTokensLogicalClusterAndIsHostedOnBroker() throws Exception {
    List<String> allowedLogicalClusters = Collections.singletonList(LC_META_ABC.logicalClusterId());
    OAuthBearerToken tokenMock = new OAuthBearerJwsToken("", new HashSet<>(), 0L, "", 0L, "", allowedLogicalClusters);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());
    Map<String, String> extensions = new HashMap<>();
    extensions.put(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY, allowedLogicalClusters.get(0));

    OAuthBearerExtensionsValidatorCallback callback = new OAuthBearerExtensionsValidatorCallback(tokenMock, new SaslExtensions(extensions));
    callbackHandler.handle(new Callback[]{callback});

    assertTrue(callback.invalidExtensions().isEmpty());
    assertTrue(callback.errorMessage().isEmpty());
  }

  @Test
  public void testPopulatesInvalidExtensionsWhenLogicalClusterExtensionDoesntMatchTokensLogicalCLuster() throws Exception {
    List<String> allowedLogicalClusters = Collections.singletonList(LC_META_ABC.logicalClusterId());
    OAuthBearerToken tokenMock = new OAuthBearerJwsToken("", new HashSet<>(), 0L, "", 0L, "", allowedLogicalClusters);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());
    Map<String, String> extensions = new HashMap<>();
    extensions.put(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY, "cluster14013"); // not the expected LKC

    OAuthBearerExtensionsValidatorCallback callback = new OAuthBearerExtensionsValidatorCallback(tokenMock, new SaslExtensions(extensions));
    callbackHandler.handle(new Callback[]{callback});

    assertFalse(callback.invalidExtensions().isEmpty());
    assertNotNull(callback.invalidExtensions().get(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY));
    assertFalse(callback.errorMessage().isEmpty());
  }

  @Test
  public void testPopulatesInvalidExtensionsWhenLogicalClusterIsNotHostedOnBroker() throws Exception {
    List<String> allowedLogicalClusters = Collections.singletonList("cp12"); // not loaded in the PhysicalClusterMetadata class
    OAuthBearerToken tokenMock = new OAuthBearerJwsToken("", new HashSet<>(), 0L, "", 0L, "", allowedLogicalClusters);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());
    Map<String, String> extensions = new HashMap<>();
    extensions.put(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY, allowedLogicalClusters.get(0));

    OAuthBearerExtensionsValidatorCallback callback = new OAuthBearerExtensionsValidatorCallback(tokenMock, new SaslExtensions(extensions));
    callbackHandler.handle(new Callback[]{callback});

    assertFalse(callback.invalidExtensions().isEmpty());
    assertNotNull(callback.invalidExtensions().get(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY));
    assertFalse(callback.errorMessage().isEmpty());
  }

  @Test(expected = ConfigException.class)
  public void testConfigureRaisesExceptionWhenInvalidKeyPath() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(36000, defaultIssuer, defaultSubject, defaultAllowedClusters);
    Map<String, String> options = baseOptions();
    options.put("publicKeyPath", jwsContainer.getPublicKeyFile().getAbsolutePath() + "/invalid!");

    createCallbackHandler(options);
  }

  @Test(expected = ConfigException.class)
  public void testConfigureRaisesExceptionWhenInvalidPhysicalMetadataInstance() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(36000, defaultIssuer, defaultSubject, defaultAllowedClusters);
    configs.put("broker.session.uuid", "made-up");
    createCallbackHandler(baseOptions());
  }

  @Test(expected = OAuthBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionWhenInvalidJws() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(36000, defaultIssuer, defaultSubject, defaultAllowedClusters);
    // override file with new public key
    OAuthUtils.writePemFile(jwsContainer.getPublicKeyFile(), OAuthUtils.generateKeyPair().getPublic());
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  @Test(expected = OAuthBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionWhenExpiredJws() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(50, defaultIssuer, defaultSubject, defaultAllowedClusters);
    Thread.sleep(100);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  @Test(expected = OAuthBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionIfDifferentIssuer() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(36000, "AWS", defaultSubject, defaultAllowedClusters);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  @Test(expected = OAuthBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionIfMissingSubject() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(36000, defaultIssuer, null, defaultAllowedClusters);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  @Test(expected = OAuthBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionIfNoExpirationTime() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(null, defaultIssuer, defaultSubject, defaultAllowedClusters);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  private void deleteLogicalClusterMetadata() throws IOException, InterruptedException {
    Utils.deleteLogicalClusterFile(LC_META_ABC, tempFolder);
    TestUtils.waitForCondition(
            () -> metadata.metadata(LC_META_ABC.logicalClusterId()) == null,
            "Expected metadata of new logical cluster to be removed from metadata cache");
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private OAuthBearerValidatorCallbackHandler createCallbackHandler(Map<String, String> options) {
    TestJaasConfig config = new TestJaasConfig();
    config.createOrUpdateEntry("Kafka", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
            (Map) options);

    OAuthBearerValidatorCallbackHandler callbackHandler = new OAuthBearerValidatorCallbackHandler();
    callbackHandler.configure(configs,
            OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            Collections.singletonList(config.getAppConfigurationEntry("Kafka")[0]));
    return callbackHandler;
  }

  private Map<String, String> baseOptions() throws Exception {
    if (jwsContainer == null) {
      jwsContainer = OAuthUtils.setUpJws(36000, defaultIssuer, defaultSubject, defaultAllowedClusters);
    }
    Map<String, String> options = new HashMap<>();
    options.put("publicKeyPath", jwsContainer.getPublicKeyFile().getAbsolutePath());
    options.put("audience", String.join(","));
    return options;
  }
}
