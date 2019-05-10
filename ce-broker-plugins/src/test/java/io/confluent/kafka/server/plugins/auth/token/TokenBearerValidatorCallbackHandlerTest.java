// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth.token;

import io.confluent.kafka.test.utils.TokenTestUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.security.auth.callback.Callback;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;


public class TokenBearerValidatorCallbackHandlerTest {
  private TokenTestUtils.JwsContainer jwsContainer;
  private String defaultIssuer = "Confluent";
  private String defaultSubject = "Customer";
  private Map<String, Object> configs;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    configs = new HashMap<>();
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
            tempFolder.getRoot().getCanonicalPath());

  }

  @After
  public void tearDown() {

  }

  @Test
  public void testAttachesJws() throws Exception {
    jwsContainer = TokenTestUtils.setUpJws(36000, defaultIssuer, defaultSubject);
    TokenBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(
            baseOptions());

    OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(jwsContainer.getJwsToken());
    callbackHandler.handle(new Callback[]{callback});

    assertNotNull(callback.token());
    assertEquals(jwsContainer.getJwsToken(), callback.token().value());
    assertNull(callback.errorStatus());
  }


  @Test(expected = ConfigException.class)
  public void testConfigureRaisesExceptionWhenInvalidKeyPath() throws Exception {
    jwsContainer = TokenTestUtils.setUpJws(36000, defaultIssuer, defaultSubject);
    Map<String, String> options = baseOptions();
    options.put("publicKeyPath", jwsContainer.getPublicKeyFile().getAbsolutePath() + "/invalid!");

    createCallbackHandler(options);
  }

  @Test(expected = TokenBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionWhenInvalidJws() throws Exception {
    jwsContainer = TokenTestUtils.setUpJws(36000, defaultIssuer, defaultSubject);
    // override file with new public key
    TokenTestUtils.writePemFile(jwsContainer.getPublicKeyFile(), TokenTestUtils.generateKeyPair().getPublic());
    TokenBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  @Test(expected = TokenBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionWhenExpiredJws() throws Exception {
    jwsContainer = TokenTestUtils.setUpJws(50, defaultIssuer, defaultSubject);
    Thread.sleep(100);
    TokenBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  @Test(expected = TokenBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionIfDifferentIssuer() throws Exception {
    jwsContainer = TokenTestUtils.setUpJws(36000, "AWS", defaultSubject);
    TokenBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  @Test(expected = TokenBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionIfMissingSubject() throws Exception {
    jwsContainer = TokenTestUtils.setUpJws(36000, defaultIssuer, null);
    TokenBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  @Test(expected = TokenBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionIfNoExpirationTime() throws Exception {
    jwsContainer = TokenTestUtils.setUpJws(null, defaultIssuer, defaultSubject);
    TokenBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private TokenBearerValidatorCallbackHandler createCallbackHandler(Map<String, String> options) {
    TestJaasConfig config = new TestJaasConfig();
    config.createOrUpdateEntry("Kafka", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
            (Map) options);

    TokenBearerValidatorCallbackHandler callbackHandler = new TokenBearerValidatorCallbackHandler();
    callbackHandler.configure(configs,
            OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            Collections.singletonList(config.getAppConfigurationEntry("Kafka")[0]));
    return callbackHandler;
  }

  private Map<String, String> baseOptions() throws Exception {
    if (jwsContainer == null) {
      jwsContainer = TokenTestUtils.setUpJws(36000, defaultIssuer, defaultSubject);
    }
    Map<String, String> options = new HashMap<>();
    options.put("publicKeyPath", jwsContainer.getPublicKeyFile().getAbsolutePath());
    options.put("audience", String.join(","));
    return options;
  }
}
