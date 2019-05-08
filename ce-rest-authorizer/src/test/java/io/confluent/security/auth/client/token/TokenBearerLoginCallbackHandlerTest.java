// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.token;

import io.confluent.security.auth.client.rest.RestClient;
import io.confluent.security.auth.common.JwtBearerToken;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.security.auth.callback.Callback;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.powermock.api.easymock.PowerMock.expectNew;
import static org.powermock.api.easymock.PowerMock.replay;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TokenBearerLoginCallbackHandler.class)
public class TokenBearerLoginCallbackHandlerTest {

  private TokenBearerLoginCallbackHandler callbackHandler;
  private JwtBearerToken token;

  @Before
  public void setUp() {
    token = new JwtBearerToken("Token", Collections.emptySet(), -1,
            "", -1L, "");
    callbackHandler = new TokenBearerLoginCallbackHandler();
  }

  @Test(expected = IllegalStateException.class)
  public void testHandleRaisesExceptionIfNotConfigured() throws Exception {
    callbackHandler.handle(new Callback[] {new SaslExtensionsCallback()});
  }

  @Test
  public void testAttachesAuthTokenToCallback() throws Exception {
    RestClient mockClient = createNiceMock(RestClient.class);

    expectNew(RestClient.class, anyObject(HashMap.class)).andReturn(mockClient);
    expect(mockClient.login()).andReturn(token);

    replay(RestClient.class, mockClient);

    OAuthBearerTokenCallback tokenCallback = new OAuthBearerTokenCallback();
    Map<String, Object> jaasConfig = buildClientJassConfigText("Token", "http://url1.com");
    callbackHandler.configure(jaasConfig, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            JaasContext.loadClientContext(jaasConfig).configurationEntries());

    callbackHandler.handle(new Callback[] {tokenCallback});
    assertEquals("Token", tokenCallback.token().value());
  }

  @Test(expected = ConfigException.class)
  public void testAttachesAuthUserInfoToCallback() throws Exception {
    RestClient mockClient = createNiceMock(RestClient.class);

    expectNew(RestClient.class, anyObject(HashMap.class)).andReturn(mockClient);
    expect(mockClient.login()).andReturn(token);

    replay(RestClient.class, mockClient);

    OAuthBearerTokenCallback tokenCallback = new OAuthBearerTokenCallback();
    Map<String, Object> jaasConfig = buildClientJassConfigText(
            "user", "password", "http://url1.com");

    callbackHandler.configure(jaasConfig, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            JaasContext.loadClientContext(jaasConfig).configurationEntries());

    callbackHandler.handle(new Callback[] {tokenCallback});
    assertEquals("Token", tokenCallback.token().value());
  }

  @Test(expected = ConfigException.class)
  public void testConfigureRaisesExceptionOnMissingAuthServiceConfig() {
    Map<String, Object> jaasConfig = buildClientJassConfigText("Token", "");

    callbackHandler.configure(jaasConfig, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            JaasContext.loadClientContext(jaasConfig).configurationEntries());
  }

  @Test(expected = ConfigException.class)
  public void testConfigureRaisesExceptionOnMissingCredentialsConfig() {
    Map<String, Object> jaasConfig = buildClientJassConfigText(null, "http://url2.com");

    callbackHandler.configure(jaasConfig, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            JaasContext.loadClientContext(jaasConfig).configurationEntries());
  }

  private Map<String, Object> buildClientJassConfigText(String user, String pass, String authServer) {
    String jaasConfigText = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required";

    if (user != null && !user.isEmpty()) {
      jaasConfigText += " username=\"" + token + "\"";
    }
    if (pass != null && !pass.isEmpty()) {
      jaasConfigText += " password=\"" + token + "\"";
    }
    if (authServer != null && !authServer.isEmpty()) {
      jaasConfigText += " metadataServerUrl=\"" + authServer + '"';
    }
    jaasConfigText += ";";

    Map<String, Object> tmp = new HashMap<>();
    tmp.put(SaslConfigs.SASL_JAAS_CONFIG, new Password(jaasConfigText));
    return Collections.unmodifiableMap(tmp);
  }

  private Map<String, Object> buildClientJassConfigText(String token, String authServer) {
    String jaasConfigText = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required";

    if (token != null && !token.isEmpty()) {
      jaasConfigText += " authenticationToken=\"" + token + "\"";
    }
    if (authServer != null && !authServer.isEmpty()) {
      jaasConfigText += " metadataServerUrl=\"" + authServer + '"';
    }
    jaasConfigText += ";";

    Map<String, Object> tmp = new HashMap<>();
    tmp.put(SaslConfigs.SASL_JAAS_CONFIG, new Password(jaasConfigText));
    return Collections.unmodifiableMap(tmp);
  }
}
