// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.security.auth.broker.token;

import io.confluent.security.auth.common.JwtBearerToken;
import io.confluent.security.auth.client.rest.RestClient;
import io.confluent.security.auth.utils.TokenUtils;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.security.auth.callback.Callback;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.powermock.api.easymock.PowerMock.expectNew;
import static org.powermock.api.easymock.PowerMock.replay;

  @RunWith(PowerMockRunner.class)
  @PrepareForTest(TokenBearerServerLoginCallbackHandler.class)
  public class TokenBearerServerLoginCallbackHandlerTest {

    private TokenBearerServerLoginCallbackHandler callbackHandler;
    private JwtBearerToken token;
    private TokenUtils.JwsContainer jwsContainer;

    @Before
    public void setUp() throws Exception {
       jwsContainer =
               TokenUtils.setUpJws(36000, "user", "password");

      token = new JwtBearerToken("Token", Collections.emptySet(), -1,
              "", -1L, "");
      callbackHandler = new TokenBearerServerLoginCallbackHandler();

    }

    @Test(expected = IllegalStateException.class)
    public void testHandleRaisesExceptionIfNotConfigured() throws Exception {
      callbackHandler.handle(new Callback[] {new SaslExtensionsCallback()});
    }

    @Test
    public void testAttachesAuthTokenToCallback() throws Exception {
      RestClient mockClient = EasyMock.createNiceMock(RestClient.class);

      expectNew(RestClient.class, EasyMock.anyObject(HashMap.class)).andReturn(mockClient);
      EasyMock.expect(mockClient.login()).andReturn(token);

      replay(RestClient.class, mockClient);

      OAuthBearerTokenCallback tokenCallback = new OAuthBearerTokenCallback();
      Map<String, Object> jaasConfig = buildClientJassConfigText("user", "password", "http://url1.com");
      callbackHandler.configure(jaasConfig, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
              JaasContext.loadClientContext(jaasConfig).configurationEntries());

      callbackHandler.handle(new Callback[] {tokenCallback});
      assertEquals("Token", tokenCallback.token().value());
    }

    @Test
    public void testNullTokenCallback() throws Exception {
      OAuthBearerTokenCallback tokenCallback = new OAuthBearerTokenCallback();
      Map<String, Object> jaasConfig = buildClientJassConfigText(null, null, null);

      callbackHandler.configure(jaasConfig, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
              JaasContext.loadClientContext(jaasConfig).configurationEntries());

      callbackHandler.handle(new Callback[] {tokenCallback});
      assertNull(tokenCallback.token());
    }


    private Map<String, Object> buildClientJassConfigText(String user, String pass, String authServer) {
      String jaasConfigText = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required";

      jaasConfigText += " publicKeyPath=\""
              + jwsContainer.getPublicKeyFile().getAbsolutePath() + "\"";
      if (user != null && !user.isEmpty()) {
        jaasConfigText += " username=\"" + token + "\"";
      }
      if (pass != null && !pass.isEmpty()) {
        jaasConfigText += " password=\"" + token + "\"";
      }
      if (authServer != null && !authServer.isEmpty()) {
        jaasConfigText += " metadataServerUrls=\"" + authServer + '"';
      }
      jaasConfigText += ";";

      Map<String, Object> tmp = new HashMap<>();
      tmp.put(SaslConfigs.SASL_JAAS_CONFIG, new Password(jaasConfigText));
      return Collections.unmodifiableMap(tmp);
    }

  }
