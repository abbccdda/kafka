// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.kafka.clients.plugins.auth.oauth;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.callback.Callback;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.common.multitenant.oauth.OAuthBearerJwsToken.OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY;
import static org.junit.Assert.assertEquals;


public class OAuthBearerLoginCallbackHandlerTest {
    private OAuthBearerLoginCallbackHandler callbackHandler;

    @Before
    public void setUp() {
        callbackHandler = new OAuthBearerLoginCallbackHandler();
    }

    @Test(expected = IllegalStateException.class)
    public void testHandleRaisesExceptionIfNotConfigured() throws Exception {
        callbackHandler.handle(new Callback[] {new SaslExtensionsCallback()});
    }

    @Test
    public void testAttachesAuthTokenToCallback() throws Exception {
        OAuthBearerTokenCallback tokenCallback = new OAuthBearerTokenCallback();
        Map<String, Object> jaasConfig = buildClientJassConfigText("Token", "Cluster1");
        callbackHandler.configure(jaasConfig, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                JaasContext.loadClientContext(jaasConfig).configurationEntries());

        callbackHandler.handle(new Callback[] {tokenCallback});

        assertEquals("Token", tokenCallback.token().value());
    }

    @Test
    public void testAttachesClusterToExtensionCallback() throws Exception {
        SaslExtensionsCallback extCallback = new SaslExtensionsCallback();
        Map<String, Object> jaasConfig = buildClientJassConfigText("Token", "Cluster1");
        callbackHandler.configure(jaasConfig, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                JaasContext.loadClientContext(jaasConfig).configurationEntries());

        callbackHandler.handle(new Callback[] {extCallback});

        assertEquals("Cluster1", extCallback.extensions().map().get(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY));
    }

    @Test
    public void testConfigureDoesntRaiseIfBothTokenAndClusterProvided() {
        Map<String, Object> jaasConfig = buildClientJassConfigText("Token", "Cluster1");

        callbackHandler.configure(jaasConfig, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                JaasContext.loadClientContext(jaasConfig).configurationEntries());
    }

    @Test(expected = ConfigException.class)
    public void testConfigureRaisesExceptionOnMissingClusterConfig() {
        Map<String, Object> jaasConfig = buildClientJassConfigText("Token", null);

        callbackHandler.configure(jaasConfig, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                JaasContext.loadClientContext(jaasConfig).configurationEntries());
    }

    @Test(expected = ConfigException.class)
    public void testConfigureRaisesExceptionOnMissingTokenConfig() {
        Map<String, Object> jaasConfig = buildClientJassConfigText(null, "Cluster");

        callbackHandler.configure(jaasConfig, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                JaasContext.loadClientContext(jaasConfig).configurationEntries());
    }

    private Map<String, Object> buildClientJassConfigText(String token, String cluster) {
        String jaasConfigText = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required";

        if (token != null && !token.isEmpty()) {
            jaasConfigText += " token=\"" + token + "\"";
        }
        if (cluster != null && !cluster.isEmpty()) {
            jaasConfigText += " cluster=\"" + cluster + '"';
        }
        jaasConfigText += ";";

        Map<String, Object> tmp = new HashMap<>();
        tmp.put(SaslConfigs.SASL_JAAS_CONFIG, new Password(jaasConfigText));
        return Collections.unmodifiableMap(tmp);
    }
}
