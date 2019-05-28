// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.kafka.clients.plugins.auth.token;

import static io.confluent.security.auth.client.provider
        .BuiltInAuthProviders.HttpCredentialProviders;

import io.confluent.security.auth.client.RestClientConfig;
import io.confluent.security.auth.client.rest.RestClient;
import io.confluent.security.auth.common.JwtBearerToken;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A {@code CallbackHandler} for the OAuthLoginModule.
 *
 * There are three cases which must be covered with Token based authentication.
 *  1. Kafka client-side User/Password credential authentication
 *  2. Kafka client-side Token credential authentication
 *  3. Kafka client impersonation.
 *
 *  All three circumstances can be handled through the use of two callback handlers.
 *
 *  {@code TokenUserBearerLoginCallbackHandler} for User/Password credential authentication.
 *  {@code TokenBearerLoginCallbackHandler} for Token credential authentication.
 *  Any application with an valid Authentication Token may impersonate another user making
 *  {@code TokenBearerLoginCallbackHandler} suitable for scenario 3 as well.
 *
 *  This class handles the token credential approach and can be configured as such.
 *
 * <pre>
 *  org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required
 *      authenticationToken="long-token-string"
 *      metadataServerUrls="https://mds:8080"
 * </pre>
 *
 * This class should be explicitly set via the
 * {@code sasl.login.callback.handler.class} configuration property
 */

public class TokenBearerLoginCallbackHandler
        extends AbstractTokenLoginCallbackHandler {
  private final Logger log = LoggerFactory.getLogger(
          TokenBearerLoginCallbackHandler.class);

  private RestClient restClient;
  private Map<String, String> configs;

  public void configure(Map<String, String> configs) {

    this.configs = configs;
    final String authenticationToken = configs.get(TOKEN_OPTION);

    if (authenticationToken.isEmpty()) {
      throw new ConfigException(String.format(
              "Missing required configuration %s which has no default value.",
              TOKEN_OPTION));
    }

    createRestClient(configs, authenticationToken);
  }

  private void createRestClient(final Map<String, String> configs, final String authenticationToken) {
    closeRestClient();

    configs.put(RestClientConfig.HTTP_AUTH_CREDENTIALS_PROVIDER_PROP,
              HttpCredentialProviders.BEARER.name());
    configs.put(RestClientConfig.TOKEN_AUTH_CREDENTIAL_PROP,
              authenticationToken);
    restClient = new RestClient(configs);
  }

  void attachAuthToken(OAuthBearerTokenCallback callback) {
    if (callback.token() != null) {
      throw new IllegalArgumentException("Callback had an Authentication Token already");
    }

    JwtBearerToken token = restClient.login();
    createRestClient(configs, token.value());
    callback.token(token);
  }

  @Override
  public void close() {
    closeRestClient();
  }

  private void closeRestClient() {
    if (restClient != null) {
      restClient.close();
    }
  }
}