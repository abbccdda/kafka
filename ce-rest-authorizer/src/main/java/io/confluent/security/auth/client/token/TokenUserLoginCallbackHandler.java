// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.token;

import io.confluent.security.auth.client.RestClientConfig;
import io.confluent.security.auth.client.provider.BuiltInAuthProviders;
import io.confluent.security.auth.client.rest.RestClient;
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
 *  This class handles the user/password credential approach and can be configured as such.
 *
 * <pre>
 *  org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required
 *      username="user"
 *      password="password"
 *      adminServer="https://mds:8080"
 * </pre>
 *
 * This class should be explicitly set via the
 * {@code sasl.login.callback.handler.class} configuration property
 */
public class TokenUserLoginCallbackHandler extends AbstractTokenLoginCallbackHandler {
  private final Logger log = LoggerFactory.getLogger(
          TokenUserLoginCallbackHandler.class);

  private RestClient restClient;

  public void configure(Map<String, String> configs) {

    final String user = configs.get(USER_OPTION);
    final String pass = configs.get(PASSWORD_OPTION);

    if (user.isEmpty() || pass.isEmpty()) {
      throw new ConfigException(String.format(
              "Both %s and %s are required and have no default values.",
              USER_OPTION, PASSWORD_OPTION));
    }

    configs.put(RestClientConfig.HTTP_AUTH_CREDENTIALS_PROVIDER_PROP,
            BuiltInAuthProviders.HttpCredentialProviders.BASIC.name());

    configs.put(RestClientConfig.BASIC_AUTH_USER_INFO_PROP,
            String.join(":", user, pass));

    restClient = new RestClient(configs);
  }

  void attachAuthToken(OAuthBearerTokenCallback callback) {
    if (callback.token() != null) {
      throw new IllegalArgumentException("Callback had an Authentication Token already");
    }

    callback.token(restClient.login());
  }

  @Override
  public void close() {
    if (restClient != null) {
      restClient.close();
    }
  }
}
