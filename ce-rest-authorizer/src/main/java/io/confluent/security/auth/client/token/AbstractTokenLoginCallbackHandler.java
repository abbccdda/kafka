// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.token;

import io.confluent.security.auth.client.RestClientConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An abstract {@code CallbackHandler} for the OAuthLoginModule.
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
 *  This abstract class handles the common functionality between them leaving the
 *  specifics up to the individual implementations.
 *
 * <p>Implementations should be explicitly set via the
 * {@code sasl.login.callback.handler.class} configuration property
 */

public abstract class AbstractTokenLoginCallbackHandler
        implements AuthenticateCallbackHandler {

  private boolean configured = false;

  static final String LOGIN_SERVER_OPTION = "adminServer";
  static final String TOKEN_OPTION = "authenticationToken";
  static final String USER_OPTION = "username";
  static final String PASSWORD_OPTION = "password";


  public abstract void configure(Map<String, String> configs);

  abstract void attachAuthToken(OAuthBearerTokenCallback callback);

  @Override
  public void configure(Map<String, ?> configs, String saslMechanism,
                        List<AppConfigurationEntry> jaasConfigEntries) {

    Map<String, String> moduleOptions = jaasConfigDef(saslMechanism, jaasConfigEntries);


    String loginServer =  moduleOptions.getOrDefault(LOGIN_SERVER_OPTION, "");
    String authenticationToken = moduleOptions.getOrDefault(TOKEN_OPTION, "");
    String user = moduleOptions.getOrDefault(USER_OPTION, "");
    String pass = moduleOptions.getOrDefault(PASSWORD_OPTION, "");

    validateHaveCredentials(user, pass, authenticationToken);

    if (loginServer == null || loginServer.isEmpty()) {
      throw new ConfigException(String.format(
              "Missing required configuration %s which has no default value.",
              LOGIN_SERVER_OPTION));
    }

    Map<String, String> loginConfigs = new HashMap<>();

    loginConfigs.put(LOGIN_SERVER_OPTION, loginServer);
    loginConfigs.put(USER_OPTION, user);
    loginConfigs.put(PASSWORD_OPTION, pass);
    loginConfigs.put(TOKEN_OPTION, authenticationToken);
    loginConfigs.put(RestClientConfig.BOOTSTRAP_METADATA_SERVER_URLS_PROP, loginServer);

    configure(loginConfigs);

    configured = true;
  }

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    if (!configured) {
      throw new IllegalStateException("Callback handler not configured");
    }

    for (Callback callback : callbacks) {
      if (callback instanceof OAuthBearerTokenCallback) {
        try {
          attachAuthToken((OAuthBearerTokenCallback) callback);
        } catch (KafkaException e) {
          throw new IOException(e.getMessage(), e);
        }
      } else {
        throw new UnsupportedCallbackException(callback);
      }
    }
  }

  @Override
  public void close() {
    // Noop
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> jaasConfigDef(String saslMechanism,
                                            List<AppConfigurationEntry> jaasConfigEntries) {

    if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism)) {
      throw new IllegalArgumentException(
              String.format("Unexpected SASL mechanism: %s", saslMechanism));
    }
    if (Objects.requireNonNull(jaasConfigEntries).size() != 1
            || jaasConfigEntries.get(0) == null) {
      throw new IllegalArgumentException(
              String.format(
                      "Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
                      jaasConfigEntries.size()));
    }

    return Collections.unmodifiableMap(
            (Map<String, String>) jaasConfigEntries.get(0).getOptions());
  }

  private void validateHaveCredentials(String user, String password,
                                       String authToken) throws ConfigException {

    if (user.isEmpty() && authToken.isEmpty()) {
      throw new ConfigException("Must supply either a user or token credentials");
    }

    if (!user.isEmpty() && password.isEmpty()) {
      throw new ConfigException("Option username specified with an empty password");
    }
  }

}
