// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.broker.token;

import io.confluent.security.auth.client.RestClientConfig;
import io.confluent.security.auth.client.provider.BuiltInAuthProviders;
import io.confluent.security.auth.client.rest.RestClient;
import io.confluent.security.auth.common.TokenUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

  /**
   * An abstract {@code CallbackHandler} for the OAuthLoginModule.
   *
   * There are two cases which must be covered with Token based authentication.
   *  1. Inter-broker communication using Authentication Tokens
   *  2. Inter-broker communication using another mechanism
   *
   *  <p>
   *    Note: Inter-broker communication is not supported by this callback handler
   *    when running the token service on the same broker.
   *  </p>
   *
   *  The first case will look almost exactly like the client login callback handler.
   *  See {@code TokenBearerLoginCallbackHandler}.
   *
   *  In the event the broker is not configured to use the OAuthLoginModule for inter-broker
   *  communication it will return a null token.
   *
   * With Inter-broker communication using Authentication Tokens
   * <pre>
   *  org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required
   *     metadataServerUrls="http://metadataServerUrls"
   *     publicKeyPath="dir-to-pem-file"
   *     username="broker"
   *     password="broker"
   * </pre>

   * Without Inter-broker communication using Authentication Tokens
   * <pre>
   *  org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required
   *     publicKeyPath="dir-to-pem-file"
   * </pre>
   *
   *
   * <p>This class should be explicitly set via the
   * {@code listener.name.XXX.YYY.sasl.login.callback.handler.class} configuration property
   */

  public class TokenBearerServerLoginCallbackHandler implements AuthenticateCallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(
            TokenBearerServerLoginCallbackHandler.class);

    private RestClient restClient;
    private boolean configured = false;
    private boolean tokenRequired = false;

    private static final String KEY_OPTION = "publicKeyPath";
    private static final String LOGIN_SERVER_OPTION = "metadataServerUrls";
    private static final String USER_OPTION = "username";
    private static final String PASSWORD_OPTION = "password";


    @Override
    public void configure(Map<String, ?> configs, String saslMechanism,
                          List<AppConfigurationEntry> jaasConfigEntries) {

      Map<String, String> moduleOptions = jaasConfigDef(saslMechanism, jaasConfigEntries);

      String publicKeyPath = moduleOptions.getOrDefault(KEY_OPTION, "");

      validatePublicKey(publicKeyPath);

      String loginServer =  moduleOptions.getOrDefault(LOGIN_SERVER_OPTION, "");
      String user = moduleOptions.getOrDefault(USER_OPTION, "");
      String pass = moduleOptions.getOrDefault(PASSWORD_OPTION, "");

      if (user.isEmpty()) {
        configured = true;
        return;
      }

      /*
       * If a user name is configured assume the intent is to
       * to use Authentication Tokens for inter-broker requests
       */
      if (loginServer.isEmpty()) {
        throw new ConfigException(String.format(
                "Missing required configuration %s which has no default value.",
                LOGIN_SERVER_OPTION));
      }

      if (pass.isEmpty()) {
        throw new ConfigException("Option username specified with an empty password");
      }

      Map<String, String> loginConfigs = new HashMap<>();

      loginConfigs.put(RestClientConfig.BOOTSTRAP_METADATA_SERVER_URLS_PROP, loginServer);
      loginConfigs.put(RestClientConfig.HTTP_AUTH_CREDENTIALS_PROVIDER_PROP,
              BuiltInAuthProviders.HttpCredentialProviders.BASIC.name());
      loginConfigs.put(RestClientConfig.BASIC_AUTH_CREDENTIALS_PROVIDER_PROP,
              BuiltInAuthProviders.BasicAuthCredentialProviders.USER_INFO.name());
      loginConfigs.put(RestClientConfig.BASIC_AUTH_USER_INFO_PROP,
              user + ":" + pass);

      restClient = new RestClient(loginConfigs);

      configured = true;
      tokenRequired = true;
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

    private void attachAuthToken(OAuthBearerTokenCallback callback) {
      if (callback.token() != null) {
        throw new IllegalArgumentException("Callback had an Authentication Token already");
      }

      /*
       * Leave token unset since its not needed for inter-broker communication.
       */
      if (!tokenRequired) {
        return;
      }

      callback.token(restClient.login());

    }

    @Override
    public void close() {
      if (restClient != null) {
        restClient.close();
      }
    }

    // Visible for testing
    public static void validatePublicKey(String publicKeyPath) {
      try {
        if (publicKeyPath.isEmpty()) {
          log.error("No publicKeyPath was provided in the JAAS config!");
          throw new ConfigException("publicKeyPath option must be set in JAAS config!");
        }

        TokenUtils.loadPublicKey(new FileInputStream(publicKeyPath));
      } catch (IOException e) {
        String errMsg = String.format("Could not load the public key from %s",
                publicKeyPath);
        log.error(errMsg);
        throw new ConfigException(errMsg, e);
      }
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
  }
