// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth.token;

import io.confluent.security.auth.common.JwtBearerToken;
import io.confluent.security.auth.common.TokenUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.PublicKey;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.HashSet;

/**
 * A {@code CallbackHandler} that recognizes
 * {@link TokenBearerValidatorCallbackHandler}
 * for validating a Authentication Tokens issued by Confluent.
 *
 * <p>It verifies the signature of a JWT through a public key it reads from a file path,
 * set in the JAAS config.
 *
 * <p>This class must be explicitly set via the
 * {@code listener.name.sasl_[plaintext|ssl].oauthbearer.sasl.server.callback.handler.class}
 * broker configuration property.
 */
public class TokenBearerValidatorCallbackHandler implements AuthenticateCallbackHandler {
  private static final Logger log = LoggerFactory.getLogger(
          TokenBearerValidatorCallbackHandler.class);
  private static final String AUTH_ERROR_MESSAGE = "Authentication failed";

  private JwtConsumer jwtConsumer;

  static class JwtVerificationException extends Exception {
    JwtVerificationException(String message) {
      super(message);
    }
  }

  private boolean configured = false;

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs, String saslMechanism,
                        List<AppConfigurationEntry> jaasConfigEntries) {
    if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism)) {
      throw new IllegalArgumentException(
              String.format("Unexpected SASL mechanism: %s", saslMechanism));
    }
    if (Objects.requireNonNull(jaasConfigEntries).size() != 1 || jaasConfigEntries.get(0) == null) {
      throw new IllegalArgumentException(
              String.format(
                      "Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
                      jaasConfigEntries.size()
              )
      );
    }

    Map<String, String> moduleOptions = Collections.unmodifiableMap(
            (Map<String, String>) jaasConfigEntries.get(0).getOptions());

    try {
      String publicKeyPath = moduleOptions.get("publicKeyPath");
      if (publicKeyPath == null) {
        throw new ConfigException("publicKeyPath option must be set in JAAS config!");
      }

      PublicKey publicKey = TokenUtils.loadPublicKey(new FileInputStream(publicKeyPath));
      jwtConsumer = TokenUtils.createJwtConsumer(publicKey);
    } catch (IOException e) {
      throw new ConfigException(String.format("Could not load the public key from %s",
              moduleOptions.get("publicKeyPath")), e);
    }

    configured = true;
  }


  @Override
  public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
    if (!configured) {
      throw new IllegalStateException("Callback handler not configured");
    }
    for (Callback callback : callbacks) {
      if (callback instanceof OAuthBearerValidatorCallback) {
        OAuthBearerValidatorCallback validationCallback = (OAuthBearerValidatorCallback) callback;
        try {
          handleValidatorCallback(validationCallback);
        } catch (JwtVerificationException e) {
          log.debug("Failed to verify token: {}", e);
          validationCallback.error(AUTH_ERROR_MESSAGE, null, null);
        }
      } else if (callback instanceof OAuthBearerExtensionsValidatorCallback) {
        handleExtensionsCallback((OAuthBearerExtensionsValidatorCallback) callback);
      } else {
        throw new UnsupportedCallbackException(callback);
      }
    }
  }

  @Override
  public void close() {
    // empty
  }

  private void handleExtensionsCallback(OAuthBearerExtensionsValidatorCallback callback) {
    /* Ignore unrecognized extensions */
  }

  private void handleValidatorCallback(OAuthBearerValidatorCallback callback)
          throws JwtVerificationException {
    String tokenValue = callback.tokenValue();
    if (tokenValue == null) {
      throw new IllegalArgumentException("Callback missing required token value");
    }

    JwtBearerToken token = processToken(tokenValue);
    callback.token(token);
    log.debug("Successfully validated token from principal {}",  token.principalName());
  }

  /**
   * Validates the JWS token against the jwtConsumer's requirements (e.g not expired, valid issuer).
   * See {@link TokenUtils#createJwtConsumer(PublicKey)} for the JWS token requirements
   */
  JwtBearerToken processToken(String jws) throws JwtVerificationException {
    try {
      JwtClaims claims = jwtConsumer.processToClaims(jws);

      return new JwtBearerToken(jws,
              new HashSet<>(),
              claims.getExpirationTime().getValueInMillis(),
              claims.getSubject(),
              claims.getIssuedAt().getValueInMillis(),
              claims.getJwtId());
    } catch (InvalidJwtException | MalformedClaimException e) {
      throw new JwtVerificationException(e.getMessage());
    }
  }
}