// (Copyright) [2019 - 2019] Confluent, Inc.
package io.confluent.kafka.server.plugins.auth;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.MultiTenantSaslServer;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.server.plugins.auth.stats.AuthenticationStats;
import io.confluent.kafka.server.plugins.auth.stats.TenantAuthenticationStats;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Optional;
import javax.net.ssl.SNIHostName;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.apache.kafka.server.audit.AuthenticationErrorInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.server.audit.AuthenticationErrorInfo.UNKNOWN_USER_ERROR;

/**
 * SaslServer implementation for SASL/PLAIN with an authenticator
 * provided through the constructor.
 */
public class PlainSaslServer implements MultiTenantSaslServer {

  public static final String PLAIN_MECHANISM = "PLAIN";
  public static final String SNI_BROKER_HOST_NAME = "SNIBrokerHostName";
  private final SaslAuthenticator authenticator;
  private static final AuthenticationStats STATS = AuthenticationStats.getInstance();
  private static final TenantAuthenticationStats TENANT_STATS =
      TenantAuthenticationStats.instance();
  private static final Logger log =
          LoggerFactory.getLogger(PlainSaslServer.class);

  private boolean complete;
  private String authorizationID;
  private TenantMetadata tenantMetadata;
  private String username = "";
  private Optional<SNIHostName> sniHostName;

  public PlainSaslServer(List<AppConfigurationEntry> jaasContextEntries,
                         SaslAuthenticator authenticator,
                         Optional<SNIHostName> sniHostNameOptional) {
    this.authenticator = authenticator;
    authenticator.initialize(jaasContextEntries);
    this.sniHostName = sniHostNameOptional;
  }

  @Override
  public byte[] evaluateResponse(byte[] response) throws SaslException {
    try {
      byte[] result = doEvaluateResponse(response);
      STATS.incrSucceeded();
      return result;
    } catch (Exception e) {
      STATS.incrFailed();
      String cause = e.getCause() == null ? "" : e.getCause().getMessage();
      log.debug("SASL/PLAIN authentication failed: {}", cause, e);
      throw e;
    } finally {
      clearMdc();
    }
  }

  private void clearMdc() {
    MDC.remove("username");
    MDC.remove("saslMechanism");
    MDC.remove("authorizationId");
    MDC.remove("tenant");
  }

  private byte[] doEvaluateResponse(byte[] responseBytes) throws SaslException {
    /*
     * Message format (from https://tools.ietf.org/html/rfc4616):
     *
     * message   = [authzid] UTF8NUL authcid UTF8NUL passwd
     * authcid   = 1*SAFE ; MUST accept up to 255 octets
     * authzid   = 1*SAFE ; MUST accept up to 255 octets
     * passwd    = 1*SAFE ; MUST accept up to 255 octets
     * UTF8NUL   = %x00 ; UTF-8 encoded NUL character
     *
     * SAFE      = UTF1 / UTF2 / UTF3 / UTF4
     *                ;; any UTF-8 encoded Unicode character except NUL
     */

    MDC.put("saslMechanism", "PLAIN");

    String response = new String(responseBytes, StandardCharsets.UTF_8);
    List<String> tokens = extractTokens(response);
    String authorizationIdFromClient = tokens.get(0);
    String username = tokens.get(1);
    String password = tokens.get(2);

    if (username.isEmpty()) {
      throw new SaslAuthenticationException("Authentication failed: username not specified", UNKNOWN_USER_ERROR);
    }
    MDC.put("username", username);

    this.username = username;

    if (password.isEmpty()) {
      AuthenticationErrorInfo errorInfo = getErrorInfo();
      throw new SaslAuthenticationException("Authentication failed: password not specified", errorInfo);
    }

    if (!authorizationIdFromClient.isEmpty() && !authorizationIdFromClient.equals(username)) {
      AuthenticationErrorInfo errorInfo = getErrorInfo();
      throw new SaslAuthenticationException("Authentication failed: Client requested an authorization id that is different from username", errorInfo);
    }

    MultiTenantPrincipal principal = authenticator.authenticate(username, password, Optional.empty());
    authorizationID = principal.user();
    MDC.put("authorizationId", authorizationID);
    tenantMetadata = principal.tenantMetadata();
    MDC.put("tenant", tenantMetadata.tenantName);
    TENANT_STATS.onSuccessfulAuthentication(principal);

    log.debug("SASL/PLAIN authentication succeeded for user {}", username);
    complete = true;
    return new byte[0];
  }

  private AuthenticationErrorInfo getErrorInfo() throws SaslException {
    String clusterId = authenticator.clusterId(username).orElse("");
    return new AuthenticationErrorInfo(
            AuditEventStatus.UNAUTHENTICATED, "", username, clusterId);
  }

  private List<String> extractTokens(String string) {
    List<String> tokens = new ArrayList<>();
    int startIndex = 0;
    for (int i = 0; i < 4; ++i) {
      int endIndex = string.indexOf("\u0000", startIndex);
      if (endIndex == -1) {
        tokens.add(string.substring(startIndex));
        break;
      }
      tokens.add(string.substring(startIndex, endIndex));
      startIndex = endIndex + 1;
    }

    if (tokens.size() != 3)
      throw new SaslAuthenticationException("Invalid SASL/PLAIN response: expected 3 tokens, got " +
          tokens.size(), UNKNOWN_USER_ERROR);

    return tokens;
  }

  @Override
  public TenantMetadata tenantMetadata() {
    throwIfNotComplete();
    return tenantMetadata;
  }

  @Override
  public String getAuthorizationID() {
    throwIfNotComplete();
    return authorizationID;
  }

  @Override
  public String getMechanismName() {
    return PLAIN_MECHANISM;
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    throwIfNotComplete();
    return null;
  }

  @Override
  public boolean isComplete() {
    return complete;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) {
    throwIfNotComplete();
    return Arrays.copyOfRange(incoming, offset, offset + len);
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) {
    throwIfNotComplete();
    return Arrays.copyOfRange(outgoing, offset, offset + len);
  }

  @Override
  public void dispose() { }

  private void throwIfNotComplete() {
    if (!complete) {
      throw new IllegalStateException("Authentication exchange has not completed");
    }
  }

  public static class PlainSaslServerFactory implements SaslServerFactory {
    private final SaslServerSupplier saslServerSupplier;

    public PlainSaslServerFactory(SaslServerSupplier saslServerSupplier) {
      this.saslServerSupplier = saslServerSupplier;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SaslServer createSaslServer(String mechanism, String protocol, String serverName,
            Map<String, ?> props, CallbackHandler cbh) throws SaslException {

      if (!PLAIN_MECHANISM.equals(mechanism)) {
        throw new SaslException(String.format("Mechanism \'%s\' is not supported. "
                + "Only PLAIN is supported.", mechanism));
      }

      if (!(cbh instanceof PlainServerCallbackHandler)) {
        throw new SaslException("CallbackHandler must be of type PlainServerCallbackHandler, "
                + "but it is: " + cbh.getClass());
      }

      // Note: This is a temporary fix to obtain JAAS configuration entries in PlainSaslServer
      // This should be replaced with a custom callback handler that is configured by Kafka with
      // the JAAS configuration entries.
      try {
        Field field = PlainServerCallbackHandler.class.getDeclaredField("jaasConfigEntries");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<AppConfigurationEntry> jaasContextEntries =
            (List<AppConfigurationEntry>) field.get(cbh);
        Optional<SNIHostName> sniHostName =
                props.containsKey(SNI_BROKER_HOST_NAME) ? ((Optional<SNIHostName>) props.get(SNI_BROKER_HOST_NAME)) : Optional.empty();
        return saslServerSupplier.get(jaasContextEntries, sniHostName);
      } catch (Throwable e) {
        throw new SaslException("Could not obtain JAAS context", e);
      }
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      String noPlainText = (String) props.get(Sasl.POLICY_NOPLAINTEXT);
      if ("true".equals(noPlainText)) {
        return new String[]{};
      } else {
        return new String[]{PLAIN_MECHANISM};
      }
    }
  }


  @Override
  public String userIdentifier() {
    return username;
  }

}
