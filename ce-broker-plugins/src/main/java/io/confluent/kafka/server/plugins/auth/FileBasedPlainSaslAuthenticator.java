// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth;

import io.confluent.kafka.multitenant.TenantMetadata;
import javax.net.ssl.SNIHostName;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.apache.kafka.server.audit.AuthenticationErrorInfo;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.server.audit.AuditEventStatus.UNAUTHENTICATED;
import static org.apache.kafka.server.audit.AuditEventStatus.UNKNOWN_USER_DENIED;

/**
 * Authenticate users based on YAML config file which is periodically reloaded
 * <p>
 * Since we have to use JAAS, the JAAS config just points to
 * the YAML config file.
 * </p>
 */
public class FileBasedPlainSaslAuthenticator implements SaslAuthenticator {
  static final String JAAS_ENTRY_CONFIG = "config_path";
  static final String JAAS_ENTRY_REFRESH_MS = "refresh_ms";
  static final String SNI_HOST_NAME_VALIDATION_MODE = "sni_host_name_validation_mode";

  /** Visible for testing **/
  protected static final String AUTHENTICATION_FAILED_MSG = "Authentication failed";
  protected static final String NOT_PROVIDED_MSG = "<not-provided>";

  private static final Logger log = LoggerFactory.getLogger(FileBasedPlainSaslAuthenticator.class);
  private static final int BCRYPT_PASSSWORD_CACHE_SIZE = 2048;
  private static final String SASL_MECHANISM_PLAIN = "PLAIN";
  private static final LRUCache<String, String> BCRYPT_PASSWORD_CACHE = new LRUCache<>(BCRYPT_PASSSWORD_CACHE_SIZE);
  public static final String LKC_PREFIX = "lkc-";

  private SecretsLoader loader;
  private SNIValidationMode mode;

  @Override
  public void initialize(List<AppConfigurationEntry>  jaasContextEntries) {
    String configFilePath = configEntryOption(
            jaasContextEntries, JAAS_ENTRY_CONFIG, FileBasedLoginModule.class.getName());
    long refreshMs = Long.parseLong(configEntryOption(
            jaasContextEntries, JAAS_ENTRY_REFRESH_MS, FileBasedLoginModule.class.getName()));
    this.mode = SNIValidationMode.fromString(configEntryOption(
            jaasContextEntries, SNI_HOST_NAME_VALIDATION_MODE, FileBasedLoginModule.class.getName()));
    log.info(String.format(
            "FileBasedPlainSaslAuthenticator initialized with mode: %s, refreshMs: %d and config path: %s.",
            this.mode.getText(), refreshMs, configFilePath));
    loader = new SecretsLoader(configFilePath, refreshMs);
  }

  /**
   * Authenticate user by checking username, password and cluster id.
   *
   * @param username username
   * @param password password
   * @param sniHostName SNI broker hostname. We store host name in SNI to route to relevant broker.
   * @return {@link MultiTenantPrincipal} containing authorization id and tenant
   * @throws SaslAuthenticationException if authentication fails.
   *         Note that the error string in the SaslAuthenticationException
   *         is returned to the client so do not leak information in it.
   * @throws SaslException if any unexpected errors are encountered
   */
  @Override
  public MultiTenantPrincipal authenticate(String username, String password, Optional<SNIHostName> sniHostName)
      throws SaslException, SaslAuthenticationException {
    try {
      Optional<String> suppliedClusterId = extractClusterIdFromHostName(sniHostName);
      Map<String, KeyConfigEntry> userInfos = loader.get();
      verifyUserExist(userInfos, username, suppliedClusterId);
      KeyConfigEntry userInfo = userInfos.get(username);
      verifySaslMechanismMatch(userInfo, username);
      verifyPassword(userInfo, username, password);
      verifyBrokerHostName(userInfo, username, sniHostName, suppliedClusterId, this.mode);
      TenantMetadata tenantMetadata = new TenantMetadata.Builder(userInfo.logicalClusterId).superUser(!userInfo.serviceAccount()).build();
      return new MultiTenantPrincipal(userInfo.userId, tenantMetadata);
    } catch (SaslAuthenticationException e) {
      throw e;
    } catch (Exception e) {
      log.error("Unexpected exception during authentication for user {}", username, e);
      throw new SaslException("Authentication failed: Unexpected exception", e);
    }
  }

  private void verifyUserExist(Map<String, KeyConfigEntry> credentials, String username, Optional<String> suppliedClusterId) {
    if (!credentials.containsKey(username)) {
      String errorMessage = "Unknown user " + username;
      AuthenticationErrorInfo errorInfo = logAndPrepareErrorInfo(
              UNKNOWN_USER_DENIED, username, suppliedClusterId.orElse(""), errorMessage);
      throw new SaslAuthenticationException(AUTHENTICATION_FAILED_MSG, errorInfo);
    }
  }

  private void verifySaslMechanismMatch(KeyConfigEntry userInfo, String username) {
    if (!SASL_MECHANISM_PLAIN.equals(userInfo.saslMechanism)) {
      String errorMessage = "Wrong SASL mechanism " + userInfo.saslMechanism + " for user " + username;
      AuthenticationErrorInfo errorInfo = logAndPrepareErrorInfo(
              UNAUTHENTICATED, username, userInfo.logicalClusterId, errorMessage);
      throw new SaslAuthenticationException(AUTHENTICATION_FAILED_MSG, errorInfo);
    }
  }

  private void verifyPassword(KeyConfigEntry userInfo, String username, String password) {
    switch (userInfo.hashFunction) {
      case "none":
        if (!userInfo.hashedSecret.equals(password)) {
          String errorMessage = "Bad password for user " + username;
          AuthenticationErrorInfo errorInfo = logAndPrepareErrorInfo(
                  UNAUTHENTICATED, username, userInfo.logicalClusterId, errorMessage);
          throw new SaslAuthenticationException(AUTHENTICATION_FAILED_MSG, errorInfo);
        }
        break;
      case "bcrypt":
        authenticateBcrypt(userInfo, username, password);
        break;
      default:
        String errorMessage = "Unknown hash function: " + userInfo.hashFunction + " for user " + userInfo.userId;
        AuthenticationErrorInfo errorInfo = logAndPrepareErrorInfo(UNAUTHENTICATED, username, userInfo.logicalClusterId, errorMessage);
        throw new SaslAuthenticationException(AUTHENTICATION_FAILED_MSG, errorInfo);
    }
  }

  /**
   * Validate SNI hostname.
   *
   * There are three modes:
   *
   * - optional_validation: clusters that uses the v3 CCloud Network architecture
   *   Validate the hostname only if a lkc prefixed hostname is provided. If not present, let it pass.
   *
   * - allow_legacy_bootstrap: v4 network architecture with legacy bootstrap (before all customers have changed their bootstrap configs)
   *   Validate lkc-prefixed host names and let pkc-prefixed host names pass.
   *   The latter is used for the bootstrapping path and we rely on an external proxy (e.g Envoy) to enforce
   *   the SNI hostname is lkc-prefixed when connecting directly to the broker (i.e not bootstrapping).
   *
   * - strict: clusters that uses the v4 CCloud Network architecture
   *   Requires SNI for both bootstrap and broker path.
   *
   * @param userInfo user specific entry
   * @param username supplied user name
   * @param sniHostName supplied hostname via SNI, might be empty if it is not supplied
   * @param suppliedClusterId parsed cluster id, might be empty if it is not supplied
   * @param mode see SNIValidationMode class
   */
  private void verifyBrokerHostName(
          KeyConfigEntry userInfo, String username, Optional<SNIHostName> sniHostName, Optional<String> suppliedClusterId, SNIValidationMode mode) {
    switch (mode) {
      case OPTIONAL_VALIDATION:
        verifyBrokerHostNameOptional(userInfo, username, suppliedClusterId);
        break;
      case ALLOW_LEGACY_BOOTSTRAP:
        verifyBrokerHostNameLegacy(userInfo, username, sniHostName, suppliedClusterId);
        break;
      case STRICT:
        verifyBrokerHostNameStrict(userInfo, username, suppliedClusterId);
        break;
    }
  }

  private void verifyBrokerHostNameOptional(KeyConfigEntry userInfo, String username, Optional<String> clusterId) {
    if (clusterId.isPresent() && !clusterId.get().equals(userInfo.logicalClusterId)) {
      failSNIHostNameAuthentication(userInfo, username, clusterId);
    }
  }

  private void verifyBrokerHostNameLegacy(KeyConfigEntry userInfo, String username, Optional<SNIHostName> sniHostName, Optional<String> suppliedClusterId) {
    if (suppliedClusterId.isPresent()) {
      verifyBrokerHostNameStrict(userInfo, username, suppliedClusterId);
      return;
    }

    if (sniHostName.isPresent() && sniHostName.get().getAsciiName().startsWith("pkc-")) {
      return;
    }

    failSNIHostNameAuthentication(userInfo, username, suppliedClusterId);
  }

  private void verifyBrokerHostNameStrict(KeyConfigEntry userInfo, String username, Optional<String> sniHostName) {
    if (sniHostName.isPresent() && isBrokerHostNameMatched(userInfo, sniHostName.get())) {
      return;
    }
    failSNIHostNameAuthentication(userInfo, username, sniHostName);
  }

  private void failSNIHostNameAuthentication(KeyConfigEntry userInfo, String username, Optional<String> suppliedClusterId) {
    String errorMessage = String.format(
            "SNI cluster ID: %s does not match API key cluster ID %s for user name: %s",
            suppliedClusterId.orElse(NOT_PROVIDED_MSG), userInfo.logicalClusterId, username);
    AuthenticationErrorInfo errorInfo = logAndPrepareErrorInfo(UNAUTHENTICATED, username, userInfo.logicalClusterId, errorMessage);
    throw new SaslAuthenticationException(AUTHENTICATION_FAILED_MSG, errorInfo);
  }

  private boolean isBrokerHostNameMatched(KeyConfigEntry userInfo, String hostName) {
    return hostName.equals(userInfo.logicalClusterId);
  }

  private void authenticateBcrypt(KeyConfigEntry userInfo, String username, String password) {
    String hash;
    synchronized (BCRYPT_PASSWORD_CACHE) {
      hash = BCRYPT_PASSWORD_CACHE.get(password);
    }
    if (userInfo.hashedSecret.equals(hash)) {
      return;
    }
    if (!BCrypt.checkpw(password, userInfo.hashedSecret)) {
      String errorMessage = "Bad password for user " + username;
      AuthenticationErrorInfo errorInfo = logAndPrepareErrorInfo(UNAUTHENTICATED, username, userInfo.logicalClusterId, errorMessage);
      throw new SaslAuthenticationException(AUTHENTICATION_FAILED_MSG, errorInfo);
    }
    synchronized (BCRYPT_PASSWORD_CACHE) {
      BCRYPT_PASSWORD_CACHE.put(password, userInfo.hashedSecret);
    }
  }

  private AuthenticationErrorInfo logAndPrepareErrorInfo(
          final AuditEventStatus auditEventStatus, final String username,
          final String clusterId, final String errorMessage) {
    log.info(errorMessage);
    return new AuthenticationErrorInfo(auditEventStatus, errorMessage, username, clusterId);
  }

  /**
   * Extract the cluster ID from the provided host name, if possible.
   *
   * The host name should be of the form: lkc-1234-00aa-usw2-az1-x092.us-west-2.aws.glb.confluent.cloud.
   * It should start with `lkc-` and have another `-`. In all other cases, no cluster Id can be extracted.
   *
   * @param sniHostName supplied host name
   * @return extracted cluster ID, might be empty
   */
  // Visibility for testing
  static Optional<String> extractClusterIdFromHostName(final Optional<SNIHostName> sniHostName) {
    if (sniHostName.isPresent()) {
      String hostName = sniHostName.get().getAsciiName();
      if (hostName.startsWith(LKC_PREFIX)) {
        // hostName should looks like: `lkc-1234-00aa-usw2-az1-x092.us-west-2.aws.glb.confluent.cloud`
        // We want the split to end after the second `-`
        String[] parts = hostName.split("-", 3);
        if (parts.length > 2) {
          return Optional.of(parts[0] + "-" + parts[1]);
        }
      }
    }
    return Optional.empty();
  }

  // Visibility for testing
  static String configEntryOption(
          List<AppConfigurationEntry> configurationEntries, String key, String loginModuleName) {
    for (AppConfigurationEntry entry : configurationEntries) {
      if (loginModuleName != null && !loginModuleName.equals(entry.getLoginModuleName())) {
        continue;
      }
      Object val = entry.getOptions().get(key);
      if (val != null) {
        return (String) val;
      }
    }
    return null;
  }

  @Override
  public Optional<String> clusterId(final String username) throws SaslException {
    try {
      Map<String, KeyConfigEntry> passwords = loader.get();
      KeyConfigEntry entry = passwords.get(username);
      if (entry != null) {
        return Optional.of(entry.logicalClusterId);
      } else {
        return Optional.empty();
      }
    } catch (Exception e) {
      log.error("Unexpected exception during authentication for user {}", username, e);
      throw new SaslException("Authentication failed: Unexpected exception", e);
    }
  }

  private enum SNIValidationMode {
    OPTIONAL_VALIDATION("optional_validation"),
    ALLOW_LEGACY_BOOTSTRAP("allow_legacy_bootstrap"),
    STRICT("strict");

    private final String text;

    SNIValidationMode(String mode) {
      this.text = mode;
    }

    public static SNIValidationMode fromString(String text) {
      for (SNIValidationMode mode: SNIValidationMode.values()) {
        if (mode.text.equals(text)) {
          return mode;
        }
      }
      log.warn("Unknown SNI validation mode: " +  text + ". Set it to optional_validation");
      return OPTIONAL_VALIDATION;
    }

    public String getText() {
      return text;
    }
  }
}
