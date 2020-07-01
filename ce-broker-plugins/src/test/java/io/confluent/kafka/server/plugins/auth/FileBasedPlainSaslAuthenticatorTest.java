// (Copyright) [2017 - 2017] Confluent, Inc.
package io.confluent.kafka.server.plugins.auth;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.server.plugins.auth.stats.AuthenticationStats;

import java.util.Optional;
import javax.net.ssl.SNIHostName;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.junit.Before;
import org.junit.Test;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class FileBasedPlainSaslAuthenticatorTest {
  private static final Logger log = LoggerFactory.getLogger(FileBasedPlainSaslAuthenticatorTest.class);
  public static final String USER_ID_1 = "23";
  public static final String TENANT_NAME_1 = "lkc-bkey";
  public static final String CLUSTER_ID_1 = "lkc-bkey";
  public static final String USERNAME_1 = "bkey";

  private final String bcryptPassword = "MKRWvhKV5Xd8VQ05JYre6f+aAq0UBXutZjsHWnQd/GYNR6DfqFeay+VNnReeTRpe";
  private List<AppConfigurationEntry> jaasEntries;
  private SaslAuthenticator saslAuth;
  private Map<String, Object> options;

  @Before
  public void setUp() throws Exception {
    options = new HashMap<>();
    options.put(FileBasedPlainSaslAuthenticator.JAAS_ENTRY_CONFIG, FileBasedPlainSaslAuthenticatorTest.class.getResource("/apikeys.json").getFile());
    options.put(FileBasedPlainSaslAuthenticator.JAAS_ENTRY_REFRESH_MS, "1000");
    setAuthenticatorValidationMode("optional_validation");
    AuthenticationStats.getInstance().reset();
  }

  private void setAuthenticatorValidationMode(String sniValidationMode) {
    options.put(FileBasedPlainSaslAuthenticator.SNI_HOST_NAME_VALIDATION_MODE, sniValidationMode);
    AppConfigurationEntry entry = new AppConfigurationEntry(
            FileBasedLoginModule.class.getName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
    jaasEntries = Collections.singletonList(entry);
    saslAuth = new FileBasedPlainSaslAuthenticator();
    saslAuth.initialize(jaasEntries);
  }

  @Test
  public void testHashedPasswordAuth() throws Exception {
    MultiTenantPrincipal principal = saslAuth.authenticate(USERNAME_1, bcryptPassword, Optional.empty());
    assertPrincipal(TENANT_NAME_1 + "_" + USER_ID_1, USER_ID_1, TENANT_NAME_1, CLUSTER_ID_1, true, principal);
  }

  @Test
  public void testPlainPasswordAuth() throws Exception {
    for (int i = 0; i < 3; i++) {
      MultiTenantPrincipal principal = saslAuth.authenticate("pkey", "no hash", Optional.empty());
      assertPrincipal("confluent_7", "7", "confluent", "confluent", true, principal);
    }
  }

  @Test
  public void testServiceAcoountAuth() throws Exception {
    for (int i = 0; i < 3; i++) {
      MultiTenantPrincipal principal = saslAuth.authenticate("skey", "service secret", Optional.empty());
      assertEquals("test_service_11", principal.getName());
      assertEquals("11", principal.user());
      assertEquals("test_service", principal.tenantMetadata().tenantName);
      assertEquals("test_service", principal.tenantMetadata().clusterId);
      assertFalse(principal.tenantMetadata().isSuperUser);
    }
  }

  @Test
  public void testInvalidUser() throws Exception {
    for (int i = 0; i < 3; i++) {
      try {
        saslAuth.authenticate("no_user", "blah", Optional.empty());
        fail("Invalid user name should fail the authentication");
      } catch (SaslAuthenticationException e) {
        //This message is returned to the client so it must not leak information
        assertEquals(FileBasedPlainSaslAuthenticator.AUTHENTICATION_FAILED_MSG, e.getMessage());
        assertEquals(AuditEventStatus.UNKNOWN_USER_DENIED, e.errorInfo().auditEventStatus());
        assertEquals("Unknown user no_user", e.errorInfo().errorMessage());
        assertEquals("no_user", e.errorInfo().identifier());
        assertEquals("", e.errorInfo().clusterId());
      }
    }
  }

  @Test
  public void testInvalidHashedPassword() throws Exception {
    for (int i = 0; i < 3; i++) {
      try {
        saslAuth.authenticate(USERNAME_1, "not right", Optional.empty());
        fail("Invalid hashed password should fail the authentication");
      } catch (SaslAuthenticationException e) {
        //This message is returned to the client so it must not leak information
        assertEquals(FileBasedPlainSaslAuthenticator.AUTHENTICATION_FAILED_MSG, e.getMessage());
        assertEquals(AuditEventStatus.UNAUTHENTICATED, e.errorInfo().auditEventStatus());
        assertEquals("Bad password for user bkey", e.errorInfo().errorMessage());
        assertEquals(USERNAME_1, e.errorInfo().identifier());
        assertEquals(CLUSTER_ID_1, e.errorInfo().clusterId());
      }
    }
  }

  @Test
  public void testInvalidPlainPassword() throws Exception {
    try {
      saslAuth.authenticate("pkey", "not right", Optional.empty());
      fail("Invalid plain password should fail the authentication");
    } catch (SaslAuthenticationException e) {
      //This message is returned to the client so it must not leak information
      assertEquals(FileBasedPlainSaslAuthenticator.AUTHENTICATION_FAILED_MSG, e.getMessage());
      assertEquals(AuditEventStatus.UNAUTHENTICATED, e.errorInfo().auditEventStatus());
      assertEquals("Bad password for user pkey", e.errorInfo().errorMessage());
      assertEquals("pkey", e.errorInfo().identifier());
      assertEquals("confluent", e.errorInfo().clusterId());
    }
  }

  @Test
  public void testCheckpwPerSecond() throws Exception {
    String configFilePath = FileBasedPlainSaslAuthenticator.configEntryOption(jaasEntries,
        FileBasedPlainSaslAuthenticator.JAAS_ENTRY_CONFIG,
        FileBasedLoginModule.class.getName());
    SecretsLoader loader = new SecretsLoader(configFilePath, 100000000);
    Map.Entry<String, KeyConfigEntry> entry = loader.get().entrySet().iterator().next();
    long calls = 0;
    long startMs = Time.SYSTEM.milliseconds();
    long endMs = startMs + 1000;
    long now;
    do {
      BCrypt.checkpw(entry.getValue().userId, entry.getValue().hashedSecret);
      calls++;
      now = Time.SYSTEM.milliseconds();
    } while (now < endMs);
    double duration = (endMs - startMs) / 1000.0;
    log.info("testCheckpwPerSecond: performed {} operations in {} seconds.  Average sec/op = {}",
        calls, duration, duration / calls);
  }

  @Test
  public void testServerFactory() throws SaslException {
    FileBasedSaslServerFactory factory = new FileBasedSaslServerFactory();
    PlainServerCallbackHandler cbh = new PlainServerCallbackHandler();
    Map<String, Object> emptyMap = Collections.<String, Object>emptyMap();
    cbh.configure(emptyMap, "PLAIN", jaasEntries);
    PlainSaslServer server = (PlainSaslServer) factory.createSaslServer("PLAIN", "", "", emptyMap, cbh);
    assertNotNull("Server not created", server);
  }

  @Test
  public void testPKCClusterIdShouldAuthenticateUserInLegacyMode() throws Exception {
    setAuthenticatorValidationMode("allow_legacy_bootstrap");
    MultiTenantPrincipal principal = saslAuth.authenticate(USERNAME_1, bcryptPassword, Optional.of(
            new SNIHostName("pkc-12345.wrong.host.name")));
    assertPrincipal(TENANT_NAME_1 + "_" + USER_ID_1, USER_ID_1, TENANT_NAME_1, CLUSTER_ID_1, true, principal);
  }

  @Test
  public void testIncorrectClusterIdShouldAuthenticateUserInOptionalMode() throws Exception {
    setAuthenticatorValidationMode("optional_validation");
    MultiTenantPrincipal principal = saslAuth.authenticate(USERNAME_1, bcryptPassword, Optional.of(
            new SNIHostName("wrong.host.name")));
    assertPrincipal(TENANT_NAME_1 + "_" + USER_ID_1, USER_ID_1, TENANT_NAME_1, CLUSTER_ID_1, true, principal);
  }

  @Test
  public void testIncorrectClusterIdShouldFailAuthenticationInStrictMode() throws Exception {
    setAuthenticatorValidationMode("strict");
    try {
      saslAuth.authenticate(USERNAME_1, bcryptPassword, Optional.of(new SNIHostName("lkc-wrong-00aa.host.name")));
      fail("Incorrect cluster Id should fail the authentication.");
    } catch (SaslAuthenticationException e) {
      //This message is returned to the client so it must not leak information
      assertEquals(FileBasedPlainSaslAuthenticator.AUTHENTICATION_FAILED_MSG, e.getMessage());
      assertEquals(AuditEventStatus.UNAUTHENTICATED, e.errorInfo().auditEventStatus());
      assertEquals(String.format(
              "SNI cluster ID: %s does not match API key cluster ID %s for user name: %s",
              "lkc-wrong", CLUSTER_ID_1, USERNAME_1), e.errorInfo().errorMessage());
      assertEquals(USERNAME_1, e.errorInfo().identifier());
      assertEquals(CLUSTER_ID_1, e.errorInfo().clusterId());
    }
  }

  @Test
  public void testCorrectClusterIdShouldAuthenticateUser() throws Exception {
    setAuthenticatorValidationMode("strict");
    MultiTenantPrincipal principal = saslAuth.authenticate(USERNAME_1, bcryptPassword, Optional.of(new SNIHostName(CLUSTER_ID_1 + "-0aa.rufus.confluent.cloud")));
    assertPrincipal(TENANT_NAME_1 + "_" + USER_ID_1, USER_ID_1, TENANT_NAME_1, CLUSTER_ID_1, true, principal);
  }

  @Test
  public void testExtractClusterIdFromHostName_shouldExtractClusterIdWhenHostNameIsWellFormed() {
    Optional<String> clusterId =  FileBasedPlainSaslAuthenticator.extractClusterIdFromHostName(
            Optional.of(new SNIHostName("lkc-1234-00aa-usw2-az1-x092.us-west-2.aws.glb.confluent.cloud")));
    assertEquals(Optional.of("lkc-1234"), clusterId);
  }

  @Test
  public void testExtractClusterIdFromHostName_shouldNotExtractClusterIdWhenHostNameIsNotLKCPrefixed() {
    Optional<String> clusterId =  FileBasedPlainSaslAuthenticator.extractClusterIdFromHostName(
            Optional.of(new SNIHostName("wrong.host.name")));
    assertEquals(Optional.empty(), clusterId);
  }

  @Test
  public void testExtractClusterIdFromHostName_shouldNotExtractClusterIdWhenHostNameDoesNotConfirmToTheFormat() {
    Optional<String> clusterId =  FileBasedPlainSaslAuthenticator.extractClusterIdFromHostName(
            Optional.of(new SNIHostName("lkc-1234.wrong.host.name")));
    assertEquals(Optional.empty(), clusterId);
  }

  private void assertPrincipal(
          String expectedUserName, String expectedUserId, String expectedTenantName, String expectedClusterId,
          boolean expectedIsSuperUser, MultiTenantPrincipal actualPrincipal) {
    assertEquals(expectedUserName, actualPrincipal.getName());
    assertEquals(expectedUserId, actualPrincipal.user());
    assertEquals(expectedTenantName, actualPrincipal.tenantMetadata().tenantName);
    assertEquals(expectedClusterId, actualPrincipal.tenantMetadata().clusterId);
    assertEquals(expectedIsSuperUser, actualPrincipal.tenantMetadata().isSuperUser);
  }

}
