// (Copyright) [2017 - 2017] Confluent, Inc.
package io.confluent.kafka.server.plugins.auth;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.server.plugins.auth.stats.AuthenticationStats;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileBasedPlainSaslAuthenticatorTest {
  private static final Logger log = LoggerFactory.getLogger(FileBasedPlainSaslAuthenticatorTest.class);
  private final String bcryptPassword = "MKRWvhKV5Xd8VQ05JYre6f+aAq0UBXutZjsHWnQd/GYNR6DfqFeay+VNnReeTRpe";
  private List<AppConfigurationEntry> jaasEntries;
  private SaslAuthenticator saslAuth;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    final String path = FileBasedPlainSaslAuthenticatorTest.class.getResource("/apikeys.json").getFile();

    Map<String, Object> options = new HashMap<>();
    options.put(FileBasedPlainSaslAuthenticator.JAAS_ENTRY_CONFIG, path);
    options.put(FileBasedPlainSaslAuthenticator.JAAS_ENTRY_REFRESH_MS, "1000");
    AppConfigurationEntry entry = new AppConfigurationEntry(FileBasedLoginModule.class.getName(),
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options);
    jaasEntries = Collections.singletonList(entry);
    saslAuth = new FileBasedPlainSaslAuthenticator();
    saslAuth.initialize(jaasEntries);
    AuthenticationStats.getInstance().reset();
  }

  @Test
  public void testHashedPasswordAuth() throws Exception {
    MultiTenantPrincipal principal = saslAuth.authenticate("bkey", bcryptPassword);
    assertEquals("rufus_23", principal.getName());
    assertEquals("23", principal.user());
    assertEquals("rufus", principal.tenantMetadata().tenantName);
    assertEquals("rufus", principal.tenantMetadata().clusterId);
    assertTrue(principal.tenantMetadata().isSuperUser);
  }

  @Test
  public void testPlainPasswordAuth() throws Exception {
    for (int i = 0; i < 3; i++) {
      MultiTenantPrincipal principal = saslAuth.authenticate("pkey", "no hash");
      assertEquals("confluent_7", principal.getName());
      assertEquals("7", principal.user());
      assertEquals("confluent", principal.tenantMetadata().tenantName);
      assertEquals("confluent", principal.tenantMetadata().clusterId);
      assertTrue(principal.tenantMetadata().isSuperUser);
    }
  }

  @Test
  public void testServiceAcoountAuth() throws Exception {
    for (int i = 0; i < 3; i++) {
      MultiTenantPrincipal principal = saslAuth.authenticate("skey", "service secret");
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
        saslAuth.authenticate("no_user", "blah");
        fail();
      } catch (SaslAuthenticationException e) {
        //This message is returned to the client so it must not leak information
        assertEquals("Authentication failed: Invalid username or password", e.getMessage());
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
        saslAuth.authenticate("bkey", "not right");
        fail();
      } catch (SaslAuthenticationException e) {
        //This message is returned to the client so it must not leak information
        assertEquals("Authentication failed: Invalid username or password", e.getMessage());
        assertEquals(AuditEventStatus.UNAUTHENTICATED, e.errorInfo().auditEventStatus());
        assertEquals("Bad password for user bkey", e.errorInfo().errorMessage());
        assertEquals("bkey", e.errorInfo().identifier());
        assertEquals("rufus", e.errorInfo().clusterId());
      }
    }
  }

  @Test
  public void testInvalidPlainPassword() throws Exception {
    try {
      saslAuth.authenticate("pkey", "not right");
      fail();
    } catch (SaslAuthenticationException e) {
      //This message is returned to the client so it must not leak information
      assertEquals("Authentication failed: Invalid username or password", e.getMessage());
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
}
