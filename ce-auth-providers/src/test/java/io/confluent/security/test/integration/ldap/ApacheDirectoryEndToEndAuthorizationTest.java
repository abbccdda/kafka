// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.test.integration.ldap;

import io.confluent.kafka.security.ldap.authorizer.LdapAuthorizer;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.license.validator.ConfluentLicenseValidator.LicenseStatus;
import io.confluent.security.auth.provider.ldap.LdapConfig;
import io.confluent.security.auth.provider.ldap.LdapGroupManager;
import io.confluent.security.minikdc.MiniKdcWithLdapService;
import io.confluent.security.minikdc.MiniKdcWithLdapService.LdapSecurityAuthentication;
import io.confluent.security.minikdc.MiniKdcWithLdapService.LdapSecurityProtocol;
import io.confluent.security.test.utils.LdapTestUtils;
import io.confluent.security.test.utils.User;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import java.util.Set;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class ApacheDirectoryEndToEndAuthorizationTest extends AbstractEndToEndAuthorizationTest {

  private MiniKdcWithLdapService miniKdcWithLdapService;

  public ApacheDirectoryEndToEndAuthorizationTest(String kafkaSaslMechanism,
      LdapSecurityProtocol ldapSecurityProtocol,
      LdapSecurityAuthentication ldapSecurityAuthentication,
      String ldapUser,
      int ldapRefreshIntervalMs) {
    super(kafkaSaslMechanism, ldapSecurityProtocol, ldapSecurityAuthentication,
        ldapUser, ldapRefreshIntervalMs);

  }

  @Before
  public void setUp() throws Throwable {
    createLdapServer();
    super.setUp();
    maybeWaitForUserGroups();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    miniKdcWithLdapService.shutdown();
  }

  /**
   * Due a to timing issue in Apache DS persistent search (https://issues.apache.org/jira/browse/DIRSERVER-2257),
   * some updates made while the persistent search is initialized may not be returned by the search. Use a small
   * read timeout to wait for LDAP initialization using {@link #maybeWaitForUserGroups} before executing tests.
   */
  @Override
  protected Properties kafkaServerConfig() throws Exception {
    Properties serverConfig = super.kafkaServerConfig();
    if (ldapRefreshIntervalMs == LdapConfig.PERSISTENT_REFRESH)
      serverConfig.put(LdapConfig.JNDI_READ_TIMEOUT_MS_PROP, "5000");
    return serverConfig;
  }

  // When using persistent search, wait until the search is initialized and existing
  // groups are loaded into cache. This can be removed once DIRSERVER-2257 is fixed.
  private void maybeWaitForUserGroups() {
    if (ldapRefreshIntervalMs != LdapConfig.PERSISTENT_REFRESH)
      return;

    kafkaCluster.brokers().forEach(broker -> {
      LdapAuthorizer authorizer = (LdapAuthorizer) broker.authorizer().get();
      LdapGroupManager ldapGroupManager = LdapTestUtils.ldapGroupManager(authorizer);
      waitForUserGroups(ldapGroupManager, KAFKA_SERVICE, ADMIN_GROUP);
      waitForUserGroups(ldapGroupManager, DEVELOPER, DEV_GROUP);
      waitForUserGroups(ldapGroupManager, SRE, DEV_GROUP, TEST_GROUP);
      waitForUserGroups(ldapGroupManager, TESTER, TEST_GROUP);
    });
  }

  private void waitForUserGroups(LdapGroupManager groupManager, String user, String... userGroups) {
    try {
      Set<String> groups = Utils.mkSet(userGroups);
      TestUtils.waitForCondition(
          () -> groups.equals(groupManager.groups(user)),
          "Group not updated for user " + user);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGroupChanges() throws Throwable {
    // Add group ACLs for dev and test topics and consumer groups
    addAcls(groupPrincipal(DEV_GROUP), DEV_TOPIC, DEV_CONSUMER_GROUP);
    addAcls(groupPrincipal(TEST_GROUP), TEST_TOPIC, TEST_CONSUMER_GROUP);

    // Add tester to dev group and verify that new group's ACLs are applied
    KafkaProducer<String, String> producer = createProducer(users.get(TESTER));
    miniKdcWithLdapService.addUserToGroup(DEV_GROUP, TESTER);
    TestUtils.waitForCondition(() -> {
      try {
        producer.partitionsFor(DEV_TOPIC);
        return true;
      } catch (AuthorizationException e) {
        return false;
      }
    }, "Groups not refreshed");
    produceConsume(users.get(TESTER), DEV_TOPIC, DEV_CONSUMER_GROUP, true);

    SecurityTestUtils.verifyConfluentLicense(kafkaCluster, LicenseStatus.LICENSE_ACTIVE);
  }

  private void createLdapServer() throws Exception {
    MiniKdcWithLdapService ldapServer =
        LdapTestUtils.createMiniKdcWithLdapService(ldapSecurityProtocol, ldapSecurityAuthentication);
    ldapServer.createGroup(ADMIN_GROUP, KAFKA_SERVICE);
    ldapServer.createGroup(DEV_GROUP, DEVELOPER, SRE);
    ldapServer.createGroup(TEST_GROUP, TESTER, SRE);
    miniKdcWithLdapService = ldapServer;
  }

  @Override
  protected Properties authorizerConfig() {
    Properties props = new Properties();
    props.putAll(LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService,
        ldapRefreshIntervalMs));
    return props;
  }

  @Override
  protected User createGssapiUser(String name, String principal) {
    File keytabFile = LdapTestUtils.createPrincipal(miniKdcWithLdapService, principal);
    String serviceName = KAFKA_SERVICE.equals(ldapUser) ? null : KAFKA_SERVICE;
    return User.gssapiUser(name, principal + "@EXAMPLE.COM", keytabFile, serviceName);
  }

  @Parameterized.Parameters(name = "saslMechanism={0}, secProtocol={1}, secAuth={2},"
          + "secUser={3}, refreshInterval={4}")
  public static Collection<Object[]> data() {
    List<Object[]> values = new ArrayList<>();
    values.add(new Object[]{
        "SCRAM-SHA-256",
        LdapSecurityProtocol.PLAINTEXT,
        LdapSecurityAuthentication.NONE,
        null,
        10
    });
    values.add(new Object[]{
        "SCRAM-SHA-256",
        LdapSecurityProtocol.SSL,
        LdapSecurityAuthentication.GSSAPI,
        LDAP_USER,
        LdapConfig.PERSISTENT_REFRESH
    });
    values.add(new Object[]{
        "GSSAPI",
        LdapSecurityProtocol.PLAINTEXT,
        LdapSecurityAuthentication.NONE,
        null,
        10
    });
    values.add(new Object[]{
        "GSSAPI",
        LdapSecurityProtocol.PLAINTEXT,
        LdapSecurityAuthentication.GSSAPI,
        LDAP_USER,
        LdapConfig.PERSISTENT_REFRESH
    });
    values.add(new Object[]{
        "GSSAPI",
        LdapSecurityProtocol.PLAINTEXT,
        LdapSecurityAuthentication.GSSAPI,
        KAFKA_SERVICE,
        10
    });
    return values;
  }
}

