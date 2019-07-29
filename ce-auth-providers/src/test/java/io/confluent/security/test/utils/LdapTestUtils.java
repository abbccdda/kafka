// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.test.utils;

import io.confluent.kafka.security.ldap.authorizer.LdapAuthorizer;
import io.confluent.license.test.utils.LicenseTestUtils;
import io.confluent.security.auth.provider.ldap.LdapConfig;
import io.confluent.security.auth.provider.ldap.LdapGroupManager;
import io.confluent.security.auth.provider.ldap.LdapGroupProvider;
import io.confluent.security.minikdc.MiniKdcWithLdapService;
import io.confluent.security.minikdc.MiniKdcWithLdapService.LdapSecurityAuthentication;
import io.confluent.security.minikdc.MiniKdcWithLdapService.LdapSecurityProtocol;

import io.confluent.kafka.test.utils.SecurityTestUtils;
import java.util.HashMap;
import kafka.server.KafkaConfig$;
import kafka.utils.TestUtils;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import scala.Some;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;

public class LdapTestUtils {

  public static MiniKdcWithLdapService createMiniKdcWithLdapService(
      LdapSecurityProtocol securityProtocol,
      LdapSecurityAuthentication authentication) throws Exception {
    File workDir = TestUtils.tempDir();
    Properties ldapConf = MiniKdcWithLdapService.createConfig();
    if (securityProtocol == LdapSecurityProtocol.SSL) {
      File ldapTrustStore = File.createTempFile("truststore", ".jks");
      Properties ldapSslProps = TestUtils.sslConfigs(Mode.SERVER, false, new Some<>(ldapTrustStore), "ldap", "localhost");
      MiniKdcWithLdapService.addSslConfig(ldapConf, ldapSslProps);
    }

    File keytabFile = null;
    if (authentication == LdapSecurityAuthentication.GSSAPI) {
      keytabFile = TestUtils.tempFile();
      String gssapiConfig = SecurityTestUtils.gssapiSaslJaasConfig(keytabFile, "ldap/localhost@EXAMPLE.COM", null);
      MiniKdcWithLdapService.addGssapiConfig(ldapConf, gssapiConfig);
    }

    MiniKdcWithLdapService ldapServer = new MiniKdcWithLdapService(ldapConf, workDir);
    ldapServer.start();

    if (keytabFile != null) {
      ldapServer.createPrincipal(keytabFile, "ldap/localhost");
    }
    return ldapServer;
  }

  public static void restartLdapServer(MiniKdcWithLdapService server) throws Exception {
    int port = server.ldapPort();

    long endTimeMs = System.currentTimeMillis() + 20000;
    while (System.currentTimeMillis() < endTimeMs) {
      try {
        server.startLdap(port);
        return;
      } catch (Exception e) {
        server.stopLdap();
      }
    }
    throw new IllegalStateException("LDAP server could not be restarted");
  }

  public static LdapGroupManager createLdapGroupManager(MiniKdcWithLdapService ldapServer,
      int refreshIntervalMs, Time time) {
    Map<String, Object> props = ldapAuthorizerConfigs(ldapServer, refreshIntervalMs);
    LdapConfig ldapConfig = new LdapConfig(props);
    return new LdapGroupManager(ldapConfig, time);
  }

  public static Map<String, Object> ldapAuthorizerConfigs(MiniKdcWithLdapService ldapServer, int refreshIntervalMs) {
    Map<String, Object> props = new HashMap<>();
    props.put(KafkaConfig$.MODULE$.AuthorizerClassNameProp(), LdapAuthorizer.class.getName());
    props.put(LdapConfig.REFRESH_INTERVAL_MS_PROP, String.valueOf(refreshIntervalMs));
    props.put(LdapConfig.GROUP_NAME_ATTRIBUTE_PROP, "cn");
    props.put(LdapConfig.GROUP_MEMBER_ATTRIBUTE_PATTERN_PROP, "uid=(.*),ou=users,dc=example,dc=com");
    for (Map.Entry<String, String> entry : ldapServer.ldapClientConfigs().entrySet()) {
      props.put(LdapConfig.CONFIG_PREFIX + entry.getKey(), entry.getValue());
    }
    props.put(LdapAuthorizer.LICENSE_PROP, LicenseTestUtils.generateLicense());

    // Due a to timing issue in Apache DS persistent search (https://issues.apache.org/jira/browse/DIRSERVER-2257),
    // some updates made while the persistent search is initialized may not be returned by the search. Use a read
    // timeout that is high enough to avoid unnecessary timeouts in tests, but low enough to trigger a second search
    // in cases where the timing window resulted in missing entries.
    if (refreshIntervalMs == LdapConfig.PERSISTENT_REFRESH) {
      props.put(LdapConfig.CONFIG_PREFIX + "com.sun.jndi.ldap.read.timeout", "5000");
    }

    return props;
  }

  public static LdapGroupManager ldapGroupManager(LdapAuthorizer authorizer) {
    LdapGroupProvider groupProvider = (LdapGroupProvider) authorizer.groupProvider();
    assertNotNull("Group provider not configured", groupProvider);
    return groupProvider.ldapGroupManager();
  }

  public static void waitForUserGroups(LdapAuthorizer authorizer,
      String user, String... groups) throws Exception {
    waitForUserGroups(ldapGroupManager(authorizer), user, groups);
  }

  public static void waitForUserGroups(LdapGroupManager ldapGroupManager,
      String user, String... groups) throws Exception {
    org.apache.kafka.test.TestUtils.waitForCondition(() ->
        Utils.mkSet(groups).equals(ldapGroupManager.groups(user)), "Groups not refreshed for user " + user);
  }

  public static File createPrincipal(MiniKdcWithLdapService miniKdcWithLdapService, String principal) {
    try {
      File keytabFile = TestUtils.tempFile();
      miniKdcWithLdapService.createPrincipal(keytabFile, principal);
      return keytabFile;
    } catch (Exception e) {
      throw new RuntimeException("Could not create keytab", e);
    }
  }
}
