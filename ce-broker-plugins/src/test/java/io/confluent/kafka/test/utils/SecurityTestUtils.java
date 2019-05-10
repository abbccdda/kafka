// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.test.utils;

import static org.junit.Assert.assertEquals;

import io.confluent.common.license.LicenseValidator;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import io.confluent.license.validator.ConfluentLicenseValidator;
import io.confluent.license.validator.ConfluentLicenseValidator.LicenseStatus;
import io.confluent.security.authorizer.EmbeddedAuthorizer;
import java.io.File;
import java.util.Set;
import javax.security.auth.login.Configuration;
import kafka.admin.ConfigCommand;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import kafka.server.KafkaServer;
import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import scala.collection.JavaConversions;

public class SecurityTestUtils {

  public static String createScramUser(EmbeddedKafkaCluster kafkaCluster, String userName, String password) {
    String mechanism = ScramMechanism.SCRAM_SHA_256.mechanismName();
    String credentials = String.format("%s=[iterations=4096,password=%s]", mechanism, password);
    String[] args = {
        "--zookeeper", kafkaCluster.zkConnect(),
        "--alter", "--add-config", credentials,
        "--entity-type", "users",
        "--entity-name", userName
    };
    ConfigCommand.main(args);

    for (KafkaServer server : kafkaCluster.brokers()) {
      CredentialCache.Cache<ScramCredential> cache = server.credentialProvider().credentialCache()
          .cache(mechanism, ScramCredential.class);
      try {
        TestUtils.waitForCondition(() -> cache.get(userName) != null,
            "SCRAM credentials not create for user " + userName);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return password;
  }

  public static String scramSaslJaasConfig(String username, String password) {
    return "org.apache.kafka.common.security.scram.ScramLoginModule required\n"
        + "username=\"" + username + "\"\n"
        + "password=\"" + password + "\";\n";
  }

  public static String gssapiSaslJaasConfig(File keytabFile, String principal, String serviceName) {
    StringBuilder builder = new StringBuilder();
    builder.append("com.sun.security.auth.module.Krb5LoginModule required\n");
    builder.append("debug=true\n");
    if (serviceName != null) {
      builder.append("serviceName=\"");
      builder.append(serviceName);
      builder.append("\"\n");
    }
    builder.append("keyTab=\"" + keytabFile.getAbsolutePath() + "\"\n");
    builder.append("principal=\"");
    builder.append(principal);
    builder.append("\"\n");
    builder.append("storeKey=\"true\"\n");
    builder.append("useKeyTab=\"true\";\n");
    return builder.toString();
  }

  public static void clearSecurityConfigs() {
    System.getProperties().stringPropertyNames().stream()
        .filter(name -> name.startsWith("java.security.krb5"))
        .forEach(System::clearProperty);
    LoginManager.closeAll();
    Configuration.setConfiguration(null);
  }

  public static String[] clusterAclArgs(String zkConnect, KafkaPrincipal principal, String op) {
    return new String[] {
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--add",
        "--cluster",
        "--operation=" + op,
        "--allow-principal=" + principal
    };
  }

  public static String[] topicBrokerReadAclArgs(String zkConnect, KafkaPrincipal principal) {
    return new String[]{
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--add",
        "--topic=*",
        "--operation=Read",
        "--allow-principal=" + principal
    };
  }

  public static String[] produceAclArgs(String zkConnect, KafkaPrincipal principal, String topic,
      PatternType patternType) {
    return new String[]{
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--add",
        "--resource-pattern-type=" + patternType.name(),
        "--topic=" + topic,
        "--producer",
        "--allow-principal=" + principal
    };
  }

  public static String[] consumeAclArgs(String zkConnect, KafkaPrincipal principal,
      String topic, String consumerGroup, PatternType patternType) {
    return new String[]{
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--add",
        "--resource-pattern-type=" + patternType.name(),
        "--topic=" + topic,
        "--group=" + consumerGroup,
        "--consumer",
        "--allow-principal=" + principal
    };
  }

  public static String[] addConsumerGroupAclArgs(String zkConnect, KafkaPrincipal principal,
      String consumerGroup, Operation op, PatternType patternType) {
    return new String[]{
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--add",
        "--resource-pattern-type=" + patternType.name(),
        "--group=" + consumerGroup,
        "--operation=" + op.name(),
        "--allow-principal=" + principal
    };
  }

  public static String[] addTopicAclArgs(String zkConnect, KafkaPrincipal principal,
      String topic, Operation op, PatternType patternType) {
    return new String[]{
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--add",
        "--resource-pattern-type=" + patternType.name(),
        "--topic=" + topic,
        "--operation=" + op.name(),
        "--allow-principal=" + principal
    };
  }

  public static String[] deleteTopicAclArgs(String zkConnect, KafkaPrincipal principal,
      String topic, String op) {
    return new String[]{
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--remove",
        "--force",
        "--topic=" + topic,
        "--operation=" + op,
        "--allow-principal=" + principal
    };
  }

  public static void waitForAclUpdate(Authorizer authorizer, KafkaPrincipal principal, Resource resource,
      Operation op, boolean deleted) {
    try {
      org.apache.kafka.test.TestUtils.waitForCondition(() -> {
        Set<Acl> acls = JavaConversions.setAsJavaSet(authorizer.getAcls(resource));
        boolean matches = acls.stream().anyMatch(acl -> acl.operation().equals(op) && acl.principal().equals(principal));
        return deleted != matches;
      }, "ACLs not updated");
    } catch (InterruptedException e) {
      throw new RuntimeException("Wait was interrupted", e);
    }
  }

  public static void verifyAuthorizerLicense(EmbeddedKafkaCluster kafkaCluster, LicenseStatus expectedStatus) {
    boolean needsLicense = expectedStatus != null;
    EmbeddedAuthorizer authorizer = (EmbeddedAuthorizer) kafkaCluster.brokers().get(0).authorizer().get();
    LicenseValidator licenseValidator = KafkaTestUtils.fieldValue(authorizer, EmbeddedAuthorizer.class, "licenseValidator");
    assertEquals(needsLicense, licenseValidator instanceof ConfluentLicenseValidator);

    Time time = Time.SYSTEM;
    ZooKeeperClient zooClient = new ZooKeeperClient(kafkaCluster.zkConnect(), 10000, 1000,
        Integer.MAX_VALUE, time, "testMetricGroup", "testMetricType");
    try {
      KafkaZkClient zkClient = new KafkaZkClient(zooClient, false, time);
      assertEquals(expectedStatus == LicenseStatus.TRIAL, zkClient.pathExists("/confluent-license/trial"));
    } finally {
      zooClient.close();
    }
  }
}
