// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.test.utils;

import static org.junit.Assert.assertEquals;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;

import io.confluent.kafka.clients.plugins.auth.oauth.OAuthBearerLoginCallbackHandler;
import io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer;
import io.confluent.kafka.server.plugins.auth.oauth.OAuthBearerServerLoginCallbackHandler;
import io.confluent.kafka.server.plugins.auth.oauth.OAuthBearerValidatorCallbackHandler;
import io.confluent.kafka.server.plugins.auth.oauth.OAuthUtils;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import io.confluent.license.validator.ConfluentLicenseValidator;
import io.confluent.license.validator.ConfluentLicenseValidator.LicenseStatus;
import io.confluent.license.validator.LicenseValidator;
import java.io.File;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import javax.security.auth.login.Configuration;
import kafka.admin.ConfigCommand;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import kafka.server.KafkaServer;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.test.TestUtils;

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

  public static void waitForAclUpdate(org.apache.kafka.server.authorizer.Authorizer authorizer, KafkaPrincipal principal, Resource resource,
      Operation op, boolean deleted) {
    try {
      org.apache.kafka.test.TestUtils.waitForCondition(() -> {
        Iterable<AclBinding> acls = authorizer.acls(new AclBindingFilter(resource.toPattern().toFilter(),
            AccessControlEntryFilter.ANY));
        boolean matches = false;
        for (AclBinding acl : acls) {
          AccessControlEntry entry = acl.entry();
          if (entry.operation().equals(op.toJava()) && entry.principal()
              .equals(principal.toString())) {
            matches = true;
            break;
          }
        }
        return deleted != matches;
      }, "ACLs not updated");
    } catch (InterruptedException e) {
      throw new RuntimeException("Wait was interrupted", e);
    }
  }

  public static void verifyAuthorizerLicense(EmbeddedKafkaCluster kafkaCluster, LicenseStatus expectedStatus) {
    boolean needsLicense = expectedStatus != null;
    ConfluentServerAuthorizer authorizer = (ConfluentServerAuthorizer) kafkaCluster.brokers().get(0).authorizer().get();
    LicenseValidator licenseValidator = KafkaTestUtils.fieldValue(authorizer, ConfluentServerAuthorizer.class, "licenseValidator");
    assertEquals(needsLicense, licenseValidator instanceof ConfluentLicenseValidator);

    Map<String, Metric> metrics = Metrics.defaultRegistry().allMetrics().entrySet().stream()
        .filter(e -> e.getKey().getName().equals(ConfluentLicenseValidator.METRIC_NAME))
        .collect(Collectors.toMap(e -> e.getKey().getGroup(), Map.Entry::getValue));
    if (expectedStatus != null) {
      metrics.forEach((k, v) -> {
        assertEquals("Unexpected license metric for " + k,
            expectedStatus.name().toLowerCase(Locale.ROOT), ((Gauge<?>) v).value());
      });
    }
  }

  public static void attachServerOAuthConfigs(Map<String, Object> configs, List<String> serverMechanisms,
                                        String listenerPrefix, OAuthUtils.JwsContainer jwsContainer) {
    configs.put("sasl.enabled.mechanisms", serverMechanisms);
    configs.put(listenerPrefix + ".oauthbearer.sasl.login.callback.handler." +
                "class", OAuthBearerServerLoginCallbackHandler.class.getName());
    configs.put(listenerPrefix + ".oauthbearer.sasl.server.callback" +
                ".handler.class", OAuthBearerValidatorCallbackHandler.class.getName());
    configs.put(listenerPrefix + ".oauthbearer.sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "publicKeyPath=\"" +
                jwsContainer.getPublicKeyFile().toPath() +  "\";");
  }

  public static void attachMechanisms(Map<String, Object> saslClientConfigs,
                                      String clientMechanism,
                                      OAuthUtils.JwsContainer jwsContainer,
                                      String allowedCluster) {
    saslClientConfigs.put("sasl.mechanism", clientMechanism);
    saslClientConfigs.put("sasl.login.callback.handler.class",
                               OAuthBearerLoginCallbackHandler.class.getName());
    saslClientConfigs.put("sasl.jaas.config",
                          "org.apache.kafka.common.security." +
                          "oauthbearer.OAuthBearerLoginModule Required token=\"" +
                          jwsContainer.getJwsToken() + "\" cluster=\"" + allowedCluster + "\";");
  }
}
