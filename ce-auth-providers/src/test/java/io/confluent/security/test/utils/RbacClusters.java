// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.test.utils;

import static org.junit.Assert.assertTrue;

import io.confluent.kafka.security.authorizer.ConfluentKafkaAuthorizer;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.KafkaTestUtils.ClientBuilder;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.auth.metadata.MetadataServiceConfig;
import io.confluent.security.auth.provider.rbac.RbacProvider;
import io.confluent.security.auth.store.data.UserKey;
import io.confluent.security.auth.store.data.UserValue;
import io.confluent.security.auth.store.kafka.KafkaAuthWriter;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.minikdc.MiniKdcWithLdapService;
import io.confluent.security.store.kafka.KafkaStoreConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import kafka.admin.AclCommand;
import kafka.security.auth.Alter$;
import kafka.security.auth.ClusterAction$;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

public class RbacClusters {

  private final Config config;
  private final SecurityProtocol kafkaSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT;
  private final String kafkaSaslMechanism = "SCRAM-SHA-256";
  private final EmbeddedKafkaCluster metadataCluster;
  public final EmbeddedKafkaCluster kafkaCluster;
  private final Map<String, User> users;
  public final MiniKdcWithLdapService miniKdcWithLdapService;
  private final KafkaAuthWriter authWriter;

  public RbacClusters(Config config) throws Exception {
    this.config = config;
    metadataCluster = new EmbeddedKafkaCluster();
    metadataCluster.startZooKeeper();
    users = createUsers(metadataCluster, config.brokerUser, config.userNames);

    if (config.enableLdap)
      miniKdcWithLdapService = LdapTestUtils.createMiniKdcWithLdapService(null, null);
    else
      miniKdcWithLdapService = null;

    // In order to start metadata service without fixed ports, start one broker
    // without metadata service and a second one with metadata service using the
    // first broker as bootstrap server for the metadata service
    metadataCluster.startBrokers(1, metadataClusterServerConfig());
    metadataCluster.startBrokers(1, metadataClusterServerConfig());
    KafkaServer metadataBroker = metadataCluster.brokers().get(1);

    ConfluentKafkaAuthorizer authorizer = (ConfluentKafkaAuthorizer) metadataBroker.authorizer().get();
    RbacProvider rbacProvider = (RbacProvider) authorizer.metadataProvider();
    TestUtils.waitForCondition(() -> rbacProvider.authStore() != null,
        "Metadata server not created");
    TestUtils.waitForCondition(() -> rbacProvider.authStore().writer() != null,
        30000, "Metadata writer not started");
    assertTrue(rbacProvider.authStore().writer() instanceof KafkaAuthWriter);
    this.authWriter = (KafkaAuthWriter) rbacProvider.authStore().writer();

    kafkaCluster = new EmbeddedKafkaCluster();
    kafkaCluster.startZooKeeper();
    createUsers(kafkaCluster, config.brokerUser, config.userNames);
    kafkaCluster.startBrokers(1, serverConfig());
  }

  public String kafkaClusterId() {
    return kafkaCluster.kafkas().get(0).kafkaServer().clusterId();
  }

  public void produceConsume(String user,
      String topic,
      String consumerGroup,
      boolean authorized) throws Throwable {
    ClientBuilder clientBuilder = clientBuilder(user);
    KafkaTestUtils.verifyProduceConsume(clientBuilder, topic, consumerGroup, authorized);
  }

  public ClientBuilder clientBuilder(String user) {
    return new ClientBuilder(kafkaCluster.bootstrapServers(),
        kafkaSecurityProtocol,
        kafkaSaslMechanism,
        users.get(user).jaasConfig);
  }

  public void assignRole(String principalType,
                         String userName,
                         String role,
                         String clusterId,
                         Set<ResourcePattern> resources) throws Exception {
    KafkaPrincipal principal = new KafkaPrincipal(principalType, userName);
    authWriter.replaceResourceRoleBinding(
        principal,
        role,
        Scope.kafkaClusterScope(clusterId),
        resources).toCompletableFuture().get(30, TimeUnit.SECONDS);
  }

  public void updateUserGroup(String userName, String group) throws Exception {
    if (config.enableLdapGroups) {
      miniKdcWithLdapService.createGroup(group, userName);
    } else {
      // Write groups directly to auth topic
      KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, userName);
      Set<KafkaPrincipal> groupPrincipals =
          Utils.mkSet(new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, group));
      UserKey key = new UserKey(principal);
      UserValue value = new UserValue(groupPrincipals);
      authWriter.writeExternalEntry(key, value, 1);
    }
  }

  public void shutdown() {
    try {
      kafkaCluster.shutdown();
      if (miniKdcWithLdapService != null)
        miniKdcWithLdapService.shutdown();
    } finally {
      metadataCluster.shutdown();
    }
  }

  public void waitUntilAccessAllowed(String user, String topic) throws Exception {
    waitUntilAccessUpdated(user, topic, true);
  }

  public void waitUntilAccessDenied(String user, String topic) throws Exception {
    waitUntilAccessUpdated(user, topic, false);
  }

  private void waitUntilAccessUpdated(String user, String topic, boolean allowed) throws Exception {
    try (AdminClient adminClient = KafkaTestUtils.createAdminClient(
        kafkaCluster.bootstrapServers(),
        kafkaSecurityProtocol,
        kafkaSaslMechanism,
        users.get(user).jaasConfig)) {
      TestUtils.waitForCondition(() -> allowed == KafkaTestUtils.canAccess(adminClient, topic),
          "Access not updated in time to " + (allowed ? "ALLOWED" : "DENIED"));
    }
  }

  private Properties scramConfigs() {
    Properties props = new Properties();
    props.setProperty(KafkaConfig$.MODULE$.ListenersProp(),
        "EXTERNAL://localhost:0,INTERNAL://localhost:0");
    props.setProperty(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(),
        "INTERNAL");
    props.setProperty(KafkaConfig$.MODULE$.ListenerSecurityProtocolMapProp(),
        "EXTERNAL:SASL_PLAINTEXT,INTERNAL:SASL_PLAINTEXT");
    props.setProperty(KafkaConfig$.MODULE$.SaslEnabledMechanismsProp(),
        kafkaSaslMechanism);
    props.setProperty(KafkaConfig$.MODULE$.SaslMechanismInterBrokerProtocolProp(),
        kafkaSaslMechanism);
    props.setProperty(
        "listener.name.external.scram-sha-256." + KafkaConfig$.MODULE$.SaslJaasConfigProp(),
        users.get(config.brokerUser).jaasConfig);
    props.setProperty(
        "listener.name.internal.scram-sha-256." + KafkaConfig$.MODULE$.SaslJaasConfigProp(),
        users.get(config.brokerUser).jaasConfig);

    props.setProperty(KafkaStoreConfig.PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        metadataCluster.bootstrapServers("INTERNAL"));
    props.setProperty(KafkaStoreConfig.PREFIX + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        "SASL_PLAINTEXT");
    props.setProperty(KafkaStoreConfig.PREFIX + SaslConfigs.SASL_MECHANISM,
        kafkaSaslMechanism);
    props.setProperty(KafkaStoreConfig.PREFIX + SaslConfigs.SASL_JAAS_CONFIG,
        users.get(config.brokerUser).jaasConfig);
    return props;
  }


  private Properties serverConfig() {
    Properties serverConfig = new Properties();
    serverConfig.putAll(scramConfigs());

    serverConfig.setProperty(KafkaConfig$.MODULE$.AuthorizerClassNameProp(),
        ConfluentKafkaAuthorizer.class.getName());
    serverConfig.setProperty("super.users", "User:" + config.brokerUser);
    serverConfig.setProperty(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ACL,RBAC");
    serverConfig.setProperty(ConfluentAuthorizerConfig.GROUP_PROVIDER_PROP, "RBAC");

    return serverConfig;
  }

  private Properties metadataClusterServerConfig() {
    Properties serverConfig = new Properties();
    serverConfig.putAll(scramConfigs());
    int existingBrokerCount = metadataCluster.brokers().size();
    serverConfig.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), String.valueOf(100 + existingBrokerCount));

    if (existingBrokerCount != 0) {
      if (config.enableLdap) {
        serverConfig.putAll(LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService, 10));
      }
      serverConfig.setProperty(KafkaConfig$.MODULE$.AuthorizerClassNameProp(),
          ConfluentKafkaAuthorizer.class.getName());
      serverConfig.setProperty(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ACL");
      serverConfig.setProperty("super.users", "User:" + config.brokerUser);
      serverConfig.setProperty(ConfluentAuthorizerConfig.METADATA_PROVIDER_PROP, "RBAC");
      serverConfig.setProperty(MetadataServiceConfig.METADATA_SERVER_LISTENERS_PROP,
          "http://0.0.0.0:8000");
      serverConfig.setProperty(MetadataServiceConfig.METADATA_SERVER_ADVERTISED_LISTENERS_PROP,
          "http://localhost:8000");
      serverConfig.setProperty(KafkaStoreConfig.NUM_PARTITIONS_PROP, "2");
      serverConfig.setProperty(KafkaStoreConfig.REPLICATION_FACTOR_PROP, "1");
      serverConfig.setProperty(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), "false");
      serverConfig.putAll(config.metadataClusterPropOverrides);
    }
    return serverConfig;
  }

  private Map<String, User> createUsers(EmbeddedKafkaCluster cluster,
                                        String brokerUser,
                                        List<String> otherUsers) {
    Map<String, User> users = new HashMap<>(otherUsers.size() + 1);
    users.put(brokerUser, User.createScramUser(cluster, brokerUser));
    otherUsers.stream()
        .map(name -> User.createScramUser(cluster, name))
        .forEach(user -> users.put(user.name, user));

    String zkConnect = cluster.zkConnect();
    KafkaPrincipal brokerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, brokerUser);
    AclCommand.main(SecurityTestUtils.clusterAclArgs(zkConnect, brokerPrincipal, ClusterAction$.MODULE$.name()));
    AclCommand.main(SecurityTestUtils.clusterAclArgs(zkConnect, brokerPrincipal, Alter$.MODULE$.name()));
    AclCommand.main(SecurityTestUtils.topicBrokerReadAclArgs(zkConnect, brokerPrincipal));
    return users;
  }

  public static class Config {
    private final Properties metadataClusterPropOverrides = new Properties();
    private String brokerUser;
    private List<String> userNames;
    private boolean enableLdap;
    private boolean enableLdapGroups;

    public Config users(String brokerUser, List<String> userNames) {
      this.brokerUser = brokerUser;
      this.userNames = userNames;
      return this;
    }

    public Config withLdapGroups() {
      this.enableLdapGroups = true;
      this.enableLdap = true;
      return this;
    }

    public Config withLdap() {
      this.enableLdap = true;
      return this;
    }

    public Config overrideMetadataBrokerConfig(String name, String value) {
      metadataClusterPropOverrides.setProperty(name, value);
      return this;
    }
  }
}
