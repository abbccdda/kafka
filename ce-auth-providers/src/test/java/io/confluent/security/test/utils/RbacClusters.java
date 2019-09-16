// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.test.utils;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.KafkaTestUtils.ClientBuilder;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.license.test.utils.LicenseTestUtils;
import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.metadata.MetadataServiceConfig;
import io.confluent.security.auth.provider.rbac.RbacProvider;
import io.confluent.security.auth.store.data.UserKey;
import io.confluent.security.auth.store.data.UserValue;
import io.confluent.security.auth.store.kafka.KafkaAuthStore;
import io.confluent.security.auth.store.kafka.KafkaAuthWriter;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.PermissionType;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.acl.AclRule;
import io.confluent.security.minikdc.MiniKdcWithLdapService;
import io.confluent.security.store.kafka.KafkaStoreConfig;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import kafka.admin.AclCommand;
import kafka.security.auth.Alter$;
import kafka.security.auth.ClusterAction$;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import scala.Option;

public class RbacClusters {

  private final Config config;
  private final SecurityProtocol kafkaSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT;
  private final String kafkaSaslMechanism = "SCRAM-SHA-256";
  private final EmbeddedKafkaCluster metadataCluster;
  public final EmbeddedKafkaCluster kafkaCluster;
  private final Map<String, User> users;
  public final MiniKdcWithLdapService miniKdcWithLdapService;
  private List<KafkaAuthWriter> authWriters;

  public RbacClusters(Config config) throws Exception {
    this.config = config;
    metadataCluster = new EmbeddedKafkaCluster();
    metadataCluster.startZooKeeper();
    users = createUsers(metadataCluster, config.brokerUser, config.userNames);

    if (config.enableLdap)
      miniKdcWithLdapService = LdapTestUtils.createMiniKdcWithLdapService(null, null);
    else
      miniKdcWithLdapService = null;

    List<Properties> metadataBrokerProps = new ArrayList<>(config.numMetadataServers);
    for (int i = 0; i < config.numMetadataServers; i++)
      metadataBrokerProps.add(metadataCluster.createBrokerConfig(100 + i, metadataClusterServerConfig(i)));
    metadataCluster.concurrentStartBrokers(metadataBrokerProps);

    updateAuthWriters();
    assertNotNull("Master writer not elected:", masterWriter());

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
    assignRole(principalType, userName, role, Scope.kafkaClusterScope(clusterId), resources);
  }

  public void assignRole(String principalType,
                         String userName,
                         String role,
                         Scope scope,
                         Set<ResourcePattern> resources) throws Exception {
    KafkaPrincipal principal = new KafkaPrincipal(principalType, userName);
    masterWriter().replaceResourceRoleBinding(
        principal,
        role,
        scope,
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
      masterWriter().writeExternalEntry(key, value, 1);
    }
  }

  public void createCentralizedAcl(KafkaPrincipal principal,
                                   String operation,
                                   String clusterId,
                                   ResourcePattern resourcePattern,
                                   PermissionType permissionType) throws Exception {
    AclRule accessRule =  new AclRule(principal, permissionType, "*",
        new Operation(operation));
    AclBinding aclBinding = new AclBinding(ResourcePattern.to(resourcePattern), accessRule.toAccessControlEntry());

    masterWriter().createAcls(Scope.kafkaClusterScope(clusterId), aclBinding)
        .toCompletableFuture().get(30, TimeUnit.SECONDS);
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

  public void restartMasterWriter() throws Exception {
    KafkaAuthWriter masterWriter = masterWriter();
    int index = authWriters.indexOf(masterWriter);
    KafkaServer masterWriterBroker = metadataCluster.brokers().get(index);

    KafkaServer anotherBroker = metadataCluster.brokers().get((index + 1) % config.numMetadataServers);
    AuthStore authStore = rbacProvider(anotherBroker).authStore();
    waitForActiveNodes(authStore, config.numMetadataServers);

    masterWriterBroker.shutdown();

    waitForActiveNodes(authStore, config.numMetadataServers - 1);
    TestUtils.waitForCondition(() -> {
      URL masterUrl = authStore.masterWriterUrl("http");
      return masterUrl == null || masterUrl.getPort() != 8000 + index;
    }, () -> "Master writer not reset after broker failure, master=" + authStore.masterWriterUrl("http"));

    masterWriterBroker.startup();
    updateAuthWriters();

    IntStream.range(0, KafkaStoreConfig.NUM_PARTITIONS).forEach(p -> {
      try {
        TestUtils.waitForCondition(() -> hasMinIsrs(masterWriterBroker, KafkaAuthStore.AUTH_TOPIC, p),
            "Insufficient ISRs for auth topic after restart");
      } catch (InterruptedException e) {
        throw new InterruptException(e);
      }
    });

    waitForActiveNodes(authStore, config.numMetadataServers);
    TestUtils.waitForCondition(() -> currentMasterWriter().isPresent(),
        "Master writer not re-elected after broker failure");
  }

  private void waitForActiveNodes(AuthStore authStore, int expectedCount) throws Exception {
    TestUtils.waitForCondition(() -> authStore.activeNodeUrls("http").size() == expectedCount,
        () -> "Unexpected nodes " + authStore.activeNodeUrls("http"));
  }

  private Optional<KafkaAuthWriter> currentMasterWriter() {
    return authWriters.stream().filter(KafkaAuthWriter::ready).findAny();
  }

  private void updateAuthWriters() {
    authWriters = metadataCluster.brokers().stream()
        .map(this::kafkaAuthWriter)
        .collect(Collectors.toList());
  }

  private KafkaAuthWriter masterWriter() {
    try {
      TestUtils.waitForCondition(() -> currentMasterWriter().isPresent(), "Master writer not found");
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
    return currentMasterWriter().orElseThrow(() -> new IllegalStateException("Master writer not found"));
  }

  private boolean hasMinIsrs(KafkaServer broker, String topic, int partition) {
    Option<UpdateMetadataRequest.PartitionState> partitionInfo = broker.metadataCache()
        .getPartitionInfo(topic, partition);
    if (partitionInfo.isEmpty())
      return false;
    UpdateMetadataRequest.PartitionState partitionState = partitionInfo.get();
    return partitionState.basePartitionState.isr.size() >= Math.min(3, kafkaCluster.brokers().size());
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

    return props;
  }

  private void attachTokenListener(Properties props, String publicKey) {

    if (publicKey == null || publicKey.isEmpty()) {
      throw new ConfigException("Public Key must be set prior to enabling Token Authentication");
    }

    String jaasConfig = String.format(
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
            + "publicKeyPath=\"%s\";", publicKey);


    props.setProperty(KafkaConfig$.MODULE$.ListenersProp(),
            props.getProperty(KafkaConfig$.MODULE$.ListenersProp())
                    + ",TOKEN://localhost:0");
    props.setProperty(KafkaConfig$.MODULE$.ListenerSecurityProtocolMapProp(),
            props.getProperty(KafkaConfig$.MODULE$.ListenerSecurityProtocolMapProp())
                    + ",TOKEN:SASL_PLAINTEXT");

    props.setProperty("listener.name.token." + KafkaConfig$.MODULE$.SaslEnabledMechanismsProp(),
            "OAUTHBEARER");
    props.setProperty(
            "listener.name.token.oauthbearer." + KafkaConfig$.MODULE$.SaslJaasConfigProp(),
            jaasConfig);
    props.setProperty("listener.name.token.oauthbearer."
                    + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS,
            "io.confluent.kafka.server.plugins.auth.token.TokenBearerValidatorCallbackHandler");
    props.setProperty("listener.name.token.oauthbearer."
                    + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
            "io.confluent.kafka.server.plugins.auth.token.TokenBearerServerLoginCallbackHandler");
  }

  private Properties metadataClientConfigs() {
    Properties props = new Properties();
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
    serverConfig.putAll(metadataClientConfigs());

    serverConfig.setProperty(KafkaConfig$.MODULE$.AuthorizerClassNameProp(),
        ConfluentServerAuthorizer.class.getName());
    serverConfig.setProperty("super.users", "User:" + config.brokerUser);
    serverConfig.setProperty(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ACL,RBAC");

    if (config.enableTokenLogin) {
      attachTokenListener(serverConfig, config.publicKey);
    }
    serverConfig.putAll(config.serverConfigOverrides);

    return serverConfig;
  }

  private Properties metadataClusterServerConfig(int index) {
    Properties serverConfig = new Properties();
    serverConfig.putAll(scramConfigs());
    serverConfig.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), String.valueOf(100 + index));

    if (config.enableLdap) {
      serverConfig.putAll(LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService, 10));
    }
    serverConfig.setProperty(KafkaConfig$.MODULE$.AuthorizerClassNameProp(),
        ConfluentServerAuthorizer.class.getName());
    serverConfig.setProperty(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ACL,RBAC");
    serverConfig.setProperty("super.users", "User:" + config.brokerUser);
    int metadataPort = 8000 + index;
    serverConfig.setProperty(MetadataServiceConfig.METADATA_SERVER_LISTENERS_PROP,
        "http://0.0.0.0:" + metadataPort);
    serverConfig.setProperty(MetadataServiceConfig.METADATA_SERVER_ADVERTISED_LISTENERS_PROP,
        "http://localhost:" + metadataPort);
    serverConfig.setProperty(KafkaStoreConfig.REPLICATION_FACTOR_PROP, "1");
    serverConfig.setProperty(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), "false");
    serverConfig.putAll(config.metadataClusterPropOverrides);
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

  private RbacProvider rbacProvider(KafkaServer kafkaServer) {
    ConfluentServerAuthorizer authorizer = (ConfluentServerAuthorizer) kafkaServer.authorizer().get();
    return (RbacProvider) authorizer.metadataProvider();
  }

  private KafkaAuthWriter kafkaAuthWriter(KafkaServer kafkaServer) {
    try {
      RbacProvider rbacProvider = rbacProvider(kafkaServer);
      TestUtils.waitForCondition(() -> rbacProvider.authStore() != null,
          "Metadata server not created");
      TestUtils.waitForCondition(() -> rbacProvider.authStore().writer() != null,
          30000, "Metadata writer not started");
      assertTrue(rbacProvider.authStore().writer() instanceof KafkaAuthWriter);
      return (KafkaAuthWriter) rbacProvider.authStore().writer();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static class Config {
    private final Properties metadataClusterPropOverrides = new Properties();
    private final Properties serverConfigOverrides = new Properties();
    private int numMetadataServers = 1;
    private String brokerUser;
    private List<String> userNames;
    private boolean enableLdap;
    private boolean enableLdapGroups;
    private boolean enableTokenLogin;
    private String publicKey;

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

    public Config withTokenLogin(String publicKey) {
      this.enableTokenLogin = true;
      this.publicKey = publicKey;
      return this;
    }

    public Config withLdap() {
      this.enableLdap = true;
      return this;
    }

    public Config withLicense() {
      overrideMetadataBrokerConfig(ConfluentAuthorizerConfig.LICENSE_PROP, LicenseTestUtils.generateLicense());
      return this;
    }

    public Config addMetadataServer() {
      this.numMetadataServers++;
      return this.overrideMetadataBrokerConfig(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), "2")
          .overrideMetadataBrokerConfig(KafkaStoreConfig.REPLICATION_FACTOR_PROP, "2");
    }

    public Config overrideMetadataBrokerConfig(String name, String value) {
      metadataClusterPropOverrides.setProperty(name, value);
      return this;
    }

    public Config overrideBrokerConfig(String name, String value) {
      serverConfigOverrides.setProperty(name, value);
      return this;
    }
  }
}
