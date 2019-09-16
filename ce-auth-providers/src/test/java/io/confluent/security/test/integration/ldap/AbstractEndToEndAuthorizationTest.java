// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.test.integration.ldap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.security.ldap.authorizer.LdapAuthorizer;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.auth.provider.ldap.LdapContextCreator;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.minikdc.MiniKdcWithLdapService.LdapSecurityAuthentication;
import io.confluent.security.minikdc.MiniKdcWithLdapService.LdapSecurityProtocol;
import io.confluent.security.test.utils.User;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.security.auth.login.Configuration;
import kafka.admin.AclCommand;
import kafka.security.auth.ClusterAction$;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public abstract class AbstractEndToEndAuthorizationTest {

  public static final String ADMIN_GROUP = "Kafka Admin";
  public static final String DEV_GROUP = "Kafka Developers";
  public static final String TEST_GROUP = "Kafka Testers";

  public static final String KAFKA_SERVICE = "kafka";
  public static final String LDAP_USER = "ldap";
  public static final String DEVELOPER = "kafkaDeveloper";
  public static final String TESTER = "kafkaTester";
  public static final String SRE = "kafkaSre";
  public static final String INTERN = "kafkaIntern";

  public static final String DEV_TOPIC = "dev-topic";
  public static final String DEV_CONSUMER_GROUP = "dev-consumer-group";
  public static final String TEST_TOPIC = "test-topic";
  public static final String TEST_CONSUMER_GROUP = "test-consumer-group";

  protected final SecurityProtocol kafkaSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT;
  protected final String kafkaSaslMechanism;
  protected final LdapSecurityProtocol ldapSecurityProtocol;
  protected final LdapSecurityAuthentication ldapSecurityAuthentication;
  protected final String ldapUser;
  protected final int ldapRefreshIntervalMs;

  protected EmbeddedKafkaCluster kafkaCluster;
  protected String zkConnect;

  protected Map<String, User> users;

  private final List<KafkaProducer<String, String>> producers = new ArrayList<>();
  private final List<KafkaConsumer<String, String>> consumers = new ArrayList<>();


  public AbstractEndToEndAuthorizationTest(String kafkaSaslMechanism,
      LdapSecurityProtocol ldapSecurityProtocol,
      LdapSecurityAuthentication ldapSecurityAuthentication,
      String ldapUser,
      int ldapRefreshIntervalMs) {

    Configuration.setConfiguration(null);
    this.kafkaSaslMechanism = kafkaSaslMechanism;
    this.ldapSecurityProtocol = ldapSecurityProtocol;
    this.ldapSecurityAuthentication = ldapSecurityAuthentication;
    switch (ldapSecurityAuthentication) {
      case NONE:
        if (ldapUser != null) {
          throw new IllegalArgumentException("LDAP user specified with authentication disabled");
        }
        break;
      case GSSAPI:
        if (ldapUser == null || ldapUser.isEmpty()) {
          throw new IllegalArgumentException("LDAP user not specified with GSSAPI");
        }
        break;
      default:
        break;
    }
    this.ldapUser = ldapUser;
    this.ldapRefreshIntervalMs = ldapRefreshIntervalMs;
  }

  @Before
  public void setUp() throws Throwable {
    kafkaCluster = new EmbeddedKafkaCluster();
    kafkaCluster.startZooKeeper();
    zkConnect = kafkaCluster.zkConnect();

    users = createUsers();
    kafkaCluster.startBrokers(1, kafkaServerConfig());
    createTopics();
  }

  @After
  public void tearDown() throws Exception {
    try {
      for (KafkaProducer<?, ?> producer : producers) {
        producer.close();
      }
      for (KafkaConsumer<?, ?> consumer : consumers) {
        consumer.close();
      }
      kafkaCluster.shutdown();
    } finally {
      SecurityTestUtils.clearSecurityConfigs();
      KafkaTestUtils.verifyThreadCleanup();
    }
  }

  @Test
  public void testProduceConsumeWithGroupAcl() throws Throwable {

    // Add group ACLs for dev and test topics and consumer groups
    addAcls(groupPrincipal(DEV_GROUP), DEV_TOPIC, DEV_CONSUMER_GROUP);
    addAcls(groupPrincipal(TEST_GROUP), TEST_TOPIC, TEST_CONSUMER_GROUP);

    // Test group-based access for developer
    produceConsume(users.get(DEVELOPER), DEV_TOPIC, DEV_CONSUMER_GROUP, true);

    // Tester has no access to topic of dev, has access to topics/consumer groups of test
    produceConsume(users.get(TESTER), DEV_TOPIC, DEV_CONSUMER_GROUP, false);
    produceConsume(users.get(TESTER), TEST_TOPIC, TEST_CONSUMER_GROUP, true);

    // SRE has access to both dev and test topics/consumer groups
    produceConsume(users.get(SRE), DEV_TOPIC, DEV_CONSUMER_GROUP, true);
    produceConsume(users.get(SRE), TEST_TOPIC, TEST_CONSUMER_GROUP, true);

    // Intern doesn't belong to any groups, but is given user-based access
    addAcls(userPrincipal(INTERN), DEV_TOPIC, DEV_CONSUMER_GROUP);
    produceConsume(users.get(INTERN), DEV_TOPIC, DEV_CONSUMER_GROUP, true);
    produceConsume(users.get(INTERN), TEST_TOPIC, TEST_CONSUMER_GROUP, false);
  }

  @Test
  public void testProduceConsumeWithWildcardPrincipalAcl() throws Throwable {
    addAcls(groupPrincipal("*"), DEV_TOPIC, DEV_CONSUMER_GROUP);
    produceConsume(users.get(DEVELOPER), DEV_TOPIC, DEV_CONSUMER_GROUP, true);
    produceConsume(users.get(TESTER), DEV_TOPIC, DEV_CONSUMER_GROUP, true);
    produceConsume(users.get(INTERN), DEV_TOPIC, DEV_CONSUMER_GROUP, false); // not in any group

    addAcls(userPrincipal("*"), TEST_TOPIC, TEST_CONSUMER_GROUP);
    produceConsume(users.get(DEVELOPER), TEST_TOPIC, TEST_CONSUMER_GROUP, true);
    produceConsume(users.get(TESTER), TEST_TOPIC, TEST_CONSUMER_GROUP, true);
    produceConsume(users.get(INTERN), TEST_TOPIC, TEST_CONSUMER_GROUP, true);
  }

  @Test
  public void testProduceConsumeWithWildcardResourceAcl() throws Throwable {
    addAcls(groupPrincipal(DEV_GROUP), "*", "*");
    produceConsume(users.get(DEVELOPER), DEV_TOPIC, DEV_CONSUMER_GROUP, true);
    produceConsume(users.get(TESTER), DEV_TOPIC, DEV_CONSUMER_GROUP, false);

    addAcls(userPrincipal(TESTER), "*", "*");
    produceConsume(users.get(TESTER), DEV_TOPIC, DEV_CONSUMER_GROUP, true);
  }

  protected KafkaPrincipal groupPrincipal(String group) {
    return new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, group);
  }

  protected KafkaPrincipal userPrincipal(String user) {
    return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user);
  }

  protected void addAcls(KafkaPrincipal principal, String topic, String consumerGroup)
      throws Exception {
    AclCommand.main(SecurityTestUtils.produceAclArgs(zkConnect, principal, topic,
        PatternType.LITERAL));
    AclCommand.main(SecurityTestUtils.consumeAclArgs(zkConnect, principal, topic, consumerGroup,
        PatternType.LITERAL));
  }

  protected void produceConsume(User user, String topic, String consumerGroup, boolean authorized)
      throws Throwable {
    try (KafkaProducer<String, String> producer = createProducer(user)) {
      KafkaTestUtils.sendRecords(producer, topic, 0, 10);
      assertTrue("No authorization exception from unauthorized client", authorized);
    } catch (AuthorizationException e) {
      assertFalse("Authorization exception from authorized client", authorized);
    }

    try (KafkaConsumer<String, String> consumer = createConsumer(user, consumerGroup)) {
      KafkaTestUtils.consumeRecords(consumer, topic, 0, 10);
      assertTrue("No authorization exception from unauthorized client", authorized);
    } catch (AuthorizationException e) {
      assertFalse("Authorization exception from authorized client", authorized);
    }
  }

  protected KafkaProducer<String, String> createProducer(User user) {
    KafkaProducer<String, String> producer = KafkaTestUtils.createProducer(
        kafkaCluster.bootstrapServers(),
        kafkaSecurityProtocol,
        kafkaSaslMechanism,
        user.jaasConfig);
    producers.add(producer);
    return producer;
  }

  private void createTopics() throws Exception {
    kafkaCluster.createTopic(DEV_TOPIC, 2, 1);
    kafkaCluster.createTopic(TEST_TOPIC, 2, 1);
  }

  private KafkaConsumer<String, String> createConsumer(User user, String consumerGroup) {
    KafkaConsumer<String, String> consumer = KafkaTestUtils.createConsumer(
        kafkaCluster.bootstrapServers(),
        kafkaSecurityProtocol,
        kafkaSaslMechanism,
        user.jaasConfig,
        consumerGroup);
    consumers.add(consumer);
    return consumer;
  }

  protected Properties kafkaServerConfig() throws Exception {
    Properties serverConfig = new Properties();
    serverConfig.setProperty(KafkaConfig$.MODULE$.ListenersProp(),
        kafkaSecurityProtocol + "://localhost:0");
    serverConfig.setProperty(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(),
        kafkaSecurityProtocol.name);
    serverConfig.setProperty(KafkaConfig$.MODULE$.SaslEnabledMechanismsProp(),
        kafkaSaslMechanism);
    serverConfig.setProperty(KafkaConfig$.MODULE$.SaslMechanismInterBrokerProtocolProp(),
        kafkaSaslMechanism);
    String brokerJaasConfigProp = ListenerName.forSecurityProtocol(kafkaSecurityProtocol)
        .saslMechanismConfigPrefix(kafkaSaslMechanism) + KafkaConfig$.MODULE$.SaslJaasConfigProp();
    serverConfig.setProperty(brokerJaasConfigProp, users.get(KAFKA_SERVICE).jaasConfig);
    serverConfig.setProperty(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
    serverConfig.setProperty(KafkaConfig$.MODULE$.AuthorizerClassNameProp(),
        LdapAuthorizer.class.getName());
    serverConfig.put(ConfluentAuthorizerConfig.ALLOW_IF_NO_ACLS_PROP, "false");
    serverConfig.putAll(authorizerConfig());

    // Test shared KafkaServer static configuration
    if (KAFKA_SERVICE.equals(ldapUser)) {
      Password jaasConfig = new Password((String) serverConfig.remove(brokerJaasConfigProp));
      serverConfig.remove("ldap.authorizer.sasl.jaas.config");
      Configuration
          .setConfiguration(LdapContextCreator.jaasContext(jaasConfig, "GSSAPI").configuration());
    }
    return serverConfig;
  }

  protected abstract Properties authorizerConfig();

  protected abstract User createGssapiUser(String name, String principal);

  private User createUser(String name, String saslMechanism) {
    User user;
    if (ScramMechanism.isScram(saslMechanism)) {
      String password = name + "-secret";
      String scramSecret = SecurityTestUtils.createScramUser(kafkaCluster, name, password);
      user = User.scramUser(name, scramSecret);
    } else if (saslMechanism.equals("GSSAPI")) {
      String hostSuffix = KAFKA_SERVICE.equals(name) ? "/localhost" : "";
     user = createGssapiUser(name, name + hostSuffix);
    } else
      throw new UnsupportedOperationException("SASL mechanism not supported " + saslMechanism);
    return user;
  }

  private Map<String, User> createUsers() {
    Map<String, User>  users = new HashMap<>();
    users.put(KAFKA_SERVICE, createUser(KAFKA_SERVICE, kafkaSaslMechanism));
    users.put(DEVELOPER, createUser(DEVELOPER, kafkaSaslMechanism));
    users.put(TESTER, createUser(TESTER, kafkaSaslMechanism));
    users.put(SRE, createUser(SRE, kafkaSaslMechanism));
    users.put(INTERN, createUser(INTERN, kafkaSaslMechanism));
    if (ldapUser != null && !users.containsKey(ldapUser)) {
      users.put(ldapUser, createUser(ldapUser, ldapSecurityAuthentication.name()));
    }

    AclCommand.main(SecurityTestUtils.clusterAclArgs(zkConnect,
        groupPrincipal(ADMIN_GROUP), ClusterAction$.MODULE$.name()));
    AclCommand.main(SecurityTestUtils.topicBrokerReadAclArgs(zkConnect,
        groupPrincipal(ADMIN_GROUP)));
    return users;
  }

}

