package io.confluent.security.audit.integration;

import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.audit.serde.CloudEventProtoSerde;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.test.utils.User;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.admin.AclCommand;
import kafka.security.auth.Alter$;
import kafka.security.auth.ClusterAction$;
import kafka.security.auth.SimpleAclAuthorizer;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class EventLogClusters {

  private final EventLogClusters.Config config;
  private final SecurityProtocol kafkaSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT;
  private final String kafkaSaslMechanism = "SCRAM-SHA-256";
  public final EmbeddedKafkaCluster kafkaCluster;
  private final Map<String, User> users;

  public EventLogClusters(EventLogClusters.Config config) throws Exception {
    this.config = config;
    kafkaCluster = new EmbeddedKafkaCluster();
    kafkaCluster.startZooKeeper();
    users = createUsers(kafkaCluster, config.brokerUser, config.eventLoggerUser,
        config.logReaderUser);
    kafkaCluster.startBrokers(1, serverConfig());
  }

  public String kafkaClusterId() {
    return kafkaCluster.kafkas().get(0).kafkaServer().clusterId();
  }

  public Properties consumerProps(String user, String consumerGroup) {
    Properties props = KafkaTestUtils.consumerProps(kafkaCluster.bootstrapServers(),
        kafkaSecurityProtocol,
        kafkaSaslMechanism,
        users.get(user).jaasConfig,
        consumerGroup);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        CloudEventProtoSerde.class.getName());
    return props;
  }

  public Properties producerProps(String user) {
    Properties props = KafkaTestUtils.producerProps(kafkaCluster.bootstrapServers(),
        kafkaSecurityProtocol,
        kafkaSaslMechanism,
        users.get(user).jaasConfig);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        CloudEventProtoSerde.class.getName());
    return props;
  }

  public void shutdown() {
    kafkaCluster.shutdown();
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

  private Properties serverConfig() {
    Properties serverConfig = new Properties();
    serverConfig.putAll(scramConfigs());

    serverConfig.setProperty(KafkaConfig$.MODULE$.AuthorizerClassNameProp(),
        SimpleAclAuthorizer.class.getName());
    //    ConfluentServerAuthorizer.class.getName());
    serverConfig.setProperty("super.users", "User:" + config.brokerUser);
    serverConfig.setProperty(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ACL");
    return serverConfig;
  }

  private Map<String, User> createUsers(EmbeddedKafkaCluster cluster,
      String brokerUser, String eventLoggerUser, String logReaderUser) {
    Map<String, User> users = new HashMap<>();
    users.put(brokerUser, User.createScramUser(cluster, brokerUser));
    users.put(eventLoggerUser, User.createScramUser(cluster, eventLoggerUser));
    users.put(logReaderUser, User.createScramUser(cluster, logReaderUser));

    String zkConnect = cluster.zkConnect();
    KafkaPrincipal brokerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, brokerUser);
    AclCommand.main(SecurityTestUtils
        .clusterAclArgs(zkConnect, brokerPrincipal, ClusterAction$.MODULE$.name()));
    AclCommand
        .main(SecurityTestUtils.clusterAclArgs(zkConnect, brokerPrincipal, Alter$.MODULE$.name()));
    AclCommand.main(SecurityTestUtils.topicBrokerReadAclArgs(zkConnect, brokerPrincipal));

    KafkaPrincipal eventLoggerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE,
        eventLoggerUser);
    AclCommand.main(SecurityTestUtils
        .produceAclArgs(zkConnect, eventLoggerPrincipal, EventLogConfig.DEFAULT_TOPIC_CONFIG,
            PatternType.PREFIXED));

    KafkaPrincipal logReaderPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, logReaderUser);
    AclCommand.main(SecurityTestUtils
        .consumeAclArgs(zkConnect, logReaderPrincipal, EventLogConfig.DEFAULT_TOPIC_CONFIG,
            "event-log", PatternType.PREFIXED));
    return users;
  }

  public static class Config {

    private String brokerUser;
    private String eventLoggerUser;
    private String logReaderUser;

    public EventLogClusters.Config users(String brokerUser, String eventLoggerUser,
        String logReaderUser) {
      this.brokerUser = brokerUser;
      this.eventLoggerUser = eventLoggerUser;
      this.logReaderUser = logReaderUser;
      return this;
    }
  }
}
