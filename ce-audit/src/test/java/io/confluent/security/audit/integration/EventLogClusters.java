/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.security.audit.integration;

import io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.audit.AuditLogConfig;
import io.confluent.security.audit.provider.ConfluentAuditLogProvider;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.test.utils.User;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.admin.AclCommand;
import kafka.security.auth.Alter$;
import kafka.security.auth.ClusterAction$;
import kafka.security.authorizer.AclAuthorizer;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
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
  private final Map<String, User> users = new HashMap<>();
  private String logWriterUser;

  public EventLogClusters(EventLogClusters.Config config) throws Exception {
    this.config = config;
    kafkaCluster = new EmbeddedKafkaCluster();
    kafkaCluster.startZooKeeper();
    createBrokerUser(config.brokerUser);
    createLogWriterUser(config.logWriterUser);
    createLogReaderUser(config.logReaderUser);
    kafkaCluster.startBrokers(config.numBrokers, serverConfig());
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
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props
        .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
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
        ByteArraySerializer.class.getName());
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
        AclAuthorizer.class.getName());
    serverConfig.setProperty("super.users", "User:" + config.brokerUser);
    serverConfig.setProperty(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ACL");
    if (config.routerConfig != null) {
      serverConfig.put(AuditLogConfig.ROUTER_CONFIG, config.routerConfig);
    }
    serverConfig.put(AuditLogConfig.AUDIT_LOG_PRINCIPAL_CONFIG,
        config.auditLogPrincipal);
    serverConfig.put("auto.create.topics.enable", false);
    return serverConfig;
  }

  private void createBrokerUser(String brokerUser) {
    users.put(brokerUser, User.createScramUser(kafkaCluster, brokerUser));
    String zkConnect = kafkaCluster.zkConnect();
    KafkaPrincipal brokerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, brokerUser);
    AclCommand.main(SecurityTestUtils
        .clusterAclArgs(zkConnect, brokerPrincipal, ClusterAction$.MODULE$.name()));
    AclCommand
        .main(SecurityTestUtils.clusterAclArgs(zkConnect, brokerPrincipal, Alter$.MODULE$.name()));
    AclCommand.main(SecurityTestUtils.topicBrokerReadAclArgs(zkConnect, brokerPrincipal));
  }

  public void createLogWriterUser(String logWriterUser) {
    users.put(logWriterUser, User.createScramUser(kafkaCluster, logWriterUser));
    String zkConnect = kafkaCluster.zkConnect();
    KafkaPrincipal eventLoggerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE,
        logWriterUser);
    AclCommand.main(SecurityTestUtils
        .produceAclArgs(zkConnect, eventLoggerPrincipal, AuditLogRouterJsonConfig.TOPIC_PREFIX,
            PatternType.PREFIXED));
    this.logWriterUser = logWriterUser;
  }

  public void createLogReaderUser(String logReaderUser) {
    users.put(logReaderUser, User.createScramUser(kafkaCluster, logReaderUser));
    String zkConnect = kafkaCluster.zkConnect();
    KafkaPrincipal logReaderPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, logReaderUser);
    AclCommand.main(SecurityTestUtils
        .consumeAclArgs(zkConnect, logReaderPrincipal, AuditLogRouterJsonConfig.TOPIC_PREFIX,
            "event-log", PatternType.PREFIXED));
  }

  public String logWriterUser() {
    return logWriterUser;
  }

  public boolean auditLoggerReady() {
    try {
      if (kafkaCluster.brokers().isEmpty()) {
        return false;
      }
      for (KafkaServer broker : kafkaCluster.brokers()) {
        ConfluentServerAuthorizer authorizer =
            (ConfluentServerAuthorizer) broker.authorizer().get();
        ConfluentAuditLogProvider provider =
            (ConfluentAuditLogProvider) authorizer.auditLogProvider();
        if (!provider.isEventLoggerReady()) {
          return false;
        }
      }
      return true;
    } catch (ClassCastException e) {
      return false;
    }
  }

  public static class Config {

    private String brokerUser;
    private String logWriterUser;
    private String logReaderUser;
    private String auditLogPrincipal = AuditLogConfig.DEFAULT_AUDIT_LOG_PRINCIPAL_CONFIG;
    private int numBrokers = 1;
    private String routerConfig = null;

    public Config users(String brokerUser, String eventLoggerUser,
        String logReaderUser) {
      this.brokerUser = brokerUser;
      this.logWriterUser = eventLoggerUser;
      this.logReaderUser = logReaderUser;
      return this;
    }

    public Config setAuditLogPrincipal(String auditLogPrincipal) {
      this.auditLogPrincipal = auditLogPrincipal;
      return this;
    }

    public void setNumBrokers(int numBrokers) {
      this.numBrokers = numBrokers;
    }

    public void setRouterConfig(String routerConfig) {
      this.routerConfig = routerConfig;
    }
  }
}
