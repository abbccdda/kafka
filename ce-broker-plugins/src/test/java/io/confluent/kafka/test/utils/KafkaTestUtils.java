// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.test.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

public class KafkaTestUtils {

  private static final Set<String> UNEXPECTED_THREADS = Utils.mkSet(
      AbstractCoordinator.HEARTBEAT_THREAD_PREFIX,
      "event-process-thread",
      "EventThread",
      "auth-writer-",
      "auth-reader-",
      "metadata-service-coordinator",
      "license-",
      "authorizer-");

  public static Properties brokerConfig(Properties overrideProps) throws Exception {
    Properties serverConfig = new Properties();
    serverConfig.setProperty(KafkaConfig$.MODULE$.ListenersProp(),
        "INTERNAL://localhost:0,EXTERNAL://localhost:0");
    serverConfig.setProperty(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(), "INTERNAL");
    serverConfig.setProperty(KafkaConfig$.MODULE$.SaslEnabledMechanismsProp(), "SCRAM-SHA-256");
    serverConfig.setProperty(KafkaConfig$.MODULE$.ListenerSecurityProtocolMapProp(),
        "INTERNAL:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT");
    serverConfig.setProperty("listener.name.external.scram-sha-256.sasl.jaas.config",
        "org.apache.kafka.common.security.scram.ScramLoginModule required;");
    serverConfig.putAll(overrideProps);

    return serverConfig;
  }

  public static Properties producerProps(
      String bootstrapServers,
      SecurityProtocol securityProtocol,
      String saslMechanism,
      String jaasConfig) {
    Properties props = new Properties();
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
    props.setProperty(SaslConfigs.SASL_MECHANISM, saslMechanism);
    props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
    props.setProperty(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    return props;
  }

  public static KafkaProducer<String, String> createProducer(
      String bootstrapServers,
      SecurityProtocol securityProtocol,
      String saslMechanism,
      String jaasConfig) {
    return new KafkaProducer<>(producerProps(bootstrapServers, securityProtocol, saslMechanism, jaasConfig));
  }

  public static void sendRecords(KafkaProducer<String, String> producer, String topic,
      int first, int count)
      throws Throwable {
    List<Future<RecordMetadata>> futures = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      int index = first + i;
      ProducerRecord<String, String> record =
          new ProducerRecord<>(topic, String.valueOf(index), "value" + index);
      futures.add(producer.send(record));
    }
    for (Future<RecordMetadata> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        throw e.getCause();
      }
    }
  }

  public static KafkaConsumer<String, String> createConsumer(
      String bootstrapServers,
      SecurityProtocol securityProtocol,
      String saslMechanism,
      String jaasConfig,
      String consumerGroup) {
    Properties props = new Properties();
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
    props.setProperty(SaslConfigs.SASL_MECHANISM, saslMechanism);
    props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
    props.setProperty(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return new KafkaConsumer<>(props);
  }

  public static void consumeRecords(KafkaConsumer<String, String> consumer, String topic,
      int first, int count) throws Exception {
    consumer.subscribe(Collections.singleton(topic));
    consumeRecords(consumer, first, count);
  }

  public static void consumeRecords(KafkaConsumer<String, String> consumer, int first, int count) throws Exception {
    int received = 0;
    long endTimeMs = System.currentTimeMillis() + 30000;
    while (received < count && System.currentTimeMillis() < endTimeMs) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
      received += records.count();
      for (ConsumerRecord<String, String> record : records) {
        int key = Integer.parseInt(record.key());
        assertTrue("Unexpected record " + key, key >= first && key < first + count);
      }
    }
  }

  public static AdminClient createAdminClient(
      String bootstrapServers,
      SecurityProtocol securityProtocol,
      String saslMechanism,
      String jaasConfig) {
    return createAdminClient(bootstrapServers, securityProtocol, saslMechanism, jaasConfig, new Properties());
  }

  public static AdminClient createAdminClient(
      String bootstrapServers,
      SecurityProtocol securityProtocol,
      String saslMechanism,
      String jaasConfig,
      Properties props) {
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
    props.setProperty(SaslConfigs.SASL_MECHANISM, saslMechanism);
    props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
    props.setProperty(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");

    return AdminClient.create(props);
  }

  @SuppressWarnings("unchecked")
  public static <T> T fieldValue(Object o, Class<?> clazz, String fieldName)  {
    try {
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return (T) field.get(o);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public static void setFinalField(Object o, Class<?> clazz, String fieldName, Object value)  {
    try {
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      int modifiers = field.getModifiers();
      modifiersField.setInt(field, modifiers & ~Modifier.FINAL);
      field.set(o, value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean canAccess(AdminClient adminClient, String topic) {
    try {
      return adminClient.describeTopics(Collections.singleton(topic)).all().get().containsKey(topic);
    } catch (Exception e) {
      return false;
    }
  }

  public static void verifyProduceConsume(ClientBuilder clientBuilder,
                                          String topic,
                                          String consumerGroup,
                                          boolean authorized) throws Throwable {
    verifyProduce(clientBuilder, topic, authorized);
    verifyConsume(clientBuilder, consumerGroup, c -> c.subscribe(Collections.singleton(topic)), authorized);
  }

  public static void verifyProduce(ClientBuilder clientBuilder,
                                   String topic,
                                   boolean authorized) throws Throwable {
    try (KafkaProducer<String, String> producer = clientBuilder.buildProducer()) {
      KafkaTestUtils.sendRecords(producer, topic, 0, 10);
      assertTrue("No authorization exception from unauthorized client", authorized);
    } catch (AuthorizationException e) {
      assertFalse("Authorization exception from authorized client", authorized);
    }
  }

  public static void verifyConsume(ClientBuilder clientBuilder,
                                   String consumerGroup,
                                   java.util.function.Consumer<KafkaConsumer<String, String>> subscribeOrAssign,
                                   boolean authorized) throws Throwable {

    try (KafkaConsumer<String, String> consumer = clientBuilder.buildConsumer(consumerGroup)) {
      subscribeOrAssign.accept(consumer);
      KafkaTestUtils.consumeRecords(consumer, 0, 10);
      assertTrue("No authorization exception from unauthorized client", authorized);
    } catch (AuthorizationException e) {
      assertFalse("Authorization exception from authorized client", authorized);
    }
  }

  public static void addProducerAcls(ClientBuilder clientBuilder,
                                     KafkaPrincipal principal,
                                     String topic,
                                     PatternType patternType) throws Exception {
    try (AdminClient adminClient = clientBuilder.buildAdminClient()) {
      AclBinding topicAcl = new AclBinding(
          new ResourcePattern(ResourceType.TOPIC, topic, patternType),
          new AccessControlEntry(principal.toString(),
              "*", AclOperation.WRITE, AclPermissionType.ALLOW));
      adminClient.createAcls(Arrays.asList(topicAcl)).all().get();
    }
  }

  public static void addConsumerAcls(ClientBuilder clientBuilder,
                                     KafkaPrincipal principal,
                                     String topic,
                                     String consumerGroup,
                                     PatternType patternType) throws Exception {
    try (AdminClient adminClient = clientBuilder.buildAdminClient()) {
      AclBinding topicAcl = new AclBinding(
          new ResourcePattern(ResourceType.TOPIC, topic, patternType),
          new AccessControlEntry(principal.toString(),
              "*", AclOperation.READ, AclPermissionType.ALLOW));
      AclBinding consumerGroupAcl = new AclBinding(
          new ResourcePattern(ResourceType.GROUP, consumerGroup, patternType),
          new AccessControlEntry(principal.toString(),
              "*", AclOperation.ALL, AclPermissionType.ALLOW));
      List<AclBinding> acls = Arrays.asList(topicAcl, consumerGroupAcl);
      adminClient.createAcls(acls).all().get();
    }
  }

  public static class ClientBuilder {
    private final String bootstrapServers;
    private final SecurityProtocol securityProtocol;
    private final String saslMechanism;
    private final String jaasConfig;

    public ClientBuilder(String bootstrapServers,
                        SecurityProtocol securityProtocol,
                        String saslMechanism,
                        String jaasConfig) {
      this.bootstrapServers = bootstrapServers;
      this.securityProtocol = securityProtocol;
      this.saslMechanism = saslMechanism;
      this.jaasConfig = jaasConfig;
    }

    public KafkaProducer<String, String> buildProducer() {
      return createProducer(bootstrapServers, securityProtocol, saslMechanism, jaasConfig);
    }

    public KafkaConsumer<String, String> buildConsumer(String consumerGroup) {
      return createConsumer(bootstrapServers, securityProtocol, saslMechanism, jaasConfig, consumerGroup);
    }

    public AdminClient buildAdminClient() {
      return createAdminClient(bootstrapServers, securityProtocol, saslMechanism, jaasConfig);
    }
  }

  public static void verifyThreadCleanup() {
    try {
      TestUtils.waitForCondition(
          () -> Thread.getAllStackTraces().keySet().stream().noneMatch(KafkaTestUtils::isUnexpectedThread),
          () -> "Unexpected threads: " + Thread.getAllStackTraces().keySet());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isUnexpectedThread(Thread thread) {
    return UNEXPECTED_THREADS.stream().anyMatch(thread.getName()::contains);
  }
}
