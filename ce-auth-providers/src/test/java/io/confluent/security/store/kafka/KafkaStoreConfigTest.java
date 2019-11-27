// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.junit.Test;

public class KafkaStoreConfigTest {

  private final AuthorizerServerInfo serverInfo = KafkaTestUtils.serverInfo("clusterA");

  @Test
  public void testClientConfigs() {
    Properties props = new Properties();
    String bootstrap = "PLAINTEXT://some.host:9092";
    String sslTruststore = "test.truststore.jks";
    props.put(KafkaStoreConfig.BOOTSTRAP_SERVERS_PROP, bootstrap);
    props.put(KafkaStoreConfig.REPLICATION_FACTOR_PROP, "1");
    props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
    props.put("confluent.metadata.ssl.truststore.location", sslTruststore);
    props.put("confluent.metadata.consumer.ssl.keystore.location", "reader.keystore.jks");
    props.put("confluent.metadata.producer.ssl.keystore.location", "writer.keystore.jks");
    props.put("confluent.metadata.coordinator.ssl.keystore.location", "coordinator.keystore.jks");
    KafkaStoreConfig config = new KafkaStoreConfig(serverInfo, props);

    Map<String, Object> readerConfigs = config.consumerConfigs("metadata-auth");
    assertEquals(bootstrap, readerConfigs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(sslTruststore, readerConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    assertEquals("reader.keystore.jks", readerConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertEquals("TLSv1.2", readerConfigs.get(SslConfigs.SSL_PROTOCOL_CONFIG));
    assertFalse(readerConfigs.containsKey("topic.replication.factor"));
    assertNotEquals(readerConfigs.get(CommonClientConfigs.CLIENT_ID_CONFIG),
        config.consumerConfigs("another-topic").get(CommonClientConfigs.CLIENT_ID_CONFIG));

    Map<String, Object> writerConfigs = config.producerConfigs("metadata-auth");
    assertEquals(bootstrap, writerConfigs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(sslTruststore, writerConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    assertEquals("writer.keystore.jks", writerConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertEquals("TLSv1.2", writerConfigs.get(SslConfigs.SSL_PROTOCOL_CONFIG));
    assertFalse(writerConfigs.containsKey("topic.replication.factor"));
    assertNotEquals(writerConfigs.get(CommonClientConfigs.CLIENT_ID_CONFIG),
        config.producerConfigs("another-topic").get(CommonClientConfigs.CLIENT_ID_CONFIG));

    Map<String, Object> coordinatorConfigs = config.coordinatorConfigs();
    assertEquals(bootstrap, coordinatorConfigs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(sslTruststore, coordinatorConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    assertEquals("coordinator.keystore.jks", coordinatorConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertEquals("TLSv1.2", coordinatorConfigs.get(SslConfigs.SSL_PROTOCOL_CONFIG));
    assertFalse(coordinatorConfigs.containsKey("topic.replication.factor"));
  }

  @Test
  public void testInterBrokerClientConfigs() {
    Properties props = new Properties();
    props.put(KafkaConfig$.MODULE$.ZkConnectProp(), "localhost:9092");
    props.put(KafkaConfig$.MODULE$.ListenersProp(), "INTERNAL://localhost:9092");
    props.put(KafkaConfig$.MODULE$.ListenerSecurityProtocolMapProp(), "INTERNAL:SSL");
    props.put(KafkaConfig$.MODULE$.LogDirProp(), "/path/to/logs");
    props.put(KafkaConfig$.MODULE$.BrokerIdProp(), 101);
    props.put(KafkaConfig$.MODULE$.MetricReporterClassesProp(), "org.apache.kafka.common.metrics.JmxReporter");
    props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
    props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "test.truststore.jks");
    props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "test.keystore.jks");
    props.put("listener.name.internal.ssl.keystore.location", "listener.keystore.jks");
    props.put("inter.broker.listener.name", "INTERNAL");
    props.put("confluent.metadata.admin.ssl.keystore.password", "admin.keystore.password");
    props.put("custom.security.config", "custom");
    props.put("confluent.metadata.custom.prefixed.security.config", "custom.prefixed");
    props.put("confluent.metadata.admin.custom.admin.security.config", "custom.admin");

    Endpoint endpoint = new Endpoint("INTERNAL", SecurityProtocol.SSL, "localhost", 9092);
    Map<String, Object> brokerConfigs = ConfluentConfigs.interBrokerClientConfigs(new KafkaConfig(props), endpoint);
    KafkaStoreConfig config = new KafkaStoreConfig(serverInfo, brokerConfigs);
    Map<String, Object> clientConfigs = config.adminClientConfigs();

    Set<String> nonClientConfigs = new HashSet<>(clientConfigs.keySet());
    nonClientConfigs.removeAll(AdminClientConfig.configNames());
    assertEquals(Utils.mkSet("custom.security.config", "custom.prefixed.security.config", "custom.admin.security.config"),
        nonClientConfigs);
    assertEquals("SSL", clientConfigs.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG));
    assertEquals("localhost:9092", clientConfigs.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("test.truststore.jks", clientConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    assertEquals("listener.keystore.jks", clientConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertEquals("admin.keystore.password", clientConfigs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
    assertEquals("TLSv1.2", clientConfigs.get(SslConfigs.SSL_PROTOCOL_CONFIG));
    assertEquals("custom", clientConfigs.get("custom.security.config"));
    assertEquals("custom.prefixed", clientConfigs.get("custom.prefixed.security.config"));
    assertEquals("custom.admin", clientConfigs.get("custom.admin.security.config"));
    assertFalse(clientConfigs.containsKey(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG));
  }
}
