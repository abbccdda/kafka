// (Copyright) [2018 - 2018] Confluent, Inc.
package io.confluent.kafka.multitenant.integration.test;

import io.confluent.kafka.multitenant.PhysicalClusterMetadata;
import io.confluent.kafka.multitenant.authorizer.MultiTenantAuthorizer;
import io.confluent.kafka.multitenant.integration.cluster.LogicalCluster;
import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;

import io.confluent.kafka.server.plugins.policy.AlterConfigPolicy;
import io.confluent.kafka.server.plugins.policy.AlterConfigPolicy.ClusterPolicyConfig;
import io.confluent.kafka.server.plugins.policy.CreateTopicPolicy;
import io.confluent.kafka.server.plugins.policy.TopicPolicyConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import kafka.security.authorizer.AclAuthorizer;
import kafka.server.KafkaConfig;

import kafka.server.KafkaServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

@Category(IntegrationTest.class)
public class MultiTenantKafkaIntegrationTest {

  private static final int BROKER_COUNT = 2;

  private final ListenerName externalListenerName = new ListenerName("external");
  private final String externalListenerPrefix = externalListenerName.configPrefix();

  private IntegrationTestHarness testHarness;
  private LogicalCluster logicalCluster1;
  private LogicalCluster logicalCluster2;
  private PhysicalCluster physicalCluster;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    testHarness = new IntegrationTestHarness(BROKER_COUNT);
  }

  private void createPhysicalAndLogicalClusters() {
    createPhysicalAndLogicalClusters(brokerProps());
  }

  private void createPhysicalAndLogicalClusters(Properties brokerProperties) {
    physicalCluster = testHarness.start(brokerProperties);

    logicalCluster1 = physicalCluster.createLogicalCluster("tenantA", 100, 9, 11, 12);
    logicalCluster2 = physicalCluster.createLogicalCluster("tenantB", 200, 9, 21, 22);
  }

  @After
  public void tearDown() {
    testHarness.shutdown();
  }

  private Properties brokerProps() {
    Properties props = new Properties();
    props.put(KafkaConfig.AuthorizerClassNameProp(), MultiTenantAuthorizer.class.getName());
    props.put(MultiTenantAuthorizer.MAX_ACLS_PER_TENANT_PROP, "100");
    props.put(AclAuthorizer.AllowEveryoneIfNoAclIsFoundProp(), "true");
    props.put(ConfluentConfigs.MULTITENANT_METADATA_CLASS_CONFIG,
        "io.confluent.kafka.multitenant.PhysicalClusterMetadata");
    props.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
        tempFolder.getRoot().getAbsolutePath());
    props.put(KafkaConfig.AlterConfigPolicyClassNameProp(), AlterConfigPolicy.class.getName());
    props.put(KafkaConfig.CreateTopicPolicyClassNameProp(), CreateTopicPolicy.class.getName());
    props.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "1");
    props.put(KafkaConfig.AutoCreateTopicsEnableProp(), "false");
    return props;
  }

  @Test
  public void testMultiTenantMetadataInstances() {
    createPhysicalAndLogicalClusters();

    List<String> brokerSessionUuids = physicalCluster.kafkaCluster().brokers().stream()
        .map(broker -> {
          Object cfgVal = broker.config().values().get(KafkaConfig.BrokerSessionUuidProp());
          return cfgVal == null ? "" : cfgVal.toString();
        })
        .distinct()
        .collect(Collectors.toList());
    assertEquals("Expect each broker to have unique session UUID.",
                 BROKER_COUNT, brokerSessionUuids.size());
    brokerSessionUuids.forEach(uuid -> assertNotNull(
        "Expect valid instance of PhysicalClusterMetadata for broker session UUID " + uuid,
        PhysicalClusterMetadata.getInstance(uuid)));
  }

  @Test
  public void testProduceConsume() throws Throwable {
    createPhysicalAndLogicalClusters();

    // Users 1 and 2 belonging to the same cluster should be able to produce
    // and consume from the same `testtopic`
    testHarness.produceConsume(logicalCluster1.user(11), logicalCluster1.user(12), "testtopic", "group1", 0);

    // Users 3 and 4 belonging to a different cluster should be able to produce
    // and consume different set of messages from `testtopic`
    testHarness.produceConsume(logicalCluster2.user(21), logicalCluster2.user(22), "testtopic", "group1", 1000);

  }

  /**
   * Alter broker configs should be rejected via the external listener when
   * confluent.alter.cluster.configs=false (the default).
   */
  @Test
  public void testAlterBrokerConfigsWhenConfigDisabled() throws Exception {
    createPhysicalAndLogicalClusters();
    AdminClient tenantAdminClient = testHarness.createAdminClient(logicalCluster1.adminUser());
    AdminClient internalAdminClient = physicalCluster.superAdminClient();
    expectAlterBrokerConfigsViaExternalListenerRejected(tenantAdminClient, internalAdminClient,
        new ConfigResource(Type.BROKER, "0"));
  }

  /**
   * Alter cluster (resourceName == "") configs should be rejected via the external listener when
   * confluent.alter.cluster.configs=false (the default).
   */
  @Test
  public void testAlterClusterConfigsWhenConfigDisabled() throws Exception {
    createPhysicalAndLogicalClusters();
    AdminClient tenantAdminClient = testHarness.createAdminClient(logicalCluster1.adminUser());
    AdminClient internalAdminClient = physicalCluster.superAdminClient();
    expectAlterBrokerConfigsViaExternalListenerRejected(tenantAdminClient, internalAdminClient,
        new ConfigResource(Type.BROKER, ""));
  }

  @SuppressWarnings("deprecation")
  private void expectAlterBrokerConfigsViaExternalListenerRejected(AdminClient tenantAdminClient,
      AdminClient internalAdminClient, ConfigResource configResource) throws Exception {
    KafkaServer broker0 = physicalCluster.kafkaCluster().kafkas().get(0).kafkaServer();
    List<String> defaultCipherSuites = broker0.config().getList(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
    int defaultMaxMessageBytes = broker0.config().messageMaxBytes();
    Map<ConfigResource, Config> newConfigs = Collections.singletonMap(
        configResource,
        new Config(asList(
            new ConfigEntry(KafkaConfig.MessageMaxBytesProp(), "10000"),
            new ConfigEntry(KafkaConfig.AutoCreateTopicsEnableProp(), "true"),
            new ConfigEntry(SslConfigs.SSL_CIPHER_SUITES_CONFIG, "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")))
    );

    // Verify that tenants cannot update dynamic broker configs using AlterConfigs
    // Note: the `ssl.cipher.suites` property is _not_ prefixed, since the multi-tenant
    // interceptor will _add_ the "external" listener prefix.
    Throwable exceptionCause = assertThrows(ExecutionException.class, () ->
        tenantAdminClient.alterConfigs(newConfigs).all().get()).getCause();
    assertEquals(PolicyViolationException.class, exceptionCause.getClass());

    assertEquals(defaultMaxMessageBytes, broker0.config().messageMaxBytes().intValue());
    assertEquals(false, broker0.config().autoCreateTopicsEnable());
    assertEquals(defaultCipherSuites, broker0.config().get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));

    // Verify that tenants cannot update dynamic broker configs using incrementalAlterConfigs
    Map<ConfigResource, Collection<AlterConfigOp>> newConfigs2 = Collections.singletonMap(
        configResource, asList(
            new AlterConfigOp(new ConfigEntry(KafkaConfig.MessageMaxBytesProp(), "15000"),
                AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(KafkaConfig.AutoCreateTopicsEnableProp(), "true"),
                AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(SslConfigs.SSL_CIPHER_SUITES_CONFIG,
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"), AlterConfigOp.OpType.SET)));

    exceptionCause = assertThrows(ExecutionException.class, () ->
        tenantAdminClient.incrementalAlterConfigs(newConfigs2).all().get()).getCause();
    assertEquals(PolicyViolationException.class, exceptionCause.getClass());

    assertEquals(defaultMaxMessageBytes, broker0.config().messageMaxBytes().intValue());
    assertEquals(false, broker0.config().autoCreateTopicsEnable());
    assertEquals(defaultCipherSuites, broker0.config().get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));

    // Verify that users with access to internal listener can update dynamic broker configs
    // Note: we need to specify a listener prefix explicitly here, for the `ssl.cipher.suites` property,
    // since the interceptor is not configured for the internal listener.
    Map<ConfigResource, Config> newConfigsInternal = Collections.singletonMap(
        configResource,
            new Config(asList(
                new ConfigEntry(KafkaConfig.MessageMaxBytesProp(), "10000"),
                new ConfigEntry(KafkaConfig.AutoCreateTopicsEnableProp(), "true"),
                new ConfigEntry(externalListenerPrefix + SslConfigs.SSL_CIPHER_SUITES_CONFIG,
                    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")))
    );
    internalAdminClient.alterConfigs(newConfigsInternal).all().get();
    TestUtils.waitForCondition(() -> broker0.config().messageMaxBytes() == 10000,
        "Dynamic config not updated");
    TestUtils.waitForCondition(() -> broker0.config().autoCreateTopicsEnable(),
        "Dynamic config not updated");
    TestUtils.waitForCondition(() -> "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256".equals(
        sslCipherSuitesFromConfig(broker0.config(), externalListenerName)),
        "Dynamic config not updated");

    // Verify that users with access to internal listener can update dynamic broker configs
    Map<ConfigResource, Collection<AlterConfigOp>> newConfigs2Internal = Collections.singletonMap(
        configResource, asList(
            new AlterConfigOp(new ConfigEntry(KafkaConfig.MessageMaxBytesProp(), "15000"),
                AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(KafkaConfig.AutoCreateTopicsEnableProp(), "true"),
                AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(externalListenerPrefix + SslConfigs.SSL_CIPHER_SUITES_CONFIG,
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"), AlterConfigOp.OpType.SET)));

    internalAdminClient.incrementalAlterConfigs(newConfigs2Internal).all().get();
    TestUtils.waitForCondition(() -> broker0.config().messageMaxBytes() == 15000,
        "Dynamic config not updated");
    TestUtils.waitForCondition(() -> broker0.config().autoCreateTopicsEnable(),
        "Dynamic config not updated");
    TestUtils.waitForCondition(() -> "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384".equals(
        sslCipherSuitesFromConfig(broker0.config(), externalListenerName)),
        "Dynamic config not updated");

    // Verify that users with access to internal listener cannot update immutable broker configs
    Map<ConfigResource, Config> invalidConfigs = Collections.singletonMap(
        configResource,
        new Config(Collections.singleton(new ConfigEntry(KafkaConfig.BrokerIdProp(), "20")))
    );
    exceptionCause = assertThrows(ExecutionException.class, () ->
        internalAdminClient.alterConfigs(invalidConfigs).all().get()).getCause();
    assertEquals(InvalidRequestException.class, exceptionCause.getClass());

    assertEquals(0, broker0.config().brokerId());
  }

  /**
   * Alter broker configs should be rejected via the external listener when
   * confluent.alter.cluster.configs=true.
   */
  @Test
  public void testAlterBrokerConfigsWhenConfigEnabled() throws Exception {
    Properties brokerProps = brokerProps();
    brokerProps.put(ClusterPolicyConfig.ALTER_ENABLE_CONFIG, "true");
    createPhysicalAndLogicalClusters(brokerProps);

    AdminClient tenantAdminClient = testHarness.createAdminClient(logicalCluster1.adminUser());
    AdminClient internalAdminClient = physicalCluster.superAdminClient();
    expectAlterBrokerConfigsViaExternalListenerRejected(tenantAdminClient, internalAdminClient,
        new ConfigResource(Type.BROKER, "0"));
  }

  /**
   * Alter cluster (resourceName == "") configs should be allowed via the external listener when
   * confluent.alter.cluster.configs=true.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testAlterClusterConfigsWhenConfigEnabled() throws Exception {
    Properties brokerProps = brokerProps();
    brokerProps.put(ClusterPolicyConfig.ALTER_ENABLE_CONFIG, "true");
    createPhysicalAndLogicalClusters(brokerProps);

    AdminClient tenantAdminClient = testHarness.createAdminClient(logicalCluster1.adminUser());
    AdminClient internalAdminClient = physicalCluster.superAdminClient();
    ConfigResource configResource = new ConfigResource(Type.BROKER, "");
    String sslCipherSuitesConfig = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";

    // Verify that dynamic broker configs can be updated via the external listener.
    // Note: the `ssl.cipher.suites` property is _not_ prefixed, since the multi-tenant
    // interceptor will _add_ the "external" listener prefix.
    KafkaServer broker0 = physicalCluster.kafkaCluster().kafkas().get(0).kafkaServer();
    long retentionMs0 = 3_600_000;
    Map<ConfigResource, Config> newConfigs = Collections.singletonMap(
        configResource,
        new Config(asList(
            new ConfigEntry(KafkaConfig.LogRetentionTimeMillisProp(), String.valueOf(retentionMs0)),
            new ConfigEntry(KafkaConfig.AutoCreateTopicsEnableProp(), "true"),
            new ConfigEntry(KafkaConfig.SslCipherSuitesProp(), sslCipherSuitesConfig)))
    );

    tenantAdminClient.alterConfigs(newConfigs).all().get();
    TestUtils.waitForCondition(() -> broker0.config().logRetentionTimeMillis() == retentionMs0,
        "Dynamic config not updated");
    TestUtils.waitForCondition(() -> broker0.config().autoCreateTopicsEnable(),
        "Dynamic config not updated");
    TestUtils.waitForCondition(() -> sslCipherSuitesConfig.equals(sslCipherSuitesFromConfig(broker0.config(), externalListenerName)),
        "Dynamic config for ssl-cipher-suites not updated.");

    String sslCipherSuitesConfig2 = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";

    long retentionMs1 = 3_600_001;
    int numPartitions = 2;
    Map<ConfigResource, Collection<AlterConfigOp>> newConfigs2 = Collections.singletonMap(
        configResource, asList(
            new AlterConfigOp(new ConfigEntry(KafkaConfig.LogRetentionTimeMillisProp(), String.valueOf(retentionMs1)),
                AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(KafkaConfig.NumPartitionsProp(), String.valueOf(numPartitions)),
                AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(KafkaConfig.AutoCreateTopicsEnableProp(), "false"),
                AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(KafkaConfig.SslCipherSuitesProp(),
                sslCipherSuitesConfig2),
                AlterConfigOp.OpType.SET)));

    tenantAdminClient.incrementalAlterConfigs(newConfigs2).all().get();
    TestUtils.waitForCondition(() -> broker0.config().logRetentionTimeMillis() == retentionMs1,
        "Dynamic config not updated");
    TestUtils.waitForCondition(() -> broker0.config().numPartitions() == numPartitions,
        "Dynamic config not updated");
    TestUtils.waitForCondition(() -> !broker0.config().autoCreateTopicsEnable(),
        "Dynamic config not updated");
    TestUtils.waitForCondition(() -> sslCipherSuitesConfig2.equals(sslCipherSuitesFromConfig(broker0.config(), externalListenerName)),
        "Dynamic config for ssl-cipher-suites not updated.");

    // Verify that users with access to internal listener can update dynamic broker configs
    // Note: we need to specify a listener prefix explicitly here, for the `ssl.cipher.suites` property,
    // since the interceptor is not configured for the internal listener.
    Map<ConfigResource, Config> newConfigsInternal = Collections.singletonMap(
        configResource,
        new Config(asList(
            new ConfigEntry(KafkaConfig.LogRetentionTimeMillisProp(), String.valueOf(retentionMs0)),
            new ConfigEntry(KafkaConfig.AutoCreateTopicsEnableProp(), "true"),
            new ConfigEntry(externalListenerPrefix + KafkaConfig.SslCipherSuitesProp(),
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")))
    );

    internalAdminClient.alterConfigs(newConfigsInternal).all().get();
    TestUtils.waitForCondition(() -> broker0.config().logRetentionTimeMillis() == retentionMs0,
        "Dynamic config not updated");
    TestUtils.waitForCondition(() -> broker0.config().autoCreateTopicsEnable(),
        "Dynamic config not updated");
    TestUtils.waitForCondition(() -> sslCipherSuitesConfig.equals(
        sslCipherSuitesFromConfig(broker0.config(), externalListenerName)),
        "Dynamic config for ssl-cipher-suites not updated.");

    Map<ConfigResource, Collection<AlterConfigOp>> newConfigs2Internal = Collections.singletonMap(
        configResource, asList(
            new AlterConfigOp(new ConfigEntry(KafkaConfig.LogRetentionTimeMillisProp(), String.valueOf(retentionMs1)),
                AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(KafkaConfig.AutoCreateTopicsEnableProp(), "false"),
                AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(externalListenerPrefix + KafkaConfig.SslCipherSuitesProp(),
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"),
                AlterConfigOp.OpType.SET)));

    internalAdminClient.incrementalAlterConfigs(newConfigs2Internal).all().get();
    TestUtils.waitForCondition(() -> broker0.config().logRetentionTimeMillis() == retentionMs1,
        "Dynamic config not updated");
    TestUtils.waitForCondition(() -> !broker0.config().autoCreateTopicsEnable(),
        "Dynamic config not updated");
    TestUtils.waitForCondition(() -> sslCipherSuitesConfig2.equals(
        sslCipherSuitesFromConfig(broker0.config(), externalListenerName)),
        "Dynamic config for ssl-cipher-suites not updated.");

    // Verify that stricter value validation is performed for the external listener.
    long retentionMs2 = 2_599_999;
    Map<ConfigResource, Collection<AlterConfigOp>> newConfigs3 = Collections.singletonMap(
        configResource, asList(
            new AlterConfigOp(new ConfigEntry(KafkaConfig.LogRetentionTimeMillisProp(),
                String.valueOf(retentionMs2)),
                OpType.SET)));

    Throwable exceptionCause = assertThrows(ExecutionException.class, () ->
        tenantAdminClient.incrementalAlterConfigs(newConfigs3).all().get()).getCause();
    assertEquals(PolicyViolationException.class, exceptionCause.getClass());
    assertEquals(retentionMs1, broker0.config().logRetentionTimeMillis());

    // Verify that config not whitelisted cannot be updated via the external listener
    int defaultMaxMessageBytes = broker0.config().messageMaxBytes();
    Map<ConfigResource, Collection<AlterConfigOp>> newConfigs4 = Collections.singletonMap(
        configResource, asList(
            new AlterConfigOp(new ConfigEntry(KafkaConfig.MessageMaxBytesProp(),
                "500000"), OpType.SET)));

    exceptionCause = assertThrows(ExecutionException.class, () ->
        tenantAdminClient.incrementalAlterConfigs(newConfigs4).all().get()).getCause();
    assertEquals(PolicyViolationException.class, exceptionCause.getClass());
    assertEquals(defaultMaxMessageBytes, broker0.config().messageMaxBytes().intValue());
  }

  private String sslCipherSuitesFromConfig(KafkaConfig kafkaConfig, ListenerName listenerName) {
    return (String) kafkaConfig.originals().get(listenerName.configPrefix() + KafkaConfig.SslCipherSuitesProp());
  }

  @Test
  public void testAcls() throws Throwable {
    createPhysicalAndLogicalClusters();

    AdminClient adminClient1 = testHarness.createAdminClient(logicalCluster1.adminUser());
    AdminClient adminClient2 = testHarness.createAdminClient(logicalCluster2.adminUser());

    assertEquals(Collections.emptySet(), describeAllAcls(adminClient1));

    List<ResourceType> resourceTypes = asList(
        ResourceType.TOPIC,
        ResourceType.GROUP,
        ResourceType.TRANSACTIONAL_ID
    );
    Set<AclBinding> acls = new HashSet<>();
    resourceTypes.forEach(resourceType ->
      acls.add(new AclBinding(
          new ResourcePattern(resourceType, "test.resource", PatternType.LITERAL),
          new AccessControlEntry(logicalCluster1.user(11).unprefixedKafkaPrincipal().toString(), "*",
              AclOperation.WRITE, AclPermissionType.ALLOW))));
    resourceTypes.forEach(resourceType ->
      acls.add(new AclBinding(
          new ResourcePattern(resourceType, "test.", PatternType.PREFIXED),
          new AccessControlEntry(logicalCluster1.user(12).unprefixedKafkaPrincipal().toString(), "*",
              AclOperation.READ, AclPermissionType.ALLOW))));
    resourceTypes.forEach(resourceType ->
      acls.add(new AclBinding(
          new ResourcePattern(resourceType, "*", PatternType.LITERAL),
          new AccessControlEntry(logicalCluster1.user(11).unprefixedKafkaPrincipal().toString(), "*",
              AclOperation.DESCRIBE, AclPermissionType.ALLOW))));

    adminClient1.createAcls(acls).all().get();
    assertEquals(acls, describeAllAcls(adminClient1));

    assertEquals(Collections.emptySet(), describeAllAcls(adminClient2));
    adminClient2.createAcls(acls).all().get();
    assertEquals(acls, describeAllAcls(adminClient2));


    adminClient2.deleteAcls(Collections.singletonList(
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.ANY, "test", PatternType.PREFIXED),
            new AccessControlEntryFilter("User:*", "*", AclOperation.ANY, AclPermissionType.ANY)
    ))).all().get();
  }

  private Set<AclBinding> describeAllAcls(AdminClient adminClient) throws Exception {
    Collection<AclBinding> acls = adminClient.describeAcls(new AclBindingFilter(
        new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
        new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY)
    )).values().get();
    return new HashSet<>(acls);
  }
}
