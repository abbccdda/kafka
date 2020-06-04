// (Copyright) [2020 - 2020] Confluent, Inc.
package org.apache.kafka.connect.runtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import static org.apache.kafka.connect.runtime.WorkerConfigDecorator.CONFLUENT_INJECT_INTO_CONNECTORS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfigDecorator.CONFLUENT_LICENSE_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfigDecorator.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfigDecorator.CONFLUENT_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfigDecorator.CONFLUENT_TOPIC_CONSUMER_PREFIX;
import static org.apache.kafka.connect.runtime.WorkerConfigDecorator.CONFLUENT_TOPIC_PREFIX;
import static org.apache.kafka.connect.runtime.WorkerConfigDecorator.CONFLUENT_TOPIC_PRODUCER_PREFIX;
import static org.apache.kafka.connect.runtime.WorkerConfigDecorator.CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class WorkerConfigDecoratorTest {

  private static final String CONFLUENT_TOPIC_CLIENT_ID = CONFLUENT_TOPIC_PREFIX + CommonClientConfigs.CLIENT_ID_CONFIG;
  private static final String PROVIDER = "vars";

  private Map<String, String> workerProps;
  private WorkerConfigDecorator decorator;
  private WorkerConfigTransformer transformer;
  @Mock private Connector connector;
  @Mock private Worker worker;
  private String connectorName;
  private Map<String, String> variableReplacements;
  private Map<String, String> connectorProps;
  @Mock private ConfigInfos mockInfos;
  private ConfigInfos undecoratedInfos;
  private ConfigInfos decoratedInfos;

  @Before
  public void beforeEach() {
    worker = PowerMock.createMock(Worker.class);
    connector = PowerMock.createMock(Connector.class);
    mockInfos = PowerMock.createMock(ConfigInfos.class);

    connectorName = "MyConnector";

    workerProps = new HashMap<>();
    workerProps.put(DistributedConfig.GROUP_ID_CONFIG, "connect-cluster");
    workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-configs");
    workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
    workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "connect-status");
    workerProps.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    workerProps.put(DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

    connectorProps = new HashMap<>();
    connectorProps.put(ConnectorConfig.NAME_CONFIG, connectorName);
    connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "org.apache.kafka.connect.mock.MockSourceConnector");

    variableReplacements = new HashMap<>();
    ConfigProvider provider = new SimpleProvider();
    transformer = new WorkerConfigTransformer(worker, Collections.singletonMap(PROVIDER, provider));
  }

  @Test
  public void shouldDecorateDistributedConfig() {
    ConfigDef configDef = new ConfigDef(DistributedConfig.configDef());
    assertLicensePropertiesNotDefined(configDef);

    ConfigDef decorated = WorkerConfigDecorator.decorateWorkerConfig(configDef, workerProps);
    assertNotSame(configDef, decorated);
    assertDistributedLicensePropertiesDefined(decorated);
  }

  @Test
  public void shouldDecorateStandaloneConfig() {
    workerProps.remove(DistributedConfig.CONFIG_TOPIC_CONFIG);
    workerProps.remove(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG);
    workerProps.remove(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG);
    workerProps.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, "offsets.dat");

    ConfigDef configDef = new ConfigDef(StandaloneConfig.configDef());
    assertLicensePropertiesNotDefined(configDef);

    ConfigDef decorated = WorkerConfigDecorator.decorateWorkerConfig(configDef, workerProps);
    assertNotSame(configDef, decorated);
    assertStandaloneLicensePropertiesDefined(decorated);
  }

  @Test
  public void shouldNotDecorateDistributedConfigIfFeatureDisabled() {
    workerProps.put(CONFLUENT_INJECT_INTO_CONNECTORS_CONFIG, "false");
    ConfigDef configDef = new ConfigDef(DistributedConfig.configDef());
    assertLicensePropertiesNotDefined(configDef);

    ConfigDef decorated = WorkerConfigDecorator.decorateWorkerConfig(configDef, workerProps);
    assertSame(configDef, decorated);
    assertLicensePropertiesNotDefined(decorated);
  }

  @Test
  public void shouldNotDecorateStandaloneConfigIfFeatureDisabled() {
    workerProps.remove(DistributedConfig.CONFIG_TOPIC_CONFIG);
    workerProps.remove(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG);
    workerProps.remove(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG);
    workerProps.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, "offsets.dat");
    workerProps.put(CONFLUENT_INJECT_INTO_CONNECTORS_CONFIG, "false");

    ConfigDef configDef = new ConfigDef(StandaloneConfig.configDef());
    assertLicensePropertiesNotDefined(configDef);

    ConfigDef decorated = WorkerConfigDecorator.decorateWorkerConfig(configDef, workerProps);
    assertSame(configDef, decorated);
    assertLicensePropertiesNotDefined(decorated);
  }

  @Test
  public void shouldFailToValidateDistributedConfigWithInvalidLicenseProperty() {
    workerProps.put("confluent.topic.replication.factor", "invalid");
    assertThrows(ConfigException.class, () -> new DistributedConfig(workerProps));
  }

  @Test
  public void shouldFailToValidateStandaloneConfigWithInvalidLicenseProperty() {
    workerProps.put("confluent.topic.replication.factor", "invalid");
    assertThrows(ConfigException.class, () -> new StandaloneConfig(workerProps));
  }

  @Test
  public void shouldFailToDecorateNullWorkerConfigDef() {
    assertThrows(NullPointerException.class,
            () -> WorkerConfigDecorator.decorateWorkerConfig(null, workerProps));
  }

  @Test
  public void shouldFailToDecorateWorkerConfigWithNullProperties() {
    ConfigDef configDef = new ConfigDef(DistributedConfig.configDef());
    assertThrows(NullPointerException.class, () -> WorkerConfigDecorator.decorateWorkerConfig(configDef, null));
  }

  @Test
  public void shouldFailToAddConfigKeysWhenOriginalConfigDefIsNull() {
    assertThrows(NullPointerException.class, () -> WorkerConfigDecorator.addWithPrefix(null, "prefix", WorkerConfig.baseConfigDef()));
  }

  @Test
  public void shouldFailToAddConfigKeysWhenTargetConfigDefIsNull() {
    assertThrows(NullPointerException.class, () -> WorkerConfigDecorator.addWithPrefix(ProducerConfig.configDef(), "prefix", null));
  }

  @Test
  public void shouldFailToAddConfigKeysWhenPrefixIsNull() {
    assertThrows(NullPointerException.class, () -> WorkerConfigDecorator.addWithPrefix(ProducerConfig.configDef(), null, WorkerConfig.baseConfigDef()));
  }

  @Test
  public void shouldFailToInitializeDecoratorWithNullWorkerConfig() {
    assertNotNull(transformer);
    assertThrows(NullPointerException.class, () -> WorkerConfigDecorator.initialize(null, transformer));
  }

  @Test
  public void shouldFailToInitializeDecoratorWithNullTransformer() {
    DistributedConfig config = new DistributedConfig(workerProps);
    assertThrows(NullPointerException.class, () -> WorkerConfigDecorator.initialize(config, null));
  }

  @Test
  public void shouldInitializeDecoratorWithDistributedConfigAndTransformer() {
    DistributedConfig config = new DistributedConfig(workerProps);
    assertNotNull(transformer);
    assertNotNull(WorkerConfigDecorator.initialize(config, transformer));
  }

  @Test
  public void shouldInitializeDecoratorWithStandaloneConfigAndTransformer() {
    workerProps.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, "/tmp/offsetfile.json");
    StandaloneConfig config = new StandaloneConfig(workerProps);
    assertNotNull(transformer);
    assertNotNull(WorkerConfigDecorator.initialize(config, transformer));
  }

  @Test
  public void shouldDetectConnectorConfigDefDoesRequireLicenseProperties() {
    WorkerConfigDecorator.LicensePropertiesDecorator decorator = new WorkerConfigDecorator.LicensePropertiesDecorator();

    ConfigDef configDef = licensedConfigDef();
    assertTrue(decorator.requiresInjectedConnectorProperties(configDef));

    configDef = unlicensedConfigDef();
    configDef.define(CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, Type.STRING, "", Importance.HIGH, "");
    assertTrue(decorator.requiresInjectedConnectorProperties(configDef));

    configDef = unlicensedConfigDef();
    configDef.define(CONFLUENT_LICENSE_CONFIG, Type.STRING, "", Importance.HIGH, "");
    assertTrue(decorator.requiresInjectedConnectorProperties(configDef));
  }

  @Test
  public void shouldDetectConnectorConfigDefDoesNotRequireLicenseProperties() {
    WorkerConfigDecorator.LicensePropertiesDecorator decorator = new WorkerConfigDecorator.LicensePropertiesDecorator();
    assertFalse(decorator.requiresInjectedConnectorProperties(unlicensedConfigDef()));
  }

  @Test
  public void shouldDetermineThatPropertiesDoesNotContainLicenseRelatedProperties() {
    WorkerConfigDecorator.LicensePropertiesDecorator decorator = new WorkerConfigDecorator.LicensePropertiesDecorator();
    assertFalse(decorator.alreadyHasInjectedConnectorProperties(unlicensedProperties()));
  }

  @Test
  public void shouldDetermineThatPropertiesDoesContainLicenseRelatedProperties() {
    WorkerConfigDecorator.LicensePropertiesDecorator decorator = new WorkerConfigDecorator.LicensePropertiesDecorator();
    assertTrue(decorator.alreadyHasInjectedConnectorProperties(licensedProperties()));
  }

  @Test
  public void shouldAddInheritedTopLevelProducerAndConsumerProperties() {
    workerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    workerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "3");
    workerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    createDecorator();

    Map<String, String> injected = injected(licensedConfigDef());
    // bootstrap is common to producers and consumers, so it should have `confluent.topic.` prefix
    assertEquals("localhost:9095", injected.get(CONFLUENT_TOPIC_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    // producer-specific properties should have `confluent.topic.producer.` prefix
    assertEquals("3", injected.get(CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.BATCH_SIZE_CONFIG));
    // consumer-specific properties should have `confluent.topic.producer.` prefix
    assertEquals("false", injected.get(CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));

    assertNoUnexpectedProducerAndConsumerProps(injected);
  }

  @Test
  public void shouldAddInheritedTopLevelProducerAndConsumerPropertiesWithDifferentBootstrapServers() {
    workerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    workerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "3");
    workerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    workerProps.put(CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, "localhost:9096");
    workerProps.put(CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9097");
    createDecorator();

    Map<String, String> injected = injected(licensedConfigDef());
    // bootstrap is common to producers and consumers, so it should have `confluent.topic.` prefix
    assertEquals("localhost:9096", injected.get(CONFLUENT_TOPIC_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    // producer-specific properties should have `confluent.topic.producer.` prefix
    assertEquals("3", injected.get(CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.BATCH_SIZE_CONFIG));
    // consumer-specific properties should have `confluent.topic.producer.` prefix
    assertEquals("false", injected.get(CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));

    assertNoUnexpectedProducerAndConsumerProps(injected);
  }

  @Test
  public void shouldComputeInjectedPropertiesFromInternalClientPropertiesAndMinimalWorkerProps() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    createDecorator();

    Map<String, String> injected = injected(licensedConfigDef());
    // bootstrap is common to producers and consumers, so it should have `confluent.topic.` prefix
    // and should not have `confluent.topic.producer.` or `confluent.topic.consumer.` prefix
    assertEquals("localhost:9095", injected.get(CONFLUENT_TOPIC_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertFalse(injected.containsKey(CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertFalse(injected.containsKey(CONFLUENT_TOPIC_CONSUMER_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

    // these should exist but should match the default
    assertEquals("", injected.get(CONFLUENT_LICENSE_CONFIG));
    assertEquals("_confluent-command", injected.get(CONFLUENT_TOPIC_CONFIG));
    assertEquals("3", injected.get(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG));

    assertNoUnexpectedProducerAndConsumerProps(injected);
  }

  @Test
  public void shouldNotDecorateConnectorConfigUsingNullConnectorName() {
    createDecorator();
    assertSame(connectorProps, decorator.decorateConnectorConfig(null, connector, licensedConfigDef(), connectorProps));
  }

  @Test(expected = NullPointerException.class)
  public void shouldFailToDecorateConnectorConfigUsingNullConnector() {
    createDecorator();
    decorator.decorateConnectorConfig(connectorName, null, licensedConfigDef(), connectorProps);
  }

  @Test(expected = NullPointerException.class)
  public void shouldFailToDecorateConnectorConfigUsingNullConnectorConfigDef() {
    createDecorator();
    decorator.decorateConnectorConfig(connectorName, connector, null, connectorProps);
  }

  @Test(expected = NullPointerException.class)
  public void shouldFailToDecorateConnectorConfigUsingNullConnectorConfig() {
    createDecorator();
    decorator.decorateConnectorConfig(connectorName, connector, licensedConfigDef(), null);
  }

  @Test
  public void shouldNotDecorateLicensedConnectorConfigWhenFeatureIsDisabled() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    workerProps.put(CONFLUENT_INJECT_INTO_CONNECTORS_CONFIG, "false");
    createDecorator();

    Map<String, String> injected = injected(licensedConfigDef());
    assertTrue(injected.isEmpty());
  }

  @Test
  public void shouldDecorateLicensedConnectorConfigWithLicensePropertiesFromWorkerConfig() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    createDecorator();

    Map<String, String> decorated = decorateConnector(licensedConfigDef());
    // bootstrap is common to producers and consumers, so it should have `confluent.topic.` prefix
    // and should not have `confluent.topic.producer.` or `confluent.topic.consumer.` prefix
    assertEquals("localhost:9095", decorated.get(CONFLUENT_TOPIC_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertFalse(decorated.containsKey(CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertFalse(decorated.containsKey(CONFLUENT_TOPIC_CONSUMER_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

    // these should exist but should match the default
    assertEquals("", decorated.get(CONFLUENT_LICENSE_CONFIG));
    assertEquals("_confluent-command", decorated.get(CONFLUENT_TOPIC_CONFIG));
    assertEquals("3", decorated.get(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG));

    assertNoUnexpectedProducerAndConsumerProps(decorated);
  }

  @Test
  public void shouldNotDecorateLicensedConnectorConfigUsingLicensePropertiesInConnector() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    createDecorator();

    connectorProps.put("confluent.license", "secret");
    connectorProps.put("confluent.topic.bootstrap.servers", "localhost:9095");
    assertFalse(connectorProps.containsKey(CONFLUENT_TOPIC_CONFIG));
    assertFalse(connectorProps.containsKey(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG));

    Map<String, String> decorated = decorateConnector(licensedConfigDef());
    assertEquals("secret", decorated.get(CONFLUENT_LICENSE_CONFIG));
    assertEquals("localhost:9095", decorated.get(CONFLUENT_TOPIC_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(connectorProps, decorated);

    assertNoUnexpectedProducerAndConsumerProps(decorated);
  }

  @Test
  public void shouldNotDecorateUnlicensedConnectorConfigThatDoesNotUseLicenseProperties() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    createDecorator();

    Map<String, String> decorated = decorateConnector(unlicensedConfigDef());
    assertEquals(connectorProps, decorated);
  }

  @Test(expected = NullPointerException.class)
  public void shouldFailToDecorateValidationResultUsingNullConnectorName() {
    decorator.decorateValidationResult(null, connector, licensedConfigDef(), connectorProps, mockInfos);
  }

  @Test(expected = NullPointerException.class)
  public void shouldFailToDecorateValidationResultUsingNullConnector() {
    decorator.decorateValidationResult(connectorName, null, licensedConfigDef(), connectorProps, mockInfos);
  }

  @Test(expected = NullPointerException.class)
  public void shouldFailToDecorateValidationResultUsingNullConnectorConfigDef() {
    decorator.decorateValidationResult(connectorName, connector, null, connectorProps, mockInfos);
  }

  @Test(expected = NullPointerException.class)
  public void shouldFailToDecorateValidationResultUsingNullConnectorConfig() {
    decorator.decorateValidationResult(connectorName, connector, licensedConfigDef(), null, mockInfos);
  }

  @Test(expected = NullPointerException.class)
  public void shouldFailToDecorateValidationResultUsingNullValidationResult() {
    decorator.decorateValidationResult(connectorName, connector, licensedConfigDef(), connectorProps, null);
  }

  @Test
  public void shouldDecorateValidationResultsForLicensedConnectorByRemovingLicenseRelatedInfo() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    createDecorator();

    validate(licensedConfigDef());
    assertNotNull(undecoratedInfos);
    assertNotNull(decoratedInfos);
    assertLicenseValidationResults(undecoratedInfos);
    assertNoLicenseValidationResults(decoratedInfos);
  }

  @Test
  public void shouldNotDecorateValidationResultsForUnlicensedConnector() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    createDecorator();

    validate(unlicensedConfigDef());
    assertNotNull(undecoratedInfos);
    assertNotNull(decoratedInfos);
    assertNoLicenseValidationResults(undecoratedInfos);
    assertNoLicenseValidationResults(decoratedInfos);
    assertUnmodifiedLicenseValidationResults();
  }

  @Test
  public void shouldNotDecorateValidationResultsForLicensedConnectorContainingLicenseRelatedInfo() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    createDecorator();

    connectorProps.put("confluent.topic", "my-license-topic");
    validate(licensedConfigDef());
    assertNotNull(undecoratedInfos);
    assertNotNull(decoratedInfos);
    assertLicenseValidationResults(undecoratedInfos);
    assertLicenseValidationResults(decoratedInfos);
    assertUnmodifiedLicenseValidationResults();
  }

  @Test
  public void shouldNotDecorateValidationResultsForUnlicensedConnectorContainingNoLicenseRelatedInfo() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    createDecorator();

    validate(unlicensedConfigDef());
    assertNotNull(undecoratedInfos);
    assertNotNull(decoratedInfos);
    assertNoLicenseValidationResults(undecoratedInfos);
    assertNoLicenseValidationResults(decoratedInfos);
    assertUnmodifiedLicenseValidationResults();
  }

  @Test
  public void shouldNotDecorateLicensedValidationResultsWhenFeatureIsDisabled() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    workerProps.put(CONFLUENT_INJECT_INTO_CONNECTORS_CONFIG, "false");
    createDecorator();

    validate(licensedConfigDef());
    assertNotNull(undecoratedInfos);
    assertNotNull(decoratedInfos);
    assertLicenseValidationResults(undecoratedInfos);
    assertLicenseValidationResults(decoratedInfos);
    assertUnmodifiedLicenseValidationResults();
  }

  @Test
  public void shouldNotDecorateValidationResultsWhenFeatureIsDisabled() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    workerProps.put(CONFLUENT_INJECT_INTO_CONNECTORS_CONFIG, "false");
    createDecorator();

    validate(licensedConfigDef());
    assertNotNull(undecoratedInfos);
    assertNotNull(decoratedInfos);
    assertLicenseValidationResults(undecoratedInfos);
    assertLicenseValidationResults(decoratedInfos);
    assertUnmodifiedLicenseValidationResults();
  }

  @Test
  public void shouldInjectWorkerLicensePropertiesWithDefaultLicenseProperties() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    // Add property used by both consumer and producer
    workerProps.put("confluent.topic.retry.backoff.ms", "1000");
    // Add property used only by producer
    workerProps.put("confluent.topic.producer.max.request.size", "20000");
    workerProps.put("confluent.topic.producer.retry.backoff.ms", "3000");
    // Add property used only by consumer
    workerProps.put("confluent.topic.consumer.fetch.min.bytes", "2");
    workerProps.put("confluent.topic.consumer.retry.backoff.ms", "4000");
    // Add property used by nothing
    workerProps.put("confluent.topic.argle", "extra");

    DistributedConfig workerConfig = new DistributedConfig(workerProps);
    ConfigDef connectorConfig = licensedConfigDef();

    Map<String, String> injected = injectLicenseProperties(workerConfig, connectorConfig);
    assertEquals("", injected.get("confluent.license"));
    assertEquals("_confluent-command", injected.get("confluent.topic"));
    assertEquals("localhost:9095", injected.get("confluent.topic.bootstrap.servers"));
    assertEquals("3", injected.get("confluent.topic.replication.factor"));
    assertEquals("1000", injected.get("confluent.topic.retry.backoff.ms"));
    assertEquals("3000", injected.get("confluent.topic.producer.retry.backoff.ms"));
    assertEquals("4000", injected.get("confluent.topic.consumer.retry.backoff.ms"));
    assertEquals("20000", injected.get("confluent.topic.producer.max.request.size"));
    assertEquals("2", injected.get("confluent.topic.consumer.fetch.min.bytes"));
    assertEquals("extra", injected.get("confluent.topic.argle"));
  }

  @Test
  public void shouldInjectWorkerLicensePropertiesWithSpecifiedLicenseProperties() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    // Override the default license properties in the worker
    workerProps.put("confluent.license", "abcdef");
    workerProps.put("confluent.topic", "new-license-topic");
    workerProps.put("confluent.topic.bootstrap.servers", "other:9095");
    workerProps.put("confluent.topic.replication.factor", "2");
    // Add property used by both consumer and producer
    workerProps.put("confluent.topic.retry.backoff.ms", "1000");
    // Add property used only by producer
    workerProps.put("confluent.topic.producer.max.request.size", "20000");
    workerProps.put("confluent.topic.producer.retry.backoff.ms", "3000");
    // Add property used only by consumer
    workerProps.put("confluent.topic.consumer.fetch.min.bytes", "2");
    workerProps.put("confluent.topic.consumer.retry.backoff.ms", "4000");
    // Add property used by nothing
    workerProps.put("confluent.topic.argle", "extra");

    DistributedConfig workerConfig = new DistributedConfig(workerProps);
    ConfigDef connectorConfig = licensedConfigDef();

    Map<String, String> injected = injectLicenseProperties(workerConfig, connectorConfig);
    assertEquals("abcdef", injected.get("confluent.license"));
    assertEquals("new-license-topic", injected.get("confluent.topic"));
    assertEquals("other:9095", injected.get("confluent.topic.bootstrap.servers"));
    assertEquals("2", injected.get("confluent.topic.replication.factor"));
    assertEquals("1000", injected.get("confluent.topic.retry.backoff.ms"));
    assertEquals("3000", injected.get("confluent.topic.producer.retry.backoff.ms"));
    assertEquals("4000", injected.get("confluent.topic.consumer.retry.backoff.ms"));
    assertEquals("20000", injected.get("confluent.topic.producer.max.request.size"));
    assertEquals("2", injected.get("confluent.topic.consumer.fetch.min.bytes"));
    assertEquals("extra", injected.get("confluent.topic.argle"));
  }

  @Test
  public void shouldInjectWorkerLicensePropertiesWithSpecifiedLicensePropertiesAndClientOverrides() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    // Override the default license properties in the worker
    workerProps.put("confluent.topic.bootstrap.servers", "other:9095");
    // Add property used only by producer
    workerProps.put("confluent.topic.producer.max.request.size", "20000");
    workerProps.put("confluent.topic.producer.retry.backoff.ms", "3000");
    // Add property used only by consumer
    workerProps.put("confluent.topic.consumer.fetch.min.bytes", "2");
    // Add property used by nothing
    workerProps.put("confluent.topic.argle", "extra");
    // Add properties that should be removed
    workerProps.put("confluent.topic.producer.bootstrap.servers", "incorrect");
    workerProps.put("confluent.topic.producer.retries", "incorrect");
    workerProps.put("confluent.topic.producer.key.serializer", "incorrect");
    workerProps.put("confluent.topic.producer.value.serializer", "incorrect");
    workerProps.put("confluent.topic.consumer.bootstrap.servers", "incorrect");
    workerProps.put("confluent.topic.consumer.key.deserializer", "incorrect");
    workerProps.put("confluent.topic.consumer.value.deserializer", "incorrect");

    DistributedConfig workerConfig = new DistributedConfig(workerProps);
    ConfigDef connectorConfig = licensedConfigDef();

    Map<String, String> injected = injectLicenseProperties(workerConfig, connectorConfig);
    assertEquals("", injected.get("confluent.license"));
    assertEquals("_confluent-command", injected.get("confluent.topic"));
    assertEquals("other:9095", injected.get("confluent.topic.bootstrap.servers"));
    assertEquals("3", injected.get("confluent.topic.replication.factor"));
    assertEquals("3000", injected.get("confluent.topic.producer.retry.backoff.ms"));
    assertEquals("20000", injected.get("confluent.topic.producer.max.request.size"));
    assertEquals("2", injected.get("confluent.topic.consumer.fetch.min.bytes"));
    assertEquals("extra", injected.get("confluent.topic.argle"));
    assertFalse(injected.containsKey("confluent.topic.producer.bootstrap.servers"));
    assertFalse(injected.containsKey("confluent.topic.producer.retries"));
    assertFalse(injected.containsKey("confluent.topic.producer.key.serializer"));
    assertFalse(injected.containsKey("confluent.topic.producer.value.serializer"));
    assertFalse(injected.containsKey("confluent.topic.consumer.bootstrap.servers"));
    assertFalse(injected.containsKey("confluent.topic.consumer.key.deserializer"));
    assertFalse(injected.containsKey("confluent.topic.consumer.value.deserializer"));
  }

  @Test
  public void shouldNotInjectWorkerLicensePropertiesIntoNonLicensedConnector() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    // Add property used by both consumer and producer
    workerProps.put("confluent.topic.retry.backoff.ms", "1000");
    // Add property used only by producer
    workerProps.put("confluent.topic.producer.max.request.size", "20000");
    workerProps.put("confluent.topic.producer.retry.backoff.ms", "3000");
    // Add property used only by consumer
    workerProps.put("confluent.topic.consumer.fetch.min.bytes", "2");
    workerProps.put("confluent.topic.consumer.retry.backoff.ms", "4000");
    // Add property used by nothing
    workerProps.put("confluent.topic.argle", "extra");

    DistributedConfig workerConfig = new DistributedConfig(workerProps);
    ConfigDef connectorConfig = unlicensedConfigDef();

    Map<String, String> injected = injectLicenseProperties(workerConfig, connectorConfig);
    assertTrue(injected.isEmpty());
  }

  @Test
  public void shouldNotInjectWorkerLicensePropertiesWhenFeatureIsNotEnabled() {
    workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
    workerProps.put(CONFLUENT_INJECT_INTO_CONNECTORS_CONFIG, "false");

    // Add property used by both consumer and producer
    workerProps.put("confluent.topic.retry.backoff.ms", "1000");
    // Add property used only by producer
    workerProps.put("confluent.topic.producer.max.request.size", "20000");
    workerProps.put("confluent.topic.producer.retry.backoff.ms", "3000");
    // Add property used only by consumer
    workerProps.put("confluent.topic.consumer.fetch.min.bytes", "2");
    workerProps.put("confluent.topic.consumer.retry.backoff.ms", "4000");
    // Add property used by nothing
    workerProps.put("confluent.topic.argle", "extra");

    DistributedConfig workerConfig = new DistributedConfig(workerProps);
    ConfigDef connectorConfig = licensedConfigDef();

    Map<String, String> injected = injectLicenseProperties(workerConfig, connectorConfig);
    assertTrue(injected.isEmpty());
  }

  protected Map<String, String> injectLicenseProperties(WorkerConfig workerConfig, ConfigDef connectorConfigDef) {
    WorkerConfigDecorator.LicensePropertiesDecorator licenseDecorator = new WorkerConfigDecorator.LicensePropertiesDecorator();
    return licenseDecorator.injectedConnectorProperties(workerConfig, connectorName, connectorConfigDef, connectorProps);
  }

  protected void createDecorator() {
    DistributedConfig config = new DistributedConfig(workerProps);
    decorator = WorkerConfigDecorator.initialize(config, transformer);
  }

  protected Map<String, String> decorateConnector(ConfigDef configDef) {
    return decorator.decorateConnectorConfig(connectorName, connector, configDef, connectorProps);
  }

  protected Map<String, String> injected(ConfigDef configDef) {
    Map<String, String> result = decorateConnector(configDef);
    connectorProps.keySet().forEach(result::remove);
    return result;
  }

  protected ConfigInfos validate(ConfigDef configDef) {
    Map<String, String> connectorProps = decorateConnector(configDef);
    List<ConfigValue> configValues = configDef.validate(connectorProps);
    Config validationResults = new Config(configValues);
    String connType = connector.getClass().getName();
    ConfigInfos results = AbstractHerder.generateResult(connType, configDef.configKeys(), validationResults.configValues(), new ArrayList<>(configDef.groups()));
    undecoratedInfos = new ConfigInfos(results.name(), results.errorCount(), new ArrayList<>(results.groups()), new ArrayList<>(results.values()));
    decoratedInfos = decorator.decorateValidationResult(connectorName, connector, configDef, this.connectorProps, results);
    return decoratedInfos;
  }

  protected ConfigDef licensedConfigDef() {
    ConfigDef configDef = unlicensedConfigDef();
    configDef.define(CONFLUENT_LICENSE_CONFIG, Type.STRING, "", Importance.HIGH, "")
             .define(CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, Type.STRING, "", Importance.HIGH, "")
             .define(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG, Type.INT, 3, Importance.HIGH, "");
    return configDef;
  }

  protected ConfigDef unlicensedConfigDef() {
    ConfigDef configDef = new ConfigDef();
    configDef.define("max.something", Type.INT, 30, Importance.HIGH, "")
             .define("foo.other", Type.STRING, "no", Importance.HIGH, "");
    return configDef;
  }

  protected Map<String, String> licensedProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(CONFLUENT_LICENSE_CONFIG, "abcdefghijk");
    return props;
  }

  protected Map<String, String> unlicensedProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("max.something", "20");
    props.put("foo.other", "yes");
    return props;
  }

  protected void assertUnmodifiedLicenseValidationResults() {
    boolean found = findLicenseValidationConfigInfos(undecoratedInfos);
    assertEquals(found, findLicenseValidationConfigInfos(decoratedInfos));
  }

  protected void assertLicenseValidationResults(ConfigInfos infos) {
    assertTrue(findLicenseValidationConfigInfos(infos));
  }

  protected void assertNoLicenseValidationResults(ConfigInfos infos) {
    assertFalse(findLicenseValidationConfigInfos(infos));
  }

  protected boolean findLicenseValidationConfigInfos(ConfigInfos infos) {
    String regex = "confluent.(topic|license).*";
    boolean found = infos.values().stream().anyMatch(i -> i.configKey().name().matches(regex));
    return found;
  }

  protected void assertDistributedLicensePropertiesDefined(ConfigDef defn) {
    assertDefn(defn, CONFLUENT_TOPIC_CLIENT_ID, Type.STRING, "", Importance.MEDIUM, "", 105);
  }

  protected void assertStandaloneLicensePropertiesDefined(ConfigDef defn) {
    assertDefn(defn, CONFLUENT_TOPIC_CLIENT_ID, Type.STRING, "", Importance.MEDIUM, "", 45);
  }

  protected void assertLicensePropertiesDefined(ConfigDef defn) {
    assertDefn(defn, CONFLUENT_LICENSE_CONFIG, Type.PASSWORD, "", Importance.MEDIUM);
    assertDefn(defn, CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, Type.LIST, Collections.emptyList(), Importance.MEDIUM);
    assertDefn(defn, CONFLUENT_TOPIC_CONFIG, Type.STRING, "", Importance.LOW);
    assertDefn(defn, CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG, Type.INT, 3, Importance.LOW);
    // These should not be defined for the license manager
    assertNotDefined(defn, CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.RETRIES_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
  }

  protected void assertLicensePropertiesNotDefined(ConfigDef defn) {
    assertNotDefined(defn, CONFLUENT_LICENSE_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_CLIENT_ID);
    // These should not be defined for the license manager
    assertNotDefined(defn, CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.RETRIES_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
    assertNotDefined(defn, CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
  }

  protected void assertDefn(ConfigDef defn, String name, Type type, Object defaultValue, Importance importance) {
    assertDefn(defn, name, type, defaultValue, importance, null, -1);
  }

  protected void assertDefn(ConfigDef defn, String name, Type type, Object defaultValue, Importance importance, String group, int orderInGroup) {
    assertNotNull(defn);
    ConfigDef.ConfigKey key = defn.configKeys().get(name);
    assertNotNull("Missing expected config key '" + name + "'", key);
    assertTrue("Missing expected config key name '" + name + "'", defn.names().contains(name));
    assertEquals(type, key.type);
    if (key.defaultValue instanceof Password) {
      assertEquals(name + " has different password", defaultValue, ((Password) key.defaultValue).value());
    } else {
      assertEquals(name + " has different default", defaultValue, key.defaultValue);
    }
    assertEquals(name + " has different importance", importance, key.importance);
    assertEquals(name + " has different group", group, key.group);
    assertEquals(name + " has different orderInGroup", orderInGroup, key.orderInGroup);
  }

  protected void assertNotDefined(ConfigDef defn, String name) {
    assertNotNull(defn);
    ConfigDef.ConfigKey key = defn.configKeys().get(name);
    assertNull(key);
    assertFalse(defn.names().contains(name));
  }

  protected void assertNoUnexpectedProducerAndConsumerProps(Map<String, String> props) {
    // Make sure the inherited properties never include these consumer-specific props
    assertNull(props.get(CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG));
    assertNull(props.get(CONFLUENT_TOPIC_CONSUMER_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    // Make sure the inherited properties never include these producer-specific props
    assertNull(props.get(CONFLUENT_TOPIC_PRODUCER_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
  }

  protected class SimpleProvider implements ConfigProvider {

    @Override
    public void configure(Map<String, ?> configs) {
      // do nothing
    }

    @Override
    public ConfigData get(String path) {
      return get(path, Collections.emptySet());
    }

    @Override
    public ConfigData get(
            String path,
            Set<String> keys
    ) {
      return new ConfigData(variableReplacements);
    }

    @Override
    public void close() throws IOException {
      // do nothing
    }
  }
}