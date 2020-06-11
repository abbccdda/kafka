// (Copyright) [2019 - 2019] Confluent, Inc.
package io.confluent.kafka.multitenant.integration.test;

import io.confluent.kafka.clients.plugins.auth.oauth.OAuthBearerLoginCallbackHandler;
import io.confluent.kafka.common.multitenant.oauth.OAuthBearerJwsToken;
import io.confluent.kafka.multitenant.MultiTenantPrincipalBuilder;
import io.confluent.kafka.multitenant.PhysicalClusterMetadata;
import io.confluent.kafka.multitenant.Utils;
import io.confluent.kafka.multitenant.authorizer.MultiTenantAuditLogConfig;
import io.confluent.kafka.multitenant.authorizer.MultiTenantAuthorizer;
import io.confluent.kafka.multitenant.integration.cluster.LogicalCluster;
import io.confluent.kafka.multitenant.integration.cluster.LogicalClusterUser;
import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;
import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;
import io.confluent.kafka.security.audit.event.ConfluentAuthenticationEvent;
import io.confluent.kafka.security.authorizer.MockAuditLogProvider;
import io.confluent.kafka.server.plugins.auth.oauth.OAuthBearerServerLoginCallbackHandler;
import io.confluent.kafka.server.plugins.auth.oauth.OAuthBearerValidatorCallbackHandler;
import io.confluent.kafka.server.plugins.auth.oauth.OAuthUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import kafka.admin.AclCommand;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.apache.kafka.server.audit.AuthenticationErrorInfo;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.confluent.kafka.multitenant.Utils.LC_META_ABC;
import static io.confluent.kafka.multitenant.Utils.initiatePhysicalClusterMetadata;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * An integration test testing OAUTHBEARER with an AdminClient
 */
@Category(IntegrationTest.class)
public class AdminClientIntegrationTest {
  private static final Logger log = LoggerFactory.getLogger(AdminClientIntegrationTest.class);

  private final String logicalClusterId = LC_META_ABC.logicalClusterId();
  private final Properties adminProperties = new Properties();
  {
    adminProperties.put("sasl.login.callback.handler.class", OAuthBearerLoginCallbackHandler.class.getName());
  }
  private IntegrationTestHarness testHarness;
  private OAuthUtils.JwsContainer jwsContainer;
  private final String[] allowedClusters = new String[] {logicalClusterId};
  private String brokerUUID;
  private PhysicalClusterMetadata metadata;
  private final String testTopic = "abcd";
  private final List<NewTopic> sampleTopics = Collections.singletonList(new NewTopic(testTopic, 3, (short) 1));
  private LogicalClusterUser testUser;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private void setUp() throws Exception {
    MockAuditLogProvider.reset();
    setUp(allowedClusters);
  }

  private void setUp(String[] allowedClusters) throws Exception {
    testHarness = new IntegrationTestHarness();

    final int serviceUserId = 1;
    String subject = serviceUserId + "";
    jwsContainer = OAuthUtils.setUpJws(100000, "Confluent", subject, allowedClusters);
    PhysicalCluster physicalCluster = testHarness.start(setUpMetadata(brokerProps()));


    final int adminUserId = 100;
    LogicalCluster logicalCluster = physicalCluster.createLogicalCluster(logicalClusterId, adminUserId, 1);
    testUser = logicalCluster.user(1);
    addAdminAcls();
  }

  private void addAdminAcls() {
    AclCommand.main(SecurityTestUtils.addTopicAclArgs(testHarness.zkConnect(), testUser.prefixedKafkaPrincipal(),
        testUser.withPrefix(testTopic), AclOperation.ALL, PatternType.LITERAL));
  }

  @After
  public void tearDown() throws Exception {
    testHarness.shutdown();
    metadata.close(brokerUUID);
  }

  private Properties setUpMetadata(Properties brokerProps) throws IOException, InterruptedException {
    brokerUUID = "uuid";
    final Map<String, Object> configs = new HashMap<>();
    configs.put("broker.session.uuid", brokerUUID);
    brokerProps.put("broker.session.uuid", brokerUUID);
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
            tempFolder.getRoot().getCanonicalPath());

    metadata = initiatePhysicalClusterMetadata(configs);

    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
    TestUtils.waitForCondition(
            () -> metadata.metadata(LC_META_ABC.logicalClusterId()) != null,
            "Expected metadata of new logical cluster to be present in metadata cache");

    return brokerProps;
  }

  private Properties brokerProps() {
    Properties props = new Properties();
    props.put("sasl.enabled.mechanisms", Collections.singletonList("OAUTHBEARER"));
    props.put("listener.name.external.oauthbearer.sasl.login.callback.handler." +
            "class", OAuthBearerServerLoginCallbackHandler.class.getName());
    props.put("listener.name.external.oauthbearer.sasl.server.callback" +
            ".handler.class", OAuthBearerValidatorCallbackHandler.class.getName());
    props.put("listener.name.external.oauthbearer.sasl.jaas.config",
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                    "publicKeyPath=\"" +
                    jwsContainer.getPublicKeyFile().toPath() +  "\";");
    props.put("client.quota.callback.class", TenantQuotaCallback.class.getName());
    props.put("listener.name.external.principal.builder.class", MultiTenantPrincipalBuilder.class.getName());
    props.put(ConfluentConfigs.ENABLE_AUTHENTICATION_AUDIT_LOGS, "true");
    props.put(KafkaConfig$.MODULE$.AuthorizerClassNameProp(), MultiTenantAuthorizer.class.getName());
    props.put(MultiTenantAuditLogConfig.MULTI_TENANT_AUDIT_LOGGER_ENABLE_CONFIG, "true");
    return props;
  }

  private String clientJaasConfig(String jwsToken, String allowedCluster) {
    return "org.apache.kafka.common.security." +
            "oauthbearer.OAuthBearerLoginModule Required token=\"" + jwsToken +
            "\" cluster=\"" + allowedCluster + "\";";
  }

  @Test
  public void testCorrectConfigurationAuthenticatesSuccessfully() throws Exception {
    setUp();
    AdminClient client = testHarness.createOAuthAdminClient(clientJaasConfig(jwsContainer.getJwsToken(), logicalClusterId), adminProperties);
    client.createTopics(sampleTopics).all().get();

    List<String> expectedTopics = sampleTopics.stream().map(NewTopic::name)
            .collect(Collectors.toList());
    assertTrue(client.listTopics().names().get().containsAll(expectedTopics));

    //Verify generated audit event
    MockAuditLogProvider auditLogProvider = MockAuditLogProvider.instance;
    ConfluentAuthenticationEvent authenticationEvent = (ConfluentAuthenticationEvent) auditLogProvider.lastAuthenticationEntry();

    assertEquals("User", authenticationEvent.principal().get().getPrincipalType());
    assertEquals("1", authenticationEvent.principal().get().getName());
    assertEquals(AuditEventStatus.SUCCESS, authenticationEvent.status());

    assertFalse(authenticationEvent.principal().get().toString().contains("tenantMetadata"));
    Assert.assertTrue(authenticationEvent.getScope().toString().contains("kafka-cluster=lkc-abc"));

    SaslAuthenticationContext authenticationContext = (SaslAuthenticationContext) authenticationEvent.authenticationContext();
    assertEquals("1", authenticationContext.server().getAuthorizationID());
  }

  @Test
  public void testAllowedClusterExtensionNotInTokenThrowsException() throws Exception {
    setUp();
    Class expectedException = SaslAuthenticationException.class;
    try {
      AdminClient client = testHarness.createOAuthAdminClient(clientJaasConfig(jwsContainer.getJwsToken(), "wrong"), adminProperties);
      client.createTopics(sampleTopics).all().get();
      fail(String.format("Expected admin command to throw a %s", expectedException));
    } catch (Exception e) {
      if (e.getCause().getClass() != expectedException)
        fail(String.format("Expected admin command to throw a %s but it threw a %s", expectedException, e.getCause().getClass()));
      log.info("Expected exception message: {}", e.getCause().getMessage());
    }
    //Verify generated auth failure audit event
    MockAuditLogProvider auditLogProvider = MockAuditLogProvider.instance;
    ConfluentAuthenticationEvent authenticationEvent = (ConfluentAuthenticationEvent) auditLogProvider.lastAuthenticationEntry();

    assertFalse(authenticationEvent.principal().isPresent());
    assertEquals(AuditEventStatus.UNAUTHENTICATED, authenticationEvent.status());
    Assert.assertTrue(authenticationEvent.getScope().toString().contains("kafka-cluster=wrong"));
    assertTrue(authenticationEvent.authenticationException().isPresent());

    AuthenticationException authenticationException = authenticationEvent.authenticationException().get();
    AuthenticationErrorInfo errorInfo = authenticationException.errorInfo();
    Assert.assertTrue(errorInfo.errorMessage().contains("logical cluster wrong is not part of the allowed clusters"));
    assertEquals("1", errorInfo.identifier());
    assertEquals("wrong", errorInfo.saslExtensions().get(OAuthBearerJwsToken.OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY));
  }

  @Test
  public void testAllowedClusterNotHostedOnBrokerThrowsException() throws Exception {
    setUp(new String[] {"other-cluster"}); // different cluster than hosted in metadata
    Class expectedException = SaslAuthenticationException.class;
    try {
      AdminClient client = testHarness.createOAuthAdminClient(clientJaasConfig(jwsContainer.getJwsToken(), "other-cluster"), adminProperties);
      client.createTopics(sampleTopics).all().get();
      fail(String.format("Expected admin command to throw a %s", expectedException));
    } catch (Exception e) {
      if (e.getCause().getClass() != expectedException)
        fail(String.format("Expected admin command to throw a %s but it threw a %s", expectedException, e.getCause().getClass()));
      log.info("Expected exception message: {}", e.getCause().getMessage());
    }

    //Verify generated auth failure audit event
    MockAuditLogProvider auditLogProvider = MockAuditLogProvider.instance;
    ConfluentAuthenticationEvent authenticationEvent = (ConfluentAuthenticationEvent) auditLogProvider.lastAuthenticationEntry();
    assertFalse(authenticationEvent.principal().isPresent());
    assertEquals(AuditEventStatus.UNAUTHENTICATED, authenticationEvent.status());
    Assert.assertTrue(authenticationEvent.getScope().toString().contains("kafka-cluster=other-cluster"));
    assertTrue(authenticationEvent.authenticationException().isPresent());

    AuthenticationException authenticationException = authenticationEvent.authenticationException().get();
    AuthenticationErrorInfo errorInfo = authenticationException.errorInfo();

    Assert.assertTrue(errorInfo.errorMessage().contains("cluster other-cluster is not hosted on this broker"));
    assertEquals("1", errorInfo.identifier());
    assertEquals("other-cluster", errorInfo.saslExtensions().get(OAuthBearerJwsToken.OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY));
  }

  @Test
  public void testInvalidTokenThrowsException() throws Exception {
    setUp(new String[] {}); // an empty clusters claim in the token is considered invalid
    Class expectedException = SaslAuthenticationException.class;
    try {
      testHarness.createOAuthAdminClient(clientJaasConfig(jwsContainer.getJwsToken(), logicalClusterId), adminProperties).createTopics(sampleTopics).all().get();
      fail(String.format("Expected admin command to throw a %s", expectedException));
    } catch (Exception e) {
      if (e.getCause().getClass() != expectedException)
        fail(String.format("Expected admin command to throw a %s but it threw a %s", expectedException, e.getCause().getClass()));
      log.info("Expected exception message: {}", e.getCause().getMessage());
    }
    //Verify generated auth failure audit event
    MockAuditLogProvider auditLogProvider = MockAuditLogProvider.instance;
    ConfluentAuthenticationEvent authenticationEvent = (ConfluentAuthenticationEvent) auditLogProvider.lastAuthenticationEntry();
    assertFalse(authenticationEvent.principal().isPresent());
    assertEquals(AuditEventStatus.UNKNOWN_USER_DENIED, authenticationEvent.status());
    assertTrue(authenticationEvent.authenticationException().isPresent());
  }

  @Test
  public void testIllegalStateExceptionOnBrokerMetadataThrowsException() throws Exception {
    setUp();
    Class expectedException = SaslAuthenticationException.class;
    try {
      metadata.close(brokerUUID);
      AdminClient client = testHarness.createOAuthAdminClient(clientJaasConfig(jwsContainer.getJwsToken(), logicalClusterId), adminProperties);
      client.createTopics(sampleTopics).all().get();
      fail(String.format("Expected admin command to throw a %s", expectedException));
    } catch (Exception e) {
      if (e.getCause().getClass() != expectedException)
        fail(String.format("Expected admin command to throw a %s but it threw a %s", expectedException, e.getCause().getClass()));
      log.info("Expected exception message: {}", e.getCause().getMessage());
    }
    //Verify generated auth failure audit event
    MockAuditLogProvider auditLogProvider = MockAuditLogProvider.instance;
    ConfluentAuthenticationEvent authenticationEvent = (ConfluentAuthenticationEvent) auditLogProvider.lastAuthenticationEntry();
    assertFalse(authenticationEvent.principal().isPresent());
    assertEquals(AuditEventStatus.UNAUTHENTICATED, authenticationEvent.status());
    assertTrue(authenticationEvent.authenticationException().isPresent());

    AuthenticationException authenticationException = authenticationEvent.authenticationException().get();
    AuthenticationErrorInfo errorInfo = authenticationException.errorInfo();
    Assert.assertTrue(errorInfo.errorMessage().contains("Could not get cluster metadata to validate the token"));
    assertEquals("1", errorInfo.identifier());
    assertTrue(errorInfo.clusterId().isEmpty());
  }
}
