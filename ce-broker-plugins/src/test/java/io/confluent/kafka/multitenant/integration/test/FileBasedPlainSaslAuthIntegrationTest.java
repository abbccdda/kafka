// (Copyright) [2020 - 2020] Confluent, Inc.
package io.confluent.kafka.multitenant.integration.test;

import io.confluent.kafka.multitenant.MultiTenantPrincipalBuilder;
import io.confluent.kafka.multitenant.PhysicalClusterMetadata;
import io.confluent.kafka.multitenant.Utils;
import io.confluent.kafka.multitenant.authorizer.MultiTenantAuditLogConfig;
import io.confluent.kafka.multitenant.authorizer.MultiTenantAuthorizer;
import io.confluent.kafka.multitenant.integration.cluster.LogicalCluster;
import io.confluent.kafka.multitenant.integration.cluster.LogicalClusterUser;
import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;
import io.confluent.kafka.security.audit.event.ConfluentAuthenticationEvent;
import io.confluent.kafka.security.authorizer.MockAuditLogProvider;
import io.confluent.kafka.server.plugins.auth.FileBasedPlainSaslAuthenticatorTest;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import kafka.admin.AclCommand;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * An integration test for File based Plain Sasl auth with AdminClient
 */
@Category(IntegrationTest.class)
public class FileBasedPlainSaslAuthIntegrationTest {

    //use lkc/user/APIkey details fron file_auth_test_apikeys.json
    private final String logicalClusterId = LC_META_ABC.logicalClusterId();
    private final int serviceUserId = 1;
    private final String serviceUserAPIkey = "APIKEY1";
    private final String serviceUserAPIkeyPassword = "pwd1";

    private IntegrationTestHarness testHarness;
    private String brokerUUID;
    private PhysicalClusterMetadata metadata;
    private final String testTopic = "abcd";
    private final List<NewTopic> sampleTopics = Collections.singletonList(new NewTopic(testTopic, 3, (short) 1));
    private final String path = FileBasedPlainSaslAuthenticatorTest.class.getResource("/file_auth_test_apikeys.json").getFile();
    private LogicalClusterUser testUser;

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        MockAuditLogProvider.reset();
        testHarness = new IntegrationTestHarness();

        PhysicalCluster physicalCluster = testHarness.start(setUpMetadata(brokerProps()));
        LogicalCluster logicalCluster = physicalCluster.createLogicalCluster(logicalClusterId, 100, serviceUserId);
        testUser = logicalCluster.user(serviceUserId);
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
        configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG, tempFolder.getRoot().getCanonicalPath());

        metadata = initiatePhysicalClusterMetadata(configs);

        Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
        TestUtils.waitForCondition(
            () -> metadata.metadata(LC_META_ABC.logicalClusterId()) != null,
            "Expected metadata of new logical cluster to be present in metadata cache");

        return brokerProps;
    }

    private Properties brokerProps() {
        Properties props = new Properties();
        props.put("sasl.enabled.mechanisms", Collections.singletonList("PLAIN"));
        props.put("listener.name.external.plain.sasl.jaas.config",
            "io.confluent.kafka.server.plugins.auth.FileBasedLoginModule required " +
                "config_path=\"" + path + "\" refresh_ms=\"1000\";");
        props.put("listener.name.external.principal.builder.class", MultiTenantPrincipalBuilder.class.getName());
        props.put(ConfluentConfigs.ENABLE_AUTHENTICATION_AUDIT_LOGS, "true");
        props.put(KafkaConfig$.MODULE$.AuthorizerClassNameProp(), MultiTenantAuthorizer.class.getName());
        props.put(MultiTenantAuditLogConfig.MULTI_TENANT_AUDIT_LOGGER_ENABLE_CONFIG, "true");
        return props;
    }

    private String clientJaasConfig(String username, String password) {
        return "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            " username=\"" + username + "\" " +
            " password=\"" + password + "\";";
    }

    @Test
    public void testSuccessfulAuthentication() throws Exception {
        AdminClient client = testHarness.createPlainAuthAdminClient(clientJaasConfig(serviceUserAPIkey, serviceUserAPIkeyPassword));
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
    public void testInvalidPassword() throws InterruptedException {
        AdminClient client = testHarness.createPlainAuthAdminClient(clientJaasConfig(serviceUserAPIkey, "WrongPassword"));
        KafkaFuture<Void> future = client.createTopics(sampleTopics).all();
        TestUtils.assertFutureError(future, SaslAuthenticationException.class);

        //Verify generated auth failure audit event
        MockAuditLogProvider auditLogProvider = MockAuditLogProvider.instance;
        ConfluentAuthenticationEvent authenticationEvent = (ConfluentAuthenticationEvent) auditLogProvider.lastAuthenticationEntry();

        assertFalse(authenticationEvent.principal().isPresent());
        assertEquals(AuditEventStatus.UNAUTHENTICATED, authenticationEvent.status());
        Assert.assertTrue(authenticationEvent.getScope().toString().contains("kafka-cluster=lkc-abc"));
        assertTrue(authenticationEvent.authenticationException().isPresent());

        AuthenticationException authenticationException = authenticationEvent.authenticationException().get();
        AuthenticationErrorInfo errorInfo = authenticationException.errorInfo();
        Assert.assertTrue(errorInfo.errorMessage().contains("Bad password for user APIKEY1"));
        assertEquals("APIKEY1", errorInfo.identifier());
        assertEquals("lkc-abc", errorInfo.clusterId());
    }

    @Test
    public void testUnknownUser() throws InterruptedException {
        AdminClient client = testHarness.createPlainAuthAdminClient(clientJaasConfig("UnknownUser", "WrongPassword"));
        KafkaFuture<Void> future = client.createTopics(sampleTopics).all();
        TestUtils.assertFutureError(future, SaslAuthenticationException.class);

        //Verify generated auth failure audit event
        MockAuditLogProvider auditLogProvider = MockAuditLogProvider.instance;
        ConfluentAuthenticationEvent authenticationEvent = (ConfluentAuthenticationEvent) auditLogProvider.lastAuthenticationEntry();

        assertFalse(authenticationEvent.principal().isPresent());
        assertEquals(AuditEventStatus.UNKNOWN_USER_DENIED, authenticationEvent.status());
        assertTrue(authenticationEvent.authenticationException().isPresent());

        AuthenticationException authenticationException = authenticationEvent.authenticationException().get();
        AuthenticationErrorInfo errorInfo = authenticationException.errorInfo();
        Assert.assertTrue(errorInfo.errorMessage().contains("Unknown user UnknownUser"));
        assertEquals("UnknownUser", errorInfo.identifier());
    }
}
