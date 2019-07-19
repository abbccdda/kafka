package io.confluent.kafka.multitenant.integration.test;

import io.confluent.kafka.multitenant.Utils;
import io.confluent.kafka.multitenant.integration.cluster.LogicalCluster;
import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.confluent.kafka.multitenant.Utils.LC_META_ABC;
import static io.confluent.kafka.multitenant.Utils.LC_META_XYZ;

public class SslCertificateIntegrationTest {

    private static final Long TEST_CACHE_RELOAD_DELAY_MS = TimeUnit.SECONDS.toMillis(5);
    private static final String SSL_CERTS_DIR = "mnt/sslcerts/";
    private static final String SSL_CERTS_MAY_URL = "/cert_exp_may";

    private IntegrationTestHarness testHarness;
    private LogicalCluster logicalCluster1;
    private LogicalCluster logicalCluster2;

    protected Time time;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
        Utils.createLogicalClusterFile(LC_META_XYZ, tempFolder);
        Utils.syncCerts(tempFolder, this.getClass().getResource(SSL_CERTS_MAY_URL), SSL_CERTS_DIR);
        testHarness = new IntegrationTestHarness();
        PhysicalCluster physicalCluster = testHarness.start(brokerProps());
        logicalCluster1 = physicalCluster.createLogicalCluster("tenantA", 100, 9, 11, 12);
        logicalCluster2 = physicalCluster.createLogicalCluster("tenantB", 200, 9, 21, 22);
    }

    @After
    public void tearDown() throws Exception {
        testHarness.shutdown();
    }

    private Properties brokerProps() throws IOException {
        Properties props = new Properties();
        props.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
                tempFolder.getRoot().getCanonicalPath());
        props.put(ConfluentConfigs.MULTITENANT_METADATA_CLASS_CONFIG,
                "io.confluent.kafka.multitenant.PhysicalClusterMetadata");
        props.put(ConfluentConfigs.MULTITENANT_METADATA_RELOAD_DELAY_MS_CONFIG,
                TEST_CACHE_RELOAD_DELAY_MS);
        props.put("listeners", "INTERNAL://localhost:0, SSL://localhost:0");
        props.put("advertised.listeners", "INTERNAL://localhost:0, SSL://localhost:0");
        props.put("listener.security.protocol.map",  "INTERNAL:PLAINTEXT, SSL:SSL");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,  tempFolder.getRoot().getCanonicalPath() + "/"
                + SSL_CERTS_DIR + "/pkcs.p12");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "mystorepassword");
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        return props;
    }

    @Test(expected = SslAuthenticationException.class)
    public void testProduceConsumeFailsOnExpiredCertificateSync() throws Throwable {
        testHarness.produceConsume(logicalCluster1.user(11), logicalCluster2.user(21),
                "testtopic", "group", 0, SecurityProtocol.SSL);
    }
}
