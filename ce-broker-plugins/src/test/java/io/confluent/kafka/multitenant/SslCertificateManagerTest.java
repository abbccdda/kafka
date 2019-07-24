package io.confluent.kafka.multitenant;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;

import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SslCertificateManagerTest {

    private static final String SSL_CERTS_DIR = "mnt/sslcerts/";
    private static final String DATA_DIR = "..data";
    private static final String BROKER_ID = "1";
    private static final URL TEST_SSL_CERTS_AUG = SslCertificateManagerTest.class.getResource("/cert_exp_aug");
    private static final URL TEST_ROOT = SslCertificateManagerTest.class.getResource("/");

    private AdminClient mockAdminClient;
    private SslCertificateManager sslCache;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        System.out.println("root resource: " + TEST_ROOT.getPath());
        Node node = new Node(1, "localhost", 9092);
        String sslCertsPath = tempFolder.getRoot().getCanonicalPath() + "/" + SSL_CERTS_DIR + "spec.json";
        mockAdminClient = spy(new MockAdminClient(singletonList(node), node));
        sslCache = new SslCertificateManager(BROKER_ID, sslCertsPath, mockAdminClient);
    }

    @After
    public void teardown() {
        sslCache.close();
    }

    @Test
    public void testAdminClientInvokedAfterCertificateSync() throws Exception {
        Utils.deleteFiles(tempFolder, SSL_CERTS_DIR);
        Utils.syncCerts(tempFolder, TEST_SSL_CERTS_AUG, SSL_CERTS_DIR);
        sslCache.loadSslCertFiles();
        verify(mockAdminClient, times(1)).incrementalAlterConfigs(any(), any());
    }

    @Test
    public void testAdminClientNotInvokedWithoutReadPermissionForCerts() throws Exception {
        Utils.syncCerts(tempFolder, TEST_SSL_CERTS_AUG, SSL_CERTS_DIR);
        String fullchainFilepath = tempFolder.getRoot().getCanonicalPath() + "/" + SSL_CERTS_DIR + "/" + DATA_DIR + "/fullchain.pem";
        String privkeyFilepath = tempFolder.getRoot().getCanonicalPath() + "/" + SSL_CERTS_DIR + "/" + DATA_DIR + "/privkey.pem";
        Files.setPosixFilePermissions(Paths.get(fullchainFilepath), PosixFilePermissions.fromString("-wx-wx-wx"));
        Files.setPosixFilePermissions(Paths.get(privkeyFilepath), PosixFilePermissions.fromString("-wx-wx-wx"));
        sslCache.loadSslCertFiles();
        verify(mockAdminClient, times(0)).incrementalAlterConfigs(any(), any());
    }

    @Test
    public void testAdminClientNotInvokedWithoutSpecFile() throws Exception {
        String specfile = "spec.json";
        Utils.moveFile(specfile, TEST_SSL_CERTS_AUG, TEST_ROOT);
        Utils.deleteFiles(tempFolder, SSL_CERTS_DIR);
        Utils.syncCerts(tempFolder, TEST_SSL_CERTS_AUG, SSL_CERTS_DIR);
        sslCache.loadSslCertFiles();
        verify(mockAdminClient, times(0)).incrementalAlterConfigs(any(), any());
        Utils.moveFile(specfile, TEST_ROOT, TEST_SSL_CERTS_AUG);
    }

    @Test
    public void testAdminClientNotInvokedWithoutPKCSCertificate() throws Exception {
        String pkcsfile = "pkcs.p12";
        Utils.moveFile(pkcsfile, TEST_SSL_CERTS_AUG, TEST_ROOT);
        Utils.deleteFiles(tempFolder, SSL_CERTS_DIR);
        Utils.syncCerts(tempFolder, TEST_SSL_CERTS_AUG, SSL_CERTS_DIR);
        sslCache.loadSslCertFiles();
        verify(mockAdminClient, times(0)).incrementalAlterConfigs(any(), any());
        Utils.moveFile(pkcsfile, TEST_ROOT, TEST_SSL_CERTS_AUG);
    }

    @Test
    public void testAdminClientNotInvokedWithoutPrivkeyPemFile() throws Exception {
        String privkeyfile = "privkey.pem";
        Utils.moveFile(privkeyfile, TEST_SSL_CERTS_AUG, TEST_ROOT);
        Utils.deleteFiles(tempFolder, SSL_CERTS_DIR);
        Utils.syncCerts(tempFolder, TEST_ROOT, SSL_CERTS_DIR);
        sslCache.loadSslCertFiles();
        verify(mockAdminClient, times(0)).incrementalAlterConfigs(any(), any());
        Utils.moveFile(privkeyfile, TEST_ROOT, TEST_SSL_CERTS_AUG);
    }

    @Test
    public void testAdminClientNotInvokedWithoutFullchainPemFile() throws Exception {
        String fullchainfile = "fullchain.pem";
        Utils.moveFile(fullchainfile, TEST_SSL_CERTS_AUG, TEST_ROOT);
        Utils.deleteFiles(tempFolder, SSL_CERTS_DIR);
        Utils.syncCerts(tempFolder, TEST_SSL_CERTS_AUG, SSL_CERTS_DIR);
        sslCache.loadSslCertFiles();
        verify(mockAdminClient, times(0)).incrementalAlterConfigs(any(), any());
        Utils.moveFile(fullchainfile, TEST_ROOT, TEST_SSL_CERTS_AUG);
    }
}
