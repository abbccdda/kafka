package io.confluent.kafka.multitenant;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.multitenant.utils.Utils;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.time.Duration;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SslCertificateManager {

    private static final Logger LOG = LoggerFactory.getLogger(PhysicalClusterMetadata.class);
    private static final Long CLOSE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
    private static final AlterConfigsOptions ALTER_OPTIONS = new AlterConfigsOptions().timeoutMs(30000);

    private final String brokerId;
    private final String sslCertsDir;
    private final String sslSpecFilename;

    private AdminClient adminClient;
    private AtomicBoolean adminClientCreated = new AtomicBoolean(false);
    private byte[] currentFullchainBytes = null;
    private byte[] currentPrivkeyBytes = null;

    SslCertificateManager(Map<String, ?> configs) {
        this(configs.get("broker.id"), configs.get(ConfluentConfigs.MULTITENANT_METADATA_SSL_CERTS_SPEC_CONFIG),
                null);
    }

    SslCertificateManager(Object brokerId, Object sslCertsPath, AdminClient adminClient) {
        this.brokerId = getBrokerId(brokerId);
        this.sslCertsDir = getSslCertsDirConfig(sslCertsPath);
        this.sslSpecFilename = getSslSpecFilename(sslCertsPath);
        this.adminClient = adminClient;
        if (this.adminClient != null)
            adminClientCreated.compareAndSet(false, true);
    }

    public void createAdminClient(String endpoint) {
        if (this.brokerId != null && this.sslCertsDir != null)
            this.adminClient = Utils.createAdminClient(endpoint);
        if (this.adminClient != null) {
            adminClientCreated.compareAndSet(false, true);
        }
    }

    public void close() {
        if (adminClient != null)
            adminClient.close(Duration.ofMillis(CLOSE_TIMEOUT_MS));
    }

    private String getBrokerId(Object brokerIdValue) {
        if (brokerIdValue == null)
            return null;
        return brokerIdValue.toString();
    }

    private String getSslCertsDirConfig(Object sslCertPath) {
        String sslCertDir = null;
        if (sslCertPath != null) {
            try {
                sslCertDir = sslCertPath.toString().substring(0, sslCertPath.toString().lastIndexOf("/"));
            } catch (IndexOutOfBoundsException e) {
                LOG.warn("Ssl cert spec config not in the correct format {}", sslCertPath, e);
            }
        }
        return sslCertDir;
    }

    private String getSslSpecFilename(Object sslCertPath) {
        String sslSpecFilename = null;
        if (sslCertPath != null) {
            try {
                sslSpecFilename = sslCertPath.toString().substring(sslCertPath.toString().lastIndexOf("/") + 1);
            } catch (IndexOutOfBoundsException e) {
                LOG.warn("Ssl cert spec config not in the correct format {}", sslCertPath, e);
            }
        }
        return sslSpecFilename;
    }

    String getSslCertDirForWatcher() {
        return sslCertsDir;
    }

    // used in unit tests
    AdminClient getAdminClient() {
        return adminClient;
    }

    void loadSslCertFiles() {
        if (!adminClientCreated.get()) {
            LOG.info("Admin client instance not created, would not proceed to certificate update");
            return;
        }
        Path sslCertsSpecFile = getSslCertsFilePath(sslSpecFilename);
        if (!Files.exists(sslCertsSpecFile)) {
            return;
        }
        getSslCertificateSpecifications();
    }

    private void getSslCertificateSpecifications() {
        try {
            Path specPath = getSslCertsFilePath(sslSpecFilename);
            ObjectMapper objectMapper = new ObjectMapper();
            SslCertificateSpecification sslSpec = objectMapper.readValue(specPath.toFile(),
                    SslCertificateSpecification.class);
            getSslConfigs(sslSpec);
        } catch (Exception e) {
            LOG.error("Error occurred while parsing certificate specifications", e);
        }
    }

    private void getSslConfigs(SslCertificateSpecification sslSpec) throws Exception {
        Path sslPemFullchainFilePath = getSslCertsFilePath(sslSpec.sslPemFullchainFilename());
        if (!Files.exists(sslPemFullchainFilePath)) {
            LOG.warn("No fullchain file found at path: {} ", sslPemFullchainFilePath);
            return;
        }
        Path sslPemPrivkeyFilePath = getSslCertsFilePath(sslSpec.sslPemPrivkeyFilename());
        if (!Files.exists(sslPemPrivkeyFilePath)) {
            LOG.warn("No privkey file found at path: {} ", sslPemPrivkeyFilePath);
            return;
        }
        Path pkcsCertFilePath = getSslCertsFilePath(sslSpec.pkcsCertFilename());
        if (!Files.exists(pkcsCertFilePath)) {
            LOG.warn("No pkcs file found at path: {} ", pkcsCertFilePath);
            return;
        }

        byte[] syncFullchainBytes = pemToBytes(sslPemFullchainFilePath);
        byte[] syncPrivkeyBytes = pemToBytes(sslPemPrivkeyFilePath);

        // pkcs files generated from same pem files at different points in time differ
        // so we compare the pem files
        if (!comparePEMCertificates(syncFullchainBytes, syncPrivkeyBytes)) {
            ConfigEntry keystoreLocation = new ConfigEntry("listener.name.external.ssl.keystore.location",
                    pkcsCertFilePath.toString());
            ConfigEntry keystoreType = new ConfigEntry("listener.name.external.ssl.keystore.type",
                    sslSpec.sslKeystoreType());

            List<AlterConfigOp> sslCertConfig = new ArrayList<>();
            sslCertConfig.add(new AlterConfigOp(keystoreLocation, AlterConfigOp.OpType.SET));
            sslCertConfig.add(new AlterConfigOp(keystoreType, AlterConfigOp.OpType.SET));

            ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
            Map<ConfigResource, Collection<AlterConfigOp>> alterSslCertConfig = new HashMap<>();
            alterSslCertConfig.put(configResource, sslCertConfig);
            invokeAdminClient(alterSslCertConfig, syncFullchainBytes, syncPrivkeyBytes);
        }
    }

    private synchronized void invokeAdminClient(Map<ConfigResource, Collection<AlterConfigOp>> alterSslCertConfig,
                                                byte[] syncFullchainBytes, byte[] syncPrivkeyBytes) throws Exception {
        AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(alterSslCertConfig, ALTER_OPTIONS);
        alterConfigsResult.all().get();
        currentFullchainBytes = syncFullchainBytes.clone();
        currentPrivkeyBytes = syncPrivkeyBytes.clone();
        LOG.info("Updated SSL certificate files for broker {} with {}", brokerId, alterConfigsResult);
    }

    private boolean comparePEMCertificates(byte[] syncFullchainBytes, byte[] syncPrivkeyBytes) {
        return Arrays.equals(currentFullchainBytes, syncFullchainBytes)
            && Arrays.equals(currentPrivkeyBytes, syncPrivkeyBytes);
    }

    private byte[] pemToBytes(Path path) throws Exception {
        String filepath = path.toString();
        File file = new File(filepath);
        FileInputStream fis = new FileInputStream(filepath);
        byte[] fileData = new byte[(int) file.length()];
        fis.read(fileData);
        return fileData;
    }

    private Path getSslCertsFilePath(String filename) {
        return Paths.get(sslCertsDir, filename);
    }
}
