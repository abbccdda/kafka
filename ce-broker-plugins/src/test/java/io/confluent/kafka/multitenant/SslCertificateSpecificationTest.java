package io.confluent.kafka.multitenant;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class SslCertificateSpecificationTest {

    private SslCertificateSpecification sslSpec;
    private Path sslSpecPath;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testSpecFileWithRequiredFields() {
        URL url = this.getClass().getResource("/cert_exp_may/spec.json");
        sslSpecPath = Paths.get(url.getPath());
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            sslSpec = objectMapper.readValue(sslSpecPath.toFile(), SslCertificateSpecification.class);
        } catch (IOException ioe) {
            fail("Failed to read ssl specification from file " + sslSpecPath);
        }
        assertEquals("PKCS12", sslSpec.sslKeystoreType());
        assertEquals("mystorepassword", sslSpec.getSslKeystorePassword());
        assertEquals("pkcs.p12", sslSpec.pkcsCertFilename());
        assertEquals("fullchain.pem", sslSpec.sslPemFullchainFilename());
        assertEquals("privkey.pem", sslSpec.sslPemPrivkeyFilename());
    }

    @Test
    public void testSpecFileContentMissingKeystoreTypeField() throws IOException {
        sslSpecPath = Utils.createSpecFile(tempFolder, Utils.SSL_CERT_SPEC_NO_TYPE);
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            sslSpec = objectMapper.readValue(sslSpecPath.toFile(), SslCertificateSpecification.class);
        } catch (IOException ioe) {
            fail("Failed to read ssl specification from file " + sslSpecPath);
        }
        assertNotEquals("PKCS12", sslSpec.sslKeystoreType());
        assertEquals("pkcs.p12", sslSpec.pkcsCertFilename());
        assertEquals("fullchain.pem", sslSpec.sslPemFullchainFilename());
        assertEquals("privkey.pem", sslSpec.sslPemPrivkeyFilename());
    }

    @Test
    public void testSpecFileContentMissingKeystoreCertFileField() throws IOException {
        sslSpecPath = Utils.createSpecFile(tempFolder, Utils.SSL_CERT_SPEC_NO_PKCSFILE);
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            sslSpec = objectMapper.readValue(sslSpecPath.toFile(), SslCertificateSpecification.class);
        } catch (IOException ioe) {
            fail("Failed to read ssl specification from file " + sslSpecPath);
        }
        assertEquals("PKCS12", sslSpec.sslKeystoreType());
        assertNotEquals("pkcs.p12", sslSpec.pkcsCertFilename());
        assertEquals("fullchain.pem", sslSpec.sslPemFullchainFilename());
        assertEquals("privkey.pem", sslSpec.sslPemPrivkeyFilename());
    }

    @Test
    public void testSpecFileContentMissingPemFilesField() throws IOException {
        sslSpecPath = Utils.createSpecFile(tempFolder, Utils.SSL_CERT_SPEC_NO_PEMFILES);
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            sslSpec = objectMapper.readValue(sslSpecPath.toFile(), SslCertificateSpecification.class);
        } catch (IOException ioe) {
            fail("Failed to read ssl specification from file " + sslSpecPath);
        }
        assertEquals("PKCS12", sslSpec.sslKeystoreType());
        assertEquals("pkcs.p12", sslSpec.pkcsCertFilename());
        assertNotEquals("fullchain.pem", sslSpec.sslPemFullchainFilename());
        assertNotEquals("privkey.pem", sslSpec.sslPemPrivkeyFilename());
    }
}
