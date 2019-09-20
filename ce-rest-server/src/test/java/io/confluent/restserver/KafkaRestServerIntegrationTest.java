// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.restserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.server.rest.RestServerException;
import org.apache.kafka.test.IntegrationTest;
import org.easymock.EasyMock;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.kafka.common.network.Mode;
import org.apache.kafka.server.rest.BrokerProxy;
import org.apache.kafka.test.TestSslUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(IntegrationTest.class)
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.net.ssl.*", "javax.security.*"})
public class KafkaRestServerIntegrationTest {

    @Mock
    private BrokerProxy brokerProxy;

    private KafkaRestServer server;

    @Before
    public void setUp() {
        server = new KafkaRestServer();
    }

    @After
    public void tearDown() {
        if (server != null)
            server.shutdown();
    }

    @Test
    public void httpServerRouteTest() throws IOException {
        String id = "cluster-id";

        server.configure(defaultConfig());

        EasyMock.expect(brokerProxy.clusterId()).andReturn(id);
        PowerMock.replayAll();

        server.startup(brokerProxy);

        CloseableHttpClient httpClient = HttpClients.createMinimal();

        HttpRequest request = new HttpGet("/v1/metadata/id");
        HttpHost httpHost = new HttpHost(server.serverUrl().getHost(), server.serverUrl().getPort());
        CloseableHttpResponse response = httpClient.execute(httpHost, request);

        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(
            "{\"id\":\"" + id + "\",\"scope\":{\"kafka-cluster\":\"" + id + "\"}}",
            EntityUtils.toString(response.getEntity()));
        PowerMock.verifyAll();
    }

    @Test
    public void httpServerUnknownRouteTest() throws IOException {
        String id = "cluster-id";

        Map<String, String> props = new HashMap<>();
        props.put(KafkaRestServerConfig.LISTENERS_CONFIG, "http://localhost:0");

        server.configure(defaultConfig());

        PowerMock.replayAll();

        server.startup(brokerProxy);

        CloseableHttpClient httpClient = HttpClients.createMinimal();

        HttpRequest request = new HttpGet("/bla");
        HttpHost httpHost = new HttpHost(server.serverUrl().getHost(), server.serverUrl().getPort());
        CloseableHttpResponse response = httpClient.execute(httpHost, request);

        assertEquals(404, response.getStatusLine().getStatusCode());
        assertEquals(
            "{\"error_code\":404,\"message\":\"HTTP 404 Not Found\"}",
            EntityUtils.toString(response.getEntity()));
        PowerMock.verifyAll();
    }

    @Test
    public void httpsServerRouteTest() throws IOException, GeneralSecurityException {
        String id = "cluster-id";

        File trustStoreFile = File.createTempFile("truststore", ".jks");

        server.configure(sslConfig(trustStoreFile));

        EasyMock.expect(brokerProxy.clusterId()).andReturn(id);
        PowerMock.replayAll();

        server.startup(brokerProxy);

        CloseableHttpClient httpClient = httpsClient(trustStoreFile);

        HttpRequest request = new HttpGet("/v1/metadata/id");
        HttpHost httpHost = new HttpHost(server.serverUrl().getHost(), server.serverUrl().getPort(), "https");
        CloseableHttpResponse response = httpClient.execute(httpHost, request);

        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(
            "{\"id\":\"" + id + "\",\"scope\":{\"kafka-cluster\":\"" + id + "\"}}",
            EntityUtils.toString(response.getEntity()));
        PowerMock.verifyAll();
    }

    @Test
    public void httpsServerUnknownRouteTest() throws IOException, GeneralSecurityException {
        String id = "cluster-id";

        File trustStoreFile = File.createTempFile("truststore", ".jks");

        server.configure(sslConfig(trustStoreFile));

        PowerMock.replayAll();

        server.startup(brokerProxy);

        CloseableHttpClient httpClient = httpsClient(trustStoreFile);

        HttpRequest request = new HttpGet("/bla");
        HttpHost httpHost = new HttpHost(server.serverUrl().getHost(), server.serverUrl().getPort(), "https");
        CloseableHttpResponse response = httpClient.execute(httpHost, request);

        assertEquals(404, response.getStatusLine().getStatusCode());
        assertEquals(
            "{\"error_code\":404,\"message\":\"HTTP 404 Not Found\"}",
            EntityUtils.toString(response.getEntity()));
        PowerMock.verifyAll();
    }

    @Test
    public void testRestServletInitializers() {
        TestServletInitializer.clearContext();

        Map<String, Object> config = defaultConfig();
        config.put(
            KafkaRestServerConfig.INITIALIZERS_CLASSES_CONFIG,
            TestServletInitializer.class.getName()
        );

        server.configure(config);

        PowerMock.replayAll();

        server.startup(brokerProxy);

        assertNotNull(TestServletInitializer.context());
    }

    @Test(expected = RestServerException.class)
    public void testBadRestServletInitializer() {
        Map<String, Object> config = defaultConfig();
        config.put(
            KafkaRestServerConfig.INITIALIZERS_CLASSES_CONFIG,
            BadServletInitializer.class.getName()
        );

        server.configure(config);

        PowerMock.replayAll();

        server.startup(brokerProxy);
    }

    private Map<String, Object> defaultConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(KafkaRestServerConfig.LISTENERS_CONFIG, "http://localhost:0");
        return props;
    }

    private Map<String, Object> sslConfig(File trustStoreFile)
        throws IOException, GeneralSecurityException {
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(false, true,
            Mode.SERVER, trustStoreFile, "server");

        Map<String, Object> props = new HashMap<>(serverSslConfig);
        props.put(KafkaRestServerConfig.LISTENERS_CONFIG, "https://localhost:0");

        return props;
    }

    private CloseableHttpClient httpsClient(File trustStoreFile)
        throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, KeyManagementException {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        FileInputStream instream = new FileInputStream(trustStoreFile);

        try {
            trustStore.load(instream, TestSslUtils.TRUST_STORE_PASSWORD.toCharArray());
        } finally {
            instream.close();
        }

        SSLContext sslContext = SSLContexts.custom()
            .loadTrustMaterial(trustStore, new TrustSelfSignedStrategy())
            .build();

        SSLConnectionSocketFactory sslConnectionFactory =
            new SSLConnectionSocketFactory(sslContext, new DefaultHostnameVerifier());

        return HttpClients.custom()
            .setSSLSocketFactory(sslConnectionFactory)
            .build();
    }

    public static class TestServletInitializer implements Consumer<ServletContextHandler> {
        private static ServletContextHandler context = null;

        @Override
        public void accept(ServletContextHandler context) {
            TestServletInitializer.context = context;
        }

        public static ServletContextHandler context() {
            return context;
        }

        public static void clearContext() {
            TestServletInitializer.context = null;
        }
    }

    public static class BadServletInitializer implements Consumer<ServletContextHandler> {

        @Override
        public void accept(ServletContextHandler context) {
            throw new RuntimeException();
        }
    }
}
