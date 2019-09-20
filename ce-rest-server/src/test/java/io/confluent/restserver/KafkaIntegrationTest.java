// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.restserver;

import static org.junit.Assert.assertEquals;

import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import kafka.server.KafkaServer;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class KafkaIntegrationTest {

    private EmbeddedKafkaCluster kafkaCluster;

    @Before
    public void setUp() throws Exception {
        kafkaCluster = new EmbeddedKafkaCluster();
        kafkaCluster.startZooKeeper();
        Properties props = new Properties();
        props.put(ConfluentConfigs.REST_SERVER_CLASS_CONFIG, KafkaRestServer.class);
        props.put(KafkaRestServerConfig.LISTENERS_CONFIG, "http://localhost:0");
        kafkaCluster.startBrokers(1, props);
    }

    @After
    public void tearDown() {
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
            kafkaCluster = null;
        }
    }

    @Test
    public void httpServerRouteTest() throws IOException {
        KafkaServer kafkaServer = kafkaCluster.brokers().get(0);
        URI uri = kafkaCluster.brokers().get(0).restServer().serverUrl();

        CloseableHttpClient httpClient = HttpClients.createMinimal();

        HttpRequest request = new HttpGet("/v1/metadata/id");
        HttpHost httpHost = new HttpHost(uri.getHost(), uri.getPort());
        CloseableHttpResponse response = httpClient.execute(httpHost, request);

        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(
            "{\"id\":\"" + kafkaServer.clusterId() + "\",\"scope\":{\"kafka-cluster\":\"" + kafkaServer.clusterId() + "\"}}",
            EntityUtils.toString(response.getEntity()));
    }

    @Test
    public void httpServerUnknownRouteTest() throws IOException {
        KafkaServer kafkaServer = kafkaCluster.brokers().get(0);
        URI uri = kafkaCluster.brokers().get(0).restServer().serverUrl();

        CloseableHttpClient httpClient = HttpClients.createMinimal();

        HttpRequest request = new HttpGet("/bla");
        HttpHost httpHost = new HttpHost(uri.getHost(), uri.getPort());
        CloseableHttpResponse response = httpClient.execute(httpHost, request);

        assertEquals(404, response.getStatusLine().getStatusCode());
        assertEquals(
            "{\"error_code\":404,\"message\":\"HTTP 404 Not Found\"}",
            EntityUtils.toString(response.getEntity()));
    }
}
