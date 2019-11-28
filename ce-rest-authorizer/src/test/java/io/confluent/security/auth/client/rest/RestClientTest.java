// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.rest;

import io.confluent.security.auth.client.RestAuthorizer;
import io.confluent.security.auth.client.RestClientConfig;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RestClientTest {

    @Test
    public void testFailOver() {
        List<String> urllList = Arrays.asList("http://url1:80", "http://url2:80", "http://url3:80");

        Map<String, Object> configs = new HashMap<>();
        configs.put(RestClientConfig.BOOTSTRAP_METADATA_SERVER_URLS_PROP, String.join(",", urllList));
        configs.put(RestClientConfig.ENABLE_METADATA_SERVER_URL_REFRESH, false);

        RestClient restClient = new RestClient(configs, Time.SYSTEM);
        RestAuthorizer authorizer = new RestAuthorizer(restClient);

        KafkaPrincipal userPrincipal =
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "principal");
        Action alterConfigs = new Action(Scope.kafkaClusterScope("clusterA"), new ResourceType("Cluster"),
                "kafka-cluster", new Operation("AlterConfigs"));
        List<Action> actionList = Collections.singletonList(alterConfigs);

        // succeed at 3 retry
        FailOverTestRequestSender requestSender = new FailOverTestRequestSender(3);
        restClient.requestSender(requestSender);
        authorizer.authorize(userPrincipal, "localhost", actionList);

        assertEquals(3, requestSender.attempt);
        assertEquals(3, requestSender.triedUrls.size());

        // succeed at 2 retry
        requestSender = new FailOverTestRequestSender(2);
        restClient.requestSender(requestSender);
        authorizer.authorize(userPrincipal, "localhost", actionList);

        assertEquals(2, requestSender.attempt);
        assertEquals(2, requestSender.triedUrls.size());
    }

    @Test
    public void testGetActiveUrls() throws Exception {
        List<String> urllList = Arrays.asList("http://url1:80", "http://url2:80", "http://url3:80");

        Map<String, Object> configs = new HashMap<>();
        configs.put(RestClientConfig.BOOTSTRAP_METADATA_SERVER_URLS_PROP, String.join(",", urllList));
        configs.put(RestClientConfig.ENABLE_METADATA_SERVER_URL_REFRESH, false);

        RestClient restClient = new RestClient(configs, Time.SYSTEM);

        restClient.requestSender(new EmptyResponseTestRequestSender());
        assertEquals(urllList, restClient.getActiveMetadataServerURLs());
        restClient.close();
    }

    @Test
    public void testRetryRequestTimeout() {
        List<String> urllList = Arrays.asList("http://url1:80");
        int timeout = 30 * 1000;

        Map<String, Object> configs = new HashMap<>();
        configs.put(RestClientConfig.BOOTSTRAP_METADATA_SERVER_URLS_PROP, String.join(",", urllList));
        // Prompts client to request ActiveUrls before returning control to caller
        configs.put(RestClientConfig.ENABLE_METADATA_SERVER_URL_REFRESH, true);
        configs.put(RestClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout);

        Time time = Time.SYSTEM;
        long target = time.milliseconds() + timeout;
        try {
            /* Blocks fetching viable MDS nodes until timeout */
            new RestClient(configs, time);
            fail("should have failed");
        } catch (RuntimeException e) {
            /* NOOP */
        }

        if (time.milliseconds() < target) {
            fail("Aborted before timeout");
        }

    }

    private static class FailOverTestRequestSender implements RequestSender {

        int successAttempt;
        int attempt = 0;
        Set<String> triedUrls = new HashSet<>();

        FailOverTestRequestSender(final int successAttempt) {
            this.successAttempt = successAttempt;
        }

        @Override
        public <T> T send(RestRequest request, final long requestTimeout) throws IOException {
            attempt++;
            triedUrls.add(request.build().toString());
            if (attempt == successAttempt)
                return request.readResponse(new ByteArrayInputStream("[]".getBytes()));
            else {
                throw new IOException("http Request Failed");
            }
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testRequestTimeout() {
        List<String> urllList = Arrays.asList("http://url1:80", "http://url2:80", "http://url3:80");

        Map<String, Object> configs = new HashMap<>();
        configs.put(RestClientConfig.BOOTSTRAP_METADATA_SERVER_URLS_PROP, String.join(",", urllList));
        configs.put(RestClientConfig.ENABLE_METADATA_SERVER_URL_REFRESH, false);
        configs.put(RestClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30 * 1000);

        Time time = new MockTime();
        RestClient restClient = new RestClient(configs, time);

        KafkaPrincipal userPrincipal =
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "principal");
        Action alterConfigs = new Action(Scope.kafkaClusterScope("clusterA"), new ResourceType("Cluster"),
            "kafka-cluster", new Operation("AlterConfigs"));
        List<Action> actionList = Collections.singletonList(alterConfigs);

        // succeed at 3 retry
        TimeoutTestRequestSender requestSender = new TimeoutTestRequestSender(time, 10 * 1000, 3);
        restClient.requestSender(requestSender);

        RestAuthorizer authorizer = new RestAuthorizer(restClient);
        authorizer.authorize(userPrincipal, "localhost", actionList);

        // test request timeout
        requestSender = new TimeoutTestRequestSender(time, 20 * 1000, 3);
        restClient.requestSender(requestSender);
        try {
            authorizer.authorize(userPrincipal, "localhost", actionList);
            fail("should have failed");
        } catch (RuntimeException e) {

        }
    }

    @Test
    public void testRestClientConfigs() {
        Properties props = new Properties();
        String sslTruststore = "test.truststore.jks";
        String sslKeystore = "test.truststore.jks";

        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststore);
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystore);
        props.put("confluent.metadata.ssl.keystore.location", "rest.keystore.jks");

        RestClientConfig config = new RestClientConfig(props);

        Map<String, ?> sslClientConfigs = config.sslClientConfigs();
        assertEquals(sslTruststore, sslClientConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertEquals("rest.keystore.jks", sslClientConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertEquals("TLSv1.2", sslClientConfigs.get(SslConfigs.SSL_PROTOCOL_CONFIG));
    }

    private static class TimeoutTestRequestSender implements RequestSender {

        private final Time time;
        private final int sleepTime;
        int attempt = 0;
        int successAttempt;

        TimeoutTestRequestSender(final Time time, final int sleepTime, final int successAttempt) {
            this.time = time;
            this.sleepTime = sleepTime;
            this.successAttempt = successAttempt;
        }

        @Override
        public <T> T send(RestRequest request, final long requestTimeout) throws IOException {
            attempt++;
            time.sleep(sleepTime);
            if (attempt == successAttempt) {
                return request.readResponse(new ByteArrayInputStream("[]".getBytes()));
            } else {
                throw new IOException("http Request Failed");
            }
        }

        @Override
        public void close() {
        }
    }

    private static class EmptyResponseTestRequestSender implements RequestSender {

        EmptyResponseTestRequestSender() {
        }

        @Override
        public <T> T send(RestRequest request, final long requestTimeout) throws IOException {
            return request.readResponse(new ByteArrayInputStream("[]".getBytes()));
        }

        @Override
        public void close() {
        }
    }
}
