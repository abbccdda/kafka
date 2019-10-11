// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.connect.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConfluentConnectClusterId;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * An integration test that ensures the /v1/metadata resource is functioning.
 * <p></p>
 * The following test configures brings up a Connect worker and ensures that a request to the
 * /v1/metadata/id endpoint succeeds and returns a valid response body.
 */
@Category(IntegrationTest.class)
public class ConfluentV1MetadataResourceTest {

    protected static final String CONNECT_CLUSTER_ID =
        "confluent-v1-metadata-resource-test-cluster";
    protected static final long KAFKA_CLUSTER_ID_TIMEOUT_MS = 10000L;

    private static final Logger log = LoggerFactory.getLogger(ExampleConnectIntegrationTest.class);

    private EmbeddedConnectCluster connect;

    @Before
    public void setup() throws IOException {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(DistributedConfig.GROUP_ID_CONFIG, CONNECT_CLUSTER_ID);

        connect = new EmbeddedConnectCluster.Builder()
            .numWorkers(1)
            .numBrokers(1)
            .workerProps(workerProps)
            .build();

        // start the clusters
        connect.start();
    }

    @After
    public void close() {
        // stop all Connect, Kafka and Zk threads.
        connect.stop();
    }

    /**
     * Make a request to the /v1/metadata/id endpoint and verify the contents of the response
     */
    @Test
    public void testIdEndpoint() throws Exception {
        String kafkaClusterId = connect.kafka()
            .createAdminClient()
            .describeCluster()
            .clusterId()
            .get(KAFKA_CLUSTER_ID_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        ConfluentConnectClusterId expectedClusterId =
            new ConfluentConnectClusterId(kafkaClusterId, CONNECT_CLUSTER_ID);

        String rawResponse = connect.executeGet(connect.endpointForResource("v1/metadata/id"));
        ConfluentConnectClusterId clusterId =
            new ObjectMapper().readValue(rawResponse, ConfluentConnectClusterId.class);

        assertEquals(
            "Metadata resource should contain accurate response, " 
                + "including expected Connect and Kafka cluster IDs",
            expectedClusterId,
            clusterId
        );
    }
}
