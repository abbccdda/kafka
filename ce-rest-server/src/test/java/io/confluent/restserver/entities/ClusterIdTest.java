// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.restserver.entities;

import org.apache.kafka.server.rest.BrokerProxy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClusterIdTest  {

    @Test
    public void ClusterIdTest() {
        String id = "my-cluster-id";
        ClusterId clusterId = new ClusterId(new BrokerProxy() {
            @Override
            public String clusterId() {
                return id;
            }
        });

        assertEquals(id, clusterId.id());
        assertEquals(id, clusterId.scope().get("kafka-cluster"));
    }
}
