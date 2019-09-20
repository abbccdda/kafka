// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.restserver.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.server.rest.BrokerProxy;
import java.util.Collections;
import java.util.Map;


public class ClusterId {
    private final String clusterId;

    @JsonCreator
    private ClusterId(@JsonProperty("id") String clusterId) {
        this.clusterId = clusterId;
    }

    public ClusterId(BrokerProxy broker) {
        this(broker.clusterId());
    }

    @JsonProperty("id")
    public String id() {
        return clusterId;
    }

    @JsonProperty("scope")
    public Map<String, String> scope() {
        return Collections.singletonMap("kafka-cluster", clusterId);
    }
}
