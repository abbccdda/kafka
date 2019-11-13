// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.connect.runtime.rest.resources;

import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConfluentConnectClusterId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/v1/metadata")
@Produces(MediaType.APPLICATION_JSON)
public class ConfluentV1MetadataResource {

    public static final String UNKNOWN_KAFKA_CLUSTER_ID = "UNKNOWN";

    private static final Logger log = LoggerFactory.getLogger(ConfluentV1MetadataResource.class);

    private final ConfluentConnectClusterId clusterId;

    public ConfluentV1MetadataResource(Herder herder, WorkerConfig workerConfig) {
        String kafkaClusterId = herder.kafkaClusterId();
        if (kafkaClusterId == null) {
            log.warn(
                "Unable to fetch cluster ID from pre-0.10.1 Kafka broker, defaulting to '{}'",
                UNKNOWN_KAFKA_CLUSTER_ID
            );
            kafkaClusterId = UNKNOWN_KAFKA_CLUSTER_ID;
        }

        String connectClusterId = workerConfig.originals()
            .getOrDefault(DistributedConfig.GROUP_ID_CONFIG, "STANDALONE")
            .toString();

        this.clusterId = new ConfluentConnectClusterId(kafkaClusterId, connectClusterId);
    }

    @GET
    @Path("id")
    public ConfluentConnectClusterId clusterId() {
        return clusterId;
    }
}
