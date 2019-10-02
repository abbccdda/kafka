// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.connect.runtime.rest.resources;

import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConfluentConnectClusterId;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/v1/metadata")
@Produces(MediaType.APPLICATION_JSON)
public class ConfluentV1MetadataResource {

    private final ConfluentConnectClusterId clusterId;

    public ConfluentV1MetadataResource(Herder herder, WorkerConfig workerConfig) {
        String kafkaClusterId = herder.kafkaClusterId();
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
