// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.restserver.resources;

import org.apache.kafka.server.rest.BrokerProxy;
import io.confluent.restserver.entities.ClusterId;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/metadata/")
@Produces(MediaType.APPLICATION_JSON)
public class MetadataResource {

    private BrokerProxy broker;

    public MetadataResource(BrokerProxy broker) {
        this.broker = broker;
    }

    @GET
    @Path("/id")
    public Response id() {
        return Response.ok().entity(new ClusterId(broker)).build();
    }
}
