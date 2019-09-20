// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.restserver.resources;

import static org.junit.Assert.assertEquals;

import io.confluent.restserver.entities.ClusterId;
import javax.ws.rs.core.Response;
import org.apache.kafka.server.rest.BrokerProxy;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class MetadataResourceTest {

    @Mock
    private BrokerProxy brokerProxy;

    @Test
    public void idTest() {
        String id = "cluster-id";
        MetadataResource resource = new MetadataResource(brokerProxy);

        EasyMock.expect(brokerProxy.clusterId()).andReturn(id);
        PowerMock.replayAll();

        Response response = resource.id();
        assertEquals(200, response.getStatus());
        ClusterId clusterId = (ClusterId) response.getEntity();
        assertEquals(id, clusterId.id());

        PowerMock.verifyAll();
    }
}
