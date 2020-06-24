package io.confluent.telemetry;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.Endpoint;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BrokerConfigUtilsTest {

    private final ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder();

    @Test
    public void testDefaultListener() {
        Endpoint endpoint =
                BrokerConfigUtils.getInterBrokerEndpoint(builder.build());
        assertTrue(endpoint.listenerName().isPresent());
        assertEquals("PLAINTEXT", endpoint.listenerName().get());
        assertEquals("PLAINTEXT", endpoint.securityProtocol().name());
        assertNull(endpoint.host());
        assertEquals(BrokerConfigUtils.PORT_DEFAULT, endpoint.port());
    }

    @Test
    public void testMultiNamedListener() {
        builder.put(BrokerConfigUtils.LISTENERS_PROP, "CLIENT://0.0.0.0:9092,REPLICATION://localhost:9093");
        builder.put(BrokerConfigUtils.INTER_BROKER_LISTENER_NAME_PROP, "REPLICATION");
        builder.put(BrokerConfigUtils.LISTENER_SECURITY_PROTOCOL_MAP_PROP, "CLIENT:PLAINTEXT,REPLICATION:SASL_SSL");

        Endpoint endpoint =
                BrokerConfigUtils.getInterBrokerEndpoint(builder.build());
        assertTrue(endpoint.listenerName().isPresent());
        assertEquals("REPLICATION", endpoint.listenerName().get());
        assertEquals("SASL_SSL", endpoint.securityProtocol().name());
        assertEquals("localhost", endpoint.host());
        assertEquals(9093, endpoint.port());
    }

    @Test
    public void testMultiNonNamedListener() {
        builder.put(BrokerConfigUtils.LISTENERS_PROP, "PLAINTEXT://0.0.0.0:9092,SASL_SSL://localhost:9093");
        builder.put(BrokerConfigUtils.INTER_BROKER_SECURITY_PROTOCOL_PROP, "SASL_SSL");

        Endpoint endpoint =
                BrokerConfigUtils.getInterBrokerEndpoint(builder.build());
        assertTrue(endpoint.listenerName().isPresent());
        assertEquals("SASL_SSL", endpoint.listenerName().get());
        assertEquals("SASL_SSL", endpoint.securityProtocol().name());
        assertEquals("localhost", endpoint.host());
        assertEquals(9093, endpoint.port());
    }
}
