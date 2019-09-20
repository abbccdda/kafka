// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.restserver;

import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.rest.BrokerProxy;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.net.ssl.*", "javax.security.*"})
public class KafkaRestServerTest {

    @Mock
    private BrokerProxy brokerProxy;

    private KafkaRestServer server;

    @Before
    public void setUp() {
        server = new KafkaRestServer();
    }

    @After
    public void tearDown() {
        if (server != null)
            server.shutdown();
    }

    @Test
    public void configureHttpListenerTest() {
        Map<String, String> props = new HashMap<>();
        props.put(KafkaRestServerConfig.LISTENERS_CONFIG, "http://localhost:8080");

        server.configure(props);
    }

    @Test
    public void configureHttpsListenerTest() {
        Map<String, String> props = new HashMap<>();
        props.put(KafkaRestServerConfig.LISTENERS_CONFIG, "https://localhost:8080");

        server.configure(props);
    }

    @Test(expected = ConfigException.class)
    public void configureNoListenerTest() {
        Map<String, String> props = new HashMap<>();

        server.configure(props);
    }

    @Test(expected = ConfigException.class)
    public void configureWrongListenerTest() {
        Map<String, String> props = new HashMap<>();
        props.put(KafkaRestServerConfig.LISTENERS_CONFIG, "abcd");

        server.configure(props);
    }

    @Test(expected = ConfigException.class)
    public void configureUnknownListenerTest() {
        Map<String, String> props = new HashMap<>();
        props.put(KafkaRestServerConfig.LISTENERS_CONFIG, "plaintext://:9090");

        server.configure(props);
    }
}
