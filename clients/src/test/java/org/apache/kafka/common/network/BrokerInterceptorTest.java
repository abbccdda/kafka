package org.apache.kafka.common.network;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.interceptor.BrokerInterceptor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// Make sure BrokerInterceptor methods are called when expected
public class BrokerInterceptorTest {

    static final String CONNECTION_ID = "1";
    static final KafkaPrincipal TEST_PRINCIPAL = new KafkaPrincipal("user", "test-user");

    Time time;
    Metrics metrics;
    ChannelBuilder channelBuilder;
    Selector selector;
    InetAddress localhost;
    BrokerInterceptor mockBrokerInterceptor;

    @Before
    public void setUp() throws UnknownHostException {
        Map<String, Object> configs = new HashMap<>();
        this.time = new MockTime();
        this.metrics = new Metrics();
        // note that right now we are not using the channels this will build
        this.channelBuilder = new PlaintextChannelBuilder(Mode.SERVER, null);
        this.channelBuilder.configure(configs);
        this.selector = new Selector(5000, this.metrics, time, "MetricGroup", channelBuilder, new LogContext());
        this.mockBrokerInterceptor = mock(BrokerInterceptor.class);
        this.localhost = InetAddress.getLocalHost();
    }

    @Test
    public void testOnAuthenticatedConnectAndDisconnect() throws Exception {

        // Channel is connected, not ready at first and then becomes ready
        // It has a remote address, MultitenantInterceptor and an authenticated principal
        KafkaChannel kafkaChannel = mock(KafkaChannel.class);
        when(kafkaChannel.id()).thenReturn(CONNECTION_ID);
        when(kafkaChannel.finishConnect()).thenReturn(true);
        when(kafkaChannel.isConnected()).thenReturn(true);
        when(kafkaChannel.ready()).thenReturn(false).thenReturn(true);
        doNothing().when(kafkaChannel).prepare();
        when(kafkaChannel.successfulAuthentications()).thenReturn(1);
        when(kafkaChannel.interceptor()).thenReturn(mockBrokerInterceptor);
        when(kafkaChannel.socketAddress()).thenReturn(localhost);
        when(kafkaChannel.principal()).thenReturn(TEST_PRINCIPAL);

        SelectionKey selectionKey = mock(SelectionKey.class);
        when(kafkaChannel.selectionKey()).thenReturn(selectionKey);
        when(selectionKey.isValid()).thenReturn(true);

        selectionKey.attach(kafkaChannel);
        Set<SelectionKey> selectionKeys = Utils.mkSet(selectionKey);
        selector.pollSelectionKeys(selectionKeys, false, System.nanoTime());

        verify(mockBrokerInterceptor, Mockito.times(1)).onAuthenticatedConnection(CONNECTION_ID,
                localhost, TEST_PRINCIPAL, metrics);
        verify(mockBrokerInterceptor, Mockito.times(0)).onAuthenticatedDisconnection(CONNECTION_ID,
                localhost, TEST_PRINCIPAL, metrics);

        selector.close(kafkaChannel, Selector.CloseMode.DISCARD_NO_NOTIFY);

        verify(mockBrokerInterceptor, Mockito.times(1)).onAuthenticatedDisconnection(CONNECTION_ID,
                localhost, TEST_PRINCIPAL, metrics);
    }



}
