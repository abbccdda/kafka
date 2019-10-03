/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.network.InvalidReceiveException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.security.auth.Subject;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.security.scram.internals.ScramMechanism.SCRAM_SHA_256;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SaslServerAuthenticatorTest {

    @Test(expected = InvalidReceiveException.class)
    public void testOversizeRequest() throws IOException {
        TransportLayer transportLayer = mock(TransportLayer.class);
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer, SCRAM_SHA_256.mechanismName());

        when(transportLayer.read(any(ByteBuffer.class))).then(invocation -> {
            invocation.<ByteBuffer>getArgument(0).putInt(SaslServerAuthenticator.MAX_RECEIVE_SIZE + 1);
            return 4;
        });
        authenticator.authenticate();
        verify(transportLayer).read(any(ByteBuffer.class));
    }

    private ApiVersionsResponse sendApiVersionsRequestAndReceiveResponse(
            TransportLayer transportLayer, SaslServerAuthenticator authenticator) throws IOException {
        final RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "clientId", 13243);
        final Struct headerStruct = header.toStruct();

        SocketChannel socketChannel = mock(SocketChannel.class);
        when(transportLayer.socketChannel()).thenReturn(socketChannel);

        Socket socket = mock(Socket.class);
        when(socketChannel.socket()).thenReturn(socket);
        when(socket.getInetAddress()).thenReturn(InetAddress.getLocalHost());

        when(transportLayer.read(any(ByteBuffer.class))).then(invocation -> {
            invocation.<ByteBuffer>getArgument(0).putInt(headerStruct.sizeOf());
            return 4;
        }).then(invocation -> {
            // serialize only the request header. the authenticator should not parse beyond this
            headerStruct.writeTo(invocation.getArgument(0));
            return headerStruct.sizeOf();
        });

        ArgumentCaptor<ByteBuffer[]> responseBuffersCaptor = ArgumentCaptor.forClass(ByteBuffer[].class);
        ByteBuffer responseBuffer = ByteBuffer.allocate(1024);

        when(transportLayer.write(responseBuffersCaptor.capture())).then(invocation -> {
            for (ByteBuffer buffer : responseBuffersCaptor.getValue())
                responseBuffer.put(buffer);
            return (long) responseBuffer.position();
        });

        authenticator.authenticate();

        responseBuffer.flip();

        int size = responseBuffer.getInt();
        assertTrue(size < responseBuffer.capacity());

        ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer, header.headerVersion());
        assertEquals(13243, responseHeader.correlationId());

        Struct struct = ApiKeys.API_VERSIONS.parseResponse((short) 0, responseBuffer);
        return (ApiVersionsResponse) AbstractResponse.parseResponse(ApiKeys.API_VERSIONS, struct, (short) 0);
    }

    private Set<ApiKeys> fetchSupportedApis(boolean isInterBrokerListener) throws IOException {
        TransportLayer transportLayer = mock(TransportLayer.class);
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer,
                SCRAM_SHA_256.mechanismName(), isInterBrokerListener);

        ApiVersionsResponse response = sendApiVersionsRequestAndReceiveResponse(transportLayer, authenticator);
        return response.apiVersions().stream()
                .map(version -> ApiKeys.forId(version.apiKey))
                .collect(Collectors.toSet());
    }

    @Test
    public void testExternalApiVersionRequestContainsNoInternalApis() throws IOException {
        assertEquals(ApiKeys.publicExposedApis(), fetchSupportedApis(false));

    }

    @Test
    public void testInterBrokerApiVersionRequestContainsInternalApis() throws IOException {
        assertEquals(ApiKeys.allApis(), fetchSupportedApis(true));
    }

    @Test
    public void testUnexpectedRequestType() throws IOException {
        TransportLayer transportLayer = mock(TransportLayer.class);
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer, SCRAM_SHA_256.mechanismName());

        final RequestHeader header = new RequestHeader(ApiKeys.METADATA, (short) 0, "clientId", 13243);
        final Struct headerStruct = header.toStruct();

        when(transportLayer.read(any(ByteBuffer.class))).then(invocation -> {
            invocation.<ByteBuffer>getArgument(0).putInt(headerStruct.sizeOf());
            return 4;
        }).then(invocation -> {
            // serialize only the request header. the authenticator should not parse beyond this
            headerStruct.writeTo(invocation.getArgument(0));
            return headerStruct.sizeOf();
        });

        try {
            authenticator.authenticate();
            fail("Expected authenticate() to raise an exception");
        } catch (IllegalSaslStateException e) {
            // expected exception
        }

        verify(transportLayer, times(2)).read(any(ByteBuffer.class));
    }

    private SaslServerAuthenticator setupAuthenticator(Map<String, ?> configs, TransportLayer transportLayer, String mechanism) {
        return setupAuthenticator(configs, transportLayer, mechanism, false);
    }

    private SaslServerAuthenticator setupAuthenticator(
            Map<String, ?> configs,
            TransportLayer transportLayer,
            String mechanism,
            boolean isInterBrokerListener) {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), new HashMap<String, Object>());
        Map<String, JaasContext> jaasContexts = Collections.singletonMap(mechanism,
                new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null));
        Map<String, Subject> subjects = Collections.singletonMap(mechanism, new Subject());
        Map<String, AuthenticateCallbackHandler> callbackHandlers = Collections.singletonMap(
                mechanism, new SaslServerCallbackHandler());
        return new SaslServerAuthenticator(configs, callbackHandlers, "node", subjects, null,
                new ListenerName("ssl"), isInterBrokerListener, SecurityProtocol.SASL_SSL, transportLayer, Collections.emptyMap(), Time.SYSTEM);
    }

}
