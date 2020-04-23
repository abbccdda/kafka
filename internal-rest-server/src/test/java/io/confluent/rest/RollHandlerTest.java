/*
 Copyright 2020 Confluent Inc.
 */

package io.confluent.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import static org.mockito.BDDMockito.given;

public class RollHandlerTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static class MockBrokerShutdownHandle implements BeginShutdownBrokerHandle {
        long brokerId;
        long brokerEpoch;
        Integer controllerId;
        long underReplicatedPartitions;
        boolean shutdownTriggered = false;
        @Override
        public long brokerId() {
            return brokerId;
        }

        @Override
        public long brokerEpoch() {
            return brokerEpoch;
        }

        @Override
        public Integer controllerId() {
            return controllerId;
        }

        @Override
        public long underReplicatedPartitions() {
            return underReplicatedPartitions;
        }

        @Override
        public void beginShutdown(long brokerEpoch) throws StaleBrokerEpochException {
            if (this.brokerEpoch != brokerEpoch) {
                throw new StaleBrokerEpochException("");
            } else {
                shutdownTriggered = true;
            }
        }
    }

    private static class MockServletOutputStream extends ServletOutputStream {
        public final ByteArrayOutputStream baos;

        MockServletOutputStream() {
            this.baos = new ByteArrayOutputStream();
        }

        public void write(int i) {
            baos.write(i);
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) { }
    }

    private static class MockServletInputStream extends ServletInputStream {
        public final ByteArrayInputStream bais;

        MockServletInputStream(ByteArrayInputStream bais) {
            this.bais = bais;
        }

        @Override
        public boolean isFinished() {
            return bais.available() > 0;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(ReadListener readListener) { }

        @Override
        public int read() {
            return bais.read();
        }
    }

    @Test
    public void testStatusReport() throws IOException {
        MockBrokerShutdownHandle handle = new MockBrokerShutdownHandle();
        handle.brokerId = 10;
        handle.brokerEpoch = 42;
        handle.underReplicatedPartitions = 1;
        handle.controllerId = 2;
        RollHandler rollHandler = new RollHandler(handle);
        MockServletOutputStream outputStream = new MockServletOutputStream();
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        given(response.getOutputStream()).willReturn(outputStream);

        rollHandler.handle("/status", null, request, response);
        ResponseContainer<RollHandler.StatusResponse> statusResponse =
                OBJECT_MAPPER.readValue(outputStream.baos.toByteArray(),
                        new TypeReference<ResponseContainer<RollHandler.StatusResponse>>() { });

        Assert.assertEquals(handle.brokerId, statusResponse.data.attributes.brokerId);
        Assert.assertEquals(handle.brokerEpoch, statusResponse.data.attributes.brokerEpoch);
        Assert.assertEquals(handle.underReplicatedPartitions,
                 statusResponse.data.attributes.underReplicatedPartitions);
        Assert.assertEquals(handle.controllerId, statusResponse.data.attributes.controllerId);
    }

    @Test
    public void testStatusReportNoController() throws IOException {
        MockBrokerShutdownHandle handle = new MockBrokerShutdownHandle();
        handle.brokerId = 10;
        handle.brokerEpoch = 42;
        handle.underReplicatedPartitions = 1;
        handle.controllerId = null;
        RollHandler rollHandler = new RollHandler(handle);
        MockServletOutputStream outputStream = new MockServletOutputStream();
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        given(response.getOutputStream()).willReturn(outputStream);

        rollHandler.handle("/status", null, request, response);
        ResponseContainer<RollHandler.StatusResponse> statusResponse =
                OBJECT_MAPPER.readValue(outputStream.baos.toByteArray(),
                        new TypeReference<ResponseContainer<RollHandler.StatusResponse>>() { });

        Assert.assertEquals(handle.brokerId, statusResponse.data.attributes.brokerId);
        Assert.assertEquals(handle.brokerEpoch, statusResponse.data.attributes.brokerEpoch);
        Assert.assertEquals(handle.underReplicatedPartitions,
                 statusResponse.data.attributes.underReplicatedPartitions);
        Assert.assertEquals(handle.controllerId, statusResponse.data.attributes.controllerId);
    }

    @Test
    public void testBeginShutdown() throws IOException {
        MockBrokerShutdownHandle handle = new MockBrokerShutdownHandle();
        handle.brokerId = 10;
        handle.brokerEpoch = 42;
        handle.underReplicatedPartitions = 1;
        handle.controllerId = 2;
        RollHandler rollHandler = new RollHandler(handle);

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        RollHandler.ShutdownRequest shutdownRequest =
                new RollHandler.ShutdownRequest(handle.brokerId, handle.brokerEpoch);
        byte[] shutdownRequestBytes = OBJECT_MAPPER.writeValueAsBytes(shutdownRequest);
        MockServletInputStream inputStream =
                 new MockServletInputStream(new ByteArrayInputStream(shutdownRequestBytes));

        given(response.getWriter()).willReturn(new PrintWriter(new ByteArrayOutputStream()));
        given(request.getInputStream()).willReturn(inputStream);

        rollHandler.handle("/shutdown", null, request, response);
        Assert.assertTrue(handle.shutdownTriggered);
    }

    @Test
    public void testBeginShutdownIncorrectEpoch() throws IOException {
        MockBrokerShutdownHandle handle = new MockBrokerShutdownHandle();
        handle.brokerId = 10;
        handle.brokerEpoch = 42;
        handle.underReplicatedPartitions = 1;
        handle.controllerId = 2;
        RollHandler rollHandler = new RollHandler(handle);

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        MockServletOutputStream outputStream = new MockServletOutputStream();

        RollHandler.ShutdownRequest shutdownRequest = // Set an incorrect epoch
                new RollHandler.ShutdownRequest(handle.brokerId, handle.brokerEpoch - 1);

        byte[] shutdownRequestBytes = OBJECT_MAPPER.writeValueAsBytes(shutdownRequest);
        MockServletInputStream inputStream =
                 new MockServletInputStream(new ByteArrayInputStream(shutdownRequestBytes));

        given(response.getWriter()).willReturn(new PrintWriter(new ByteArrayOutputStream()));
        given(response.getOutputStream()).willReturn(outputStream);
        given(request.getInputStream()).willReturn(inputStream);

        rollHandler.handle("/shutdown", null, request, response);

        ResponseContainer<RollHandler.StatusResponse> statusResponse =
                OBJECT_MAPPER.readValue(outputStream.baos.toByteArray(),
                        new TypeReference<ResponseContainer<RollHandler.StatusResponse>>() { });
        Assert.assertEquals("expected no data in response", statusResponse.data, null);
        Assert.assertEquals("expected one error struct", statusResponse.errors.size(), 1);

        Assert.assertEquals(statusResponse.errors.get(0).status, 500);
        Assert.assertEquals(statusResponse.errors.get(0).id, 0);
    }
}