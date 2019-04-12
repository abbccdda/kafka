// Copyright 2018, Confluent

package io.confluent.aegis.proxy;

import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.RequestHeaderData;

import java.net.SocketAddress;

/**
 * Reads Kafka requests received from the tenant.
 */
public class TenantReader extends Reader {
    private final Flow flow;

    private RequestHeaderData header;

    private ApiMessage request;

    TenantReader(Flow flow, SocketAddress localAddress, SocketAddress remoteAddress) {
        super(new LogContext(String.format("Flow-%d-%d: %s<-%s: ",
                flow.id(),
                flow.brokerIndex(),
                localAddress,
                remoteAddress)).logger(TenantReader.class));
        this.flow = flow;
    }

    @Override
    void tryReadStructuredData(ChannelHandlerContext ctx) throws Exception {
        if (header == null) {
            RequestHeaderData nextHeader = new RequestHeaderData();
            if (!readMessage(nextHeader, (short) 0)) {
                return;
            }
            this.header = nextHeader;
            this.request = ApiMessageType.fromApiKey(header.requestApiKey()).newRequest();
            if (log().isTraceEnabled()) {
                log().trace("Read tenant request header {}.", this.header);
            }
            flow.sendToBroker(new FrameStart(frameLength(), header));
        }
        if (!readMessage(this.request, header.requestApiVersion())) {
            return;
        }
        if (log().isTraceEnabled()) {
            log().trace("Read {}", this.request);
        }
        Message curRequest = request;
        reset();
        flow.sendToBroker(curRequest);
        flow.sendToBroker(FrameEnd.INSTANCE);
    }

    @Override
    void reset() {
        super.reset();
        this.header = null;
        this.request = null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log().info("Error reading request from tenant.", cause);
        flow.close();
    }
}
