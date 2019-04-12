// Copyright 2018, Confluent

package io.confluent.aegis.proxy;

import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;

/**
 * Reads the Kafka responses sent back from the broker.
 */
public class BrokerReader extends Reader {
    private final Flow flow;

    private final BrokerWriter writer;

    private RequestHeaderData header;

    private ApiMessage response;

    BrokerReader(Flow flow, BrokerWriter writer) {
        // Until the channelActive callback is invoked, we don't know what
        // the remote and local addresses are for this handler.  So just set
        // the log to flow.log() for now.
        super(flow.log());
        this.flow = flow;
        this.writer = writer;
        this.header = null;
        this.response = null;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        setLogger(new LogContext(String.format("Flow-%d-%d: %s<=%s: ",
            flow.id(),
            flow.brokerIndex(),
            ctx.channel().localAddress(),
            ctx.channel().remoteAddress())).logger(BrokerReader.class));
        writer.setLogger(new LogContext(String.format("Flow-%d-%d: %s=>%s: ",
            flow.id(),
            flow.brokerIndex(),
            ctx.channel().localAddress(),
            ctx.channel().remoteAddress())).logger(BrokerWriter.class));
        writer.log().info("Connected.");
        super.channelActive(ctx);
    }

    @Override
    void tryReadStructuredData(ChannelHandlerContext ctx) {
        if (header == null) {
            ResponseHeaderData respHeader = new ResponseHeaderData();
            if (!readMessage(respHeader, (short) 0)) {
                return;
            }
            RequestHeaderData nextHeader = flow.
                findAndRemovePending(respHeader.correlationId());
            if (nextHeader == null) {
                throw new RuntimeException("Can't find any request corresponding " +
                        "to correlation ID " + respHeader.correlationId());
            }
            this.header = nextHeader;
            this.response = ApiMessageType.fromApiKey(header.requestApiKey()).newResponse();
            flow.sendToTenant(new FrameStart(frameLength(), header));
        }
        if (!readMessage(this.response, header.requestApiVersion())) {
            return;
        }
        if (log().isTraceEnabled()) {
            log().trace("Read {}", this.response);
        }
        Message curRequest = response;
        this.header = null;
        this.response = null;
        flow.sendToTenant(curRequest);
        flow.sendToTenant(FrameEnd.INSTANCE);
    }

    @Override
    void reset() {
        super.reset();
        this.header = null;
        this.response = null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log().info("Error reading response from broker.", cause);
        flow.close();
    }
}
