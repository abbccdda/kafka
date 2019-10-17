// Copyright 2018, Confluent

package io.confluent.aegis.proxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.message.ResponseHeaderData;

import java.net.SocketAddress;

/**
 * Writes Kafka responses to the tenant.
 */
public class TenantWriter extends Writer {
    private final Flow flow;

    private FrameStart frameStart;

    TenantWriter(Flow flow, SocketAddress localAddress, SocketAddress remoteAddress) {
        super(new LogContext(String.format("Flow-%d-%d: %s->%s: ",
            flow.id(),
            flow.brokerIndex(),
            localAddress,
            remoteAddress)).logger(TenantReader.class));
        this.flow = flow;
        this.frameStart = null;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log().info("Error writing response to the tenant.", cause);
        flow.close();
    }

    @Override
    public final void write(ChannelHandlerContext ctx, Object in, ChannelPromise promise)
            throws Exception {
        if (in instanceof FrameStart) {
            @SuppressWarnings("unchecked")
            FrameStart frameStart = (FrameStart) in;
            if (this.frameStart != null) {
                throw new IllegalStateException("Frame start sent while another frame was " +
                    "in progress.");
            }
            this.frameStart = frameStart;
            // Write the response header.
            ResponseHeaderData header = new ResponseHeaderData().
                setCorrelationId(frameStart.header().correlationId());
            writeHeader(ctx, promise, header, frameStart.totalLength());
        } else if (in instanceof Message) {
            @SuppressWarnings("unchecked")
            Message message = (Message) in;
            if (frameStart == null) {
                throw new RuntimeException("Message sent with no frame in progress.");
            }
            writeMessage(ctx, promise, message, frameStart.header().requestApiVersion());
        } else if (in instanceof FrameEnd) {
            if (this.frameStart == null) {
                throw new RuntimeException("Frame end sent with no frame in progress.");
            }
            log().trace("Flushing.");
            ctx.flush();
            this.frameStart = null;
        } else {
            throw new RuntimeException("Received unknown object " + in);
        }
    }
}
