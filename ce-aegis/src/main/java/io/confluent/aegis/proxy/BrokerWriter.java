// Copyright 2018, Confluent

package io.confluent.aegis.proxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.kafka.common.protocol.Message;

/**
 * Writes Kafka requests to the broker.
 */
public class BrokerWriter extends Writer {
    private final Flow flow;

    private FrameStart frameStart;

    BrokerWriter(Flow flow) {
        super(flow.log());
        this.flow = flow;
        this.frameStart = null;
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
            writeHeader(ctx, promise, frameStart.header(), frameStart.totalLength());
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

    @SuppressWarnings("deprecation")
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log().info("Error writing response to the broker.", cause);
        flow.close();
    }
}
