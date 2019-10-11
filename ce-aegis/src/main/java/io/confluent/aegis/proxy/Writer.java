// Copyright 2018, Confluent

package io.confluent.aegis.proxy;

import io.confluent.aegis.common.ByteBufAccessor;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.slf4j.Logger;

/**
 * Writes a Kafka message to a channel.
 */
public class Writer extends ChannelOutboundHandlerAdapter {
    private final static int FRAME_LENGTH = 4;

    private final ObjectSerializationCache serializationCache = new ObjectSerializationCache();
    private Logger log;

    Writer(Logger log) {
        this.log = log;
    }

    void setLogger(Logger log) {
        this.log = log;
    }

    Logger log() {
        return log;
    }

    final void writeHeader(ChannelHandlerContext ctx, ChannelPromise promise,
                           Message message, int frameLength) {
        int length = message.size(serializationCache, (short) 0);
        ByteBuf buf = ctx.alloc().directBuffer(length + FRAME_LENGTH);
        ByteBufAccessor accessor = new ByteBufAccessor(buf);
        accessor.writeInt(frameLength);
        message.write(accessor, serializationCache, (short) 0);
        log.trace("Writing {} with frame length {}.", message, frameLength);
        ctx.write(buf, promise);
    }

    final void writeMessage(ChannelHandlerContext ctx, ChannelPromise promise,
                            Message message, short version) {
        int length = message.size(serializationCache, version);
        ByteBuf buf = ctx.alloc().directBuffer(length);
        ByteBufAccessor accessor = new ByteBufAccessor(buf);
        message.write(accessor, serializationCache, version);
        log.trace("Writing {} byte message.", length);
        ctx.write(buf, promise);
    }
}
