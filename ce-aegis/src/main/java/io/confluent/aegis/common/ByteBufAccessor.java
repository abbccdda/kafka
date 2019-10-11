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

package io.confluent.aegis.common;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.utils.ByteUtils;

public class ByteBufAccessor implements Readable, Writable {
    private final ByteBuf buf;

    public ByteBufAccessor(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public byte readByte() {
        return buf.readByte();
    }

    @Override
    public short readShort() {
        return buf.readShort();
    }

    @Override
    public int readInt() {
        return buf.readInt();
    }

    @Override
    public long readLong() {
        return buf.readLong();
    }

    @Override
    public void readArray(byte[] arr) {
        buf.readBytes(arr);
    }

    @Override
    public int readUnsignedVarint() {
        return ByteUtils.readUnsignedVarint(buf);
    }

    @Override
    public void writeByte(byte val) {
        buf.writeByte(val);
    }

    @Override
    public void writeShort(short val) {
        buf.writeShort(val);
    }

    @Override
    public void writeInt(int val) {
        buf.writeInt(val);
    }

    @Override
    public void writeLong(long val) {
        buf.writeLong(val);
    }

    @Override
    public void writeByteArray(byte[] arr) {
        buf.writeBytes(arr);
    }

    @Override
    public void writeUnsignedVarint(int i) {
        ByteUtils.writeUnsignedVarint(i, buf);
    }
}
