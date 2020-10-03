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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.EnvelopeRequestData;
import org.apache.kafka.common.message.EnvelopeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;

import java.nio.ByteBuffer;

public class EnvelopeRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<EnvelopeRequest> {

        private final EnvelopeRequestData data;

        public Builder(ByteBuffer embedData) {
            this(embedData, null);
        }

        public Builder(ByteBuffer embedData,
                       ByteBuffer serializedPrincipal) {
            super(ApiKeys.ENVELOPE);
            this.data = new EnvelopeRequestData()
                            .setRequestData(embedData)
                            .setRequestPrincipal(serializedPrincipal);
        }

        @Override
        public EnvelopeRequest build(short version) {
            return new EnvelopeRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final EnvelopeRequestData data;

    public EnvelopeRequest(EnvelopeRequestData data, short version) {
        super(ApiKeys.ENVELOPE, version);
        this.data = data;
    }

    public ByteBuffer requestData() {
        return data.requestData();
    }

    public String clientHostName() {
        return data.clientHostName();
    }

    public KafkaPrincipal requestPrincipal(KafkaPrincipalSerde principalSerde) {
        return principalSerde.deserialize(data.requestPrincipal().array(), version());
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new EnvelopeResponse(new EnvelopeResponseData()
                                        .setErrorCode(Errors.forException(e).code()));
    }
}
