/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.clients;

import java.nio.ByteBuffer;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;

public interface ClientInterceptor extends Configurable {

    /**
     * Serializes the specified request header and request body to create a
     * {@link Send} instance containing the request for sending to a broker.
     *
     * @param requestHeader Request header that will be added to the returned Send
     * @param requestBody Request body that will be added to the returned Send
     * @param destination Destination connection id
     * @return Serialized request for sending to broker
     */
    Send toSend(RequestHeader requestHeader, AbstractRequest requestBody, String destination);

    /**
     * Parses response body from the given buffer.
     *
     * @param responseBuffer Buffer containing serialized response
     * @param requestHeader Request header that specifies request API key and version
     * @return Response body as a {@link Struct}
     */
    Struct parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader);
}
