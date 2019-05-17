// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.rest;

import io.confluent.security.auth.client.rest.exceptions.RestClientException;

import java.io.Closeable;
import java.io.IOException;

public interface RequestSender extends Closeable {
    /**
     * @param request    An instance of RestRequest
     * @param requestTimeout    request timeout
     * @return The deserialized response to the HTTP request, or null if no data is expected.
     */

    <T> T send(RestRequest request, final long requestTimeout) throws IOException, RestClientException;
}
