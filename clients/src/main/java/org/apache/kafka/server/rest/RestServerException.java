// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.server.rest;

import org.apache.kafka.common.KafkaException;

public class RestServerException extends KafkaException {

    public RestServerException(String s) {
        super(s);
    }

    public RestServerException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public RestServerException(Throwable throwable) {
        super(throwable);
    }
}
