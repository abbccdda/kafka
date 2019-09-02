/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.kafka.security.fips.exceptions;

import org.apache.kafka.common.errors.SslAuthenticationException;

/*
 * This exception indicates that kafka broker protocols configured are not FIPS compliant.
 * <p>
 * This exception will arise when non-FIPS compliant kafka broker protocols configured.
 */

public class InvalidFipsBrokerProtocolException extends SslAuthenticationException {
    private static final long serialVersionUID = 1L;

    public InvalidFipsBrokerProtocolException(String message) {
        super(message);
    }

    public InvalidFipsBrokerProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}