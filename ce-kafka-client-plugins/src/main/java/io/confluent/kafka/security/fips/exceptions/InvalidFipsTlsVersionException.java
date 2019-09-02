/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.kafka.security.fips.exceptions;

import org.apache.kafka.common.errors.SslAuthenticationException;

/*
 * This exception indicates that SSL/TLS protocol versions are not FIPS compliant.
 * <p>
 * This exception will arise when non-FIPS compliant TLS versions configured.
 */

public class InvalidFipsTlsVersionException extends SslAuthenticationException {
    private static final long serialVersionUID = 1L;

    public InvalidFipsTlsVersionException(String message) {
        super(message);
    }

    public InvalidFipsTlsVersionException(String message, Throwable cause) {
        super(message, cause);
    }
}