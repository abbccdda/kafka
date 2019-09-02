/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.kafka.security.fips.exceptions;

import org.apache.kafka.common.errors.SslAuthenticationException;

/*
 * This exception indicates that cipher suites configured are not FIPS compliant.
 * <p>
 * This exception will arise when non-FIPS compliant cipher suites configured.
 */

public class InvalidFipsTlsCipherSuiteException extends SslAuthenticationException {
    private static final long serialVersionUID = 1L;

    public InvalidFipsTlsCipherSuiteException(String message) {
        super(message);
    }

    public InvalidFipsTlsCipherSuiteException(String message, Throwable cause) {
        super(message, cause);
    }
}