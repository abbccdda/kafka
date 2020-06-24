/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.http.server.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Binding annotation for Inter Broker Listener endpoint.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface InterBrokerListener {
}
