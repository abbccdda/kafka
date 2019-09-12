/*
 Copyright 2019 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A request log filter provides a way to choose a subset of requests to be included
 * in the request log. This could be used to provide request sampling, such as in
 * {@link SamplingRequestLogFilter}, or more specific search criteria.
 *
 * Filters are not assumed to be thread-safe. Each Kafka request handler will
 * get a separate instance. However, it is not safe to assume that reconfiguration
 * (if supported) will be done in the same thread, so it still must be protected.
 */
public interface RequestLogFilter extends Reconfigurable {

    RequestLogFilter MATCH_NONE = new RequestLogFilter() {
        @Override
        public boolean shouldLogRequest(RequestContext ctx, long currentTimeNanos) {
            return false;
        }
    };

    boolean shouldLogRequest(RequestContext ctx, long currentTimeNanos);

    @Override
    default Set<String> reconfigurableConfigs() {
        return Collections.emptySet();
    }

    @Override
    default void validateReconfiguration(Map<String, ?> configs) throws ConfigException {

    }

    @Override
    default void reconfigure(Map<String, ?> configs) {

    }

    @Override
    default void configure(Map<String, ?> configs) {

    }

}
