// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.interceptor;

import org.apache.kafka.common.metrics.Metrics;

public interface Monitorable {
    void registerMetrics(Metrics metrics);
}
