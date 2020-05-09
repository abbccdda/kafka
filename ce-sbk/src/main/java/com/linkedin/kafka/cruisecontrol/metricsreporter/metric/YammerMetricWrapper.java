/*
 Copyright 2019 Confluent Inc.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import java.util.Map;

/**
 * This class wraps a Yammer MetricName object in order to allow storing the computed tags
 */
public class YammerMetricWrapper {
  private com.yammer.metrics.core.MetricName _metricName;
  private Map<String, String> _tags;

  public YammerMetricWrapper(com.yammer.metrics.core.MetricName metricName) {
    this._metricName = metricName;
  }

  public com.yammer.metrics.core.MetricName metricName() {
    return this._metricName;
  }

  public Map<String, String> tags() {
    if (this._tags == null) {
      this._tags = MetricsUtils.yammerMetricScopeToTags(_metricName.getScope());
    }

    return this._tags;
  }
}
