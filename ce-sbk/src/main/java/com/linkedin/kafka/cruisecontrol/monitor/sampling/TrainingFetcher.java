/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import java.util.Set;

import com.yammer.metrics.core.TimerContext;
import org.apache.kafka.common.TopicPartition;


/**
 * A metric fetcher that is responsible for fetching the partition metric samples for model training.
 */
class TrainingFetcher extends MetricFetcher {
  private final MetricSampler _metricSampler;
  private final SampleStore _sampleStore;
  private final Set<TopicPartition> _assignedPartitions;
  private final long _startTimeMs;
  private final long _endTimeMs;
  private final Timer _fetcherTimer;
  private final MetadataClient _metadataClient;
  private final Meter _fetcherFailureRate;
  private final MetricDef _metricDef;
  private final long _timeout;

  TrainingFetcher(MetricSampler metricSampler,
                  MetadataClient metadataClient,
                  SampleStore sampleStore,
                  Set<TopicPartition> assignedPartitions,
                  long startTimeMs,
                  long endTimeMs,
                  MetricDef metricDef,
                  Timer fetcherTimer,
                  Meter fetcherFailureRate) {
    _sampleStore = sampleStore;
    _metricSampler = metricSampler;
    _metadataClient = metadataClient;
    _startTimeMs = startTimeMs;
    _endTimeMs = endTimeMs;
    _assignedPartitions = assignedPartitions;
    _metricDef = metricDef;
    _fetcherTimer = fetcherTimer;
    _fetcherFailureRate = fetcherFailureRate;
    _timeout = System.currentTimeMillis() + (endTimeMs - startTimeMs) / 2;
  }

  @Override
  protected void fetchMetricsForAssignedPartitions() throws MetricSamplingException {
    final TimerContext ctx = _fetcherTimer.time();

    try {
      MetricSampler.Samples samples =
          _metricSampler.getSamples(_metadataClient.cluster(), _assignedPartitions, _startTimeMs, _endTimeMs,
                                    MetricSampler.SamplingMode.BROKER_METRICS_ONLY, _metricDef, _timeout);
      ModelParameters.addMetricObservation(samples.brokerMetricSamples());

      _sampleStore.storeSamples(samples);
    } catch (Exception e) {
      _fetcherFailureRate.mark();
      throw e;
    } finally {
      ctx.stop();
    }
  }
}
