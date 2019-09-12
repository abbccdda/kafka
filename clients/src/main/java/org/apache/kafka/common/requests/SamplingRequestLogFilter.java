/*
 Copyright 2019 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This class implements simple adaptive request sampling method which attempts to select
 * samples evenly over the course of one minute using the relative rate from the previous
 * minute to estimate the interval.
 *
 * We allow both a default sample interval and per-API intervals to be configured. Any request
 * which does not match one of the overridden intervals will apply toward the default
 * interval. Conversely, if a request matches an overridden interval, then it does not
 * apply toward the default.
 */
public class SamplingRequestLogFilter implements RequestLogFilter {
    private static final Logger log = LoggerFactory.getLogger(SamplingRequestLogFilter.class);

    public static final String DEFAULT_SAMPLES_PER_MIN = "confluent.request.log.samples.per.min";
    public static final String OVERRIDE_API_SAMPLES_PER_MIN = "confluent.request.log.api.samples.per.min";

    private static final Set<String> RECONFIGURABLE_CONFIGS = new HashSet<>();
    static {
        RECONFIGURABLE_CONFIGS.add(DEFAULT_SAMPLES_PER_MIN);
        RECONFIGURABLE_CONFIGS.add(OVERRIDE_API_SAMPLES_PER_MIN);
    }

    private volatile TimeBasedSampler defaultSampler = null;
    private volatile EnumMap<ApiKeys, TimeBasedSampler> apiSamplers = new EnumMap<>(ApiKeys.class);

    @Override
    public boolean shouldLogRequest(RequestContext ctx, long requestTimeNanos) {
        TimeBasedSampler apiSampler = apiSamplers.get(ctx.header.apiKey());
        if (apiSampler != null)
            return apiSampler.maybeSample(requestTimeNanos);
        if (defaultSampler != null)
            return defaultSampler.maybeSample(requestTimeNanos);
        return false;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        configure(configs);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.debug("Received configuration: {}", configs);
        this.defaultSampler = maybeParseDefaultSampler(configs).orElse(null);
        this.apiSamplers = parseApiSamplers(configs);
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        maybeParseDefaultSampler(configs);
        parseApiSamplers(configs);
    }

    private EnumMap<ApiKeys, TimeBasedSampler> parseApiSamplers(Map<String, ?> configs) {
        EnumMap<ApiKeys, TimeBasedSampler> apiIntervals = new EnumMap<>(ApiKeys.class);

        Object configValue = configs.get(OVERRIDE_API_SAMPLES_PER_MIN);
        if (configValue == null)
            return apiIntervals;

        if (!(configValue instanceof String))
            throw new ConfigException("Invalid value `" + configValue + "` " +
                    "found for " + OVERRIDE_API_SAMPLES_PER_MIN + " (should be a string)");

        String apiSampleRates = (String) configValue;
        for (String apiSamplePairString : apiSampleRates.split(",")) {
            String[] apiSamplePair = apiSamplePairString.split("=");
            if (apiSamplePair.length != 2)
                throw new ConfigException("Invalid value `" + apiSamplePairString + "` " +
                        "found in " + OVERRIDE_API_SAMPLES_PER_MIN + "=`" + configValue + "`");

            ApiKeys apiKey = ApiKeys.findByName(apiSamplePair[0]);
            if (apiKey == null)
                throw new ConfigException("Invalid value `" + apiSamplePair[0] + "` " +
                        "found in " + OVERRIDE_API_SAMPLES_PER_MIN + "=`" + configValue + "`");

            try {
                long samplesPerMin = Long.valueOf(apiSamplePair[1]);
                apiIntervals.put(apiKey, perMinuteSampler(samplesPerMin));
            } catch (NumberFormatException e) {
                throw new ConfigException("Invalid value `" + apiSamplePair[1] + "` " +
                        "found in " + OVERRIDE_API_SAMPLES_PER_MIN + "=`" + configValue + "`");
            }
        }
        return apiIntervals;
    }

    private Optional<TimeBasedSampler> maybeParseDefaultSampler(Map<String, ?> configs) {
        Object samplesPerMin = configs.get(DEFAULT_SAMPLES_PER_MIN);
        if (samplesPerMin == null)
            return Optional.empty();
        if (samplesPerMin instanceof Number)
            return Optional.of(perMinuteSampler(((Number) samplesPerMin).longValue()));
        if (samplesPerMin instanceof String) {
            try {
                String samplesPerMinString = (String) samplesPerMin;
                if (samplesPerMinString.isEmpty())
                    return Optional.empty();
                return Optional.of(perMinuteSampler(Long.valueOf(samplesPerMinString)));
            } catch (NumberFormatException e) {
                throw new ConfigException("Invalid value `" + samplesPerMin + "` " +
                        "found in " + OVERRIDE_API_SAMPLES_PER_MIN);
            }
        }
        throw new ConfigException("Invalid default interval value `" + samplesPerMin + "`");
    }

    private static TimeBasedSampler perMinuteSampler(long samplesPerMin) {
        if (samplesPerMin == 0)
            return new NoOpSampler();
        else
            return new AdaptiveSampler(Duration.ofMinutes(1), samplesPerMin);
    }

    private interface TimeBasedSampler {
        /**
         * Check whether or not to sample a request.
         *
         * @param requestTimeMs The time that a request was received for potential sampling
         * @return true if the request should be logged
         */
        boolean maybeSample(long requestTimeMs);
    }

    private static class NoOpSampler implements TimeBasedSampler {

        @Override
        public boolean maybeSample(long requestTimeMs) {
            return false;
        }
    }

    private static class AdaptiveSampler implements TimeBasedSampler {
        private final long intervalDurationNanos;
        private final long samplesPerInterval;

        private long intervalStartNanos = 0L;
        private long numIntervalRequests = 0;
        private long numIntervalSamples = 0;
        private long estimatedRequestsPerSample = 1;

        AdaptiveSampler(Duration interval, long samplesPerInterval) {
            if (samplesPerInterval <= 0)
                throw new IllegalArgumentException("The number of samples per interval must be greater than 0");
            this.intervalDurationNanos = interval.toNanos();
            this.samplesPerInterval = samplesPerInterval;
        }

        @Override
        public boolean maybeSample(long requestTimeNanos) {
            if (requestTimeNanos - intervalStartNanos > intervalDurationNanos) {
                estimatedRequestsPerSample = Math.max(numIntervalRequests / samplesPerInterval, 1);
                numIntervalRequests = 0;
                numIntervalSamples = 0;
                intervalStartNanos = requestTimeNanos;
            }

            numIntervalRequests++;

            if (numIntervalSamples < samplesPerInterval && numIntervalRequests % estimatedRequestsPerSample == 0) {
                numIntervalSamples++;
                return true;
            }

            return false;
        }

    }

}
