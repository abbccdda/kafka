/*
 Copyright 2019 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SamplingRequestLogFilterTest {
    private final MockTime time = new MockTime();

    private static class Request {
        private final RequestContext context;
        private final long time;

        private Request(RequestContext context, long time) {
            this.context = context;
            this.time = time;
        }
    }

    @Test
    public void testDefaultSampling() {
        SamplingRequestLogFilter filter = new SamplingRequestLogFilter();
        filter.configure(singletonMap(SamplingRequestLogFilter.DEFAULT_SAMPLES_PER_MIN, 10));

        // Generate one request every second
        Iterator<Request> generator = fixedIntervalRequestGenerator(ApiKeys.METADATA, Duration.ofSeconds(1)).iterator();

        // In the first minute, we have no rate expectation, so we just take the first 10 samples
        List<Request> firstMinute = sampleForDuration(filter, generator, Duration.ofMinutes(1))
                .collect(Collectors.toList());
        assertEquals(10, firstMinute.size());
        assertSampleInterval(firstMinute, Duration.ofSeconds(1));

        // From then on, we take evenly spaced samples assuming the previous rate
        List<Request> secondMinute = sampleForDuration(filter, generator, Duration.ofMinutes(1))
                .collect(Collectors.toList());
        assertEquals(10, secondMinute.size());
        assertSampleInterval(secondMinute, Duration.ofSeconds(6));
    }

    @Test
    public void testOverrideSamplingNoDefault() {
        SamplingRequestLogFilter filter = new SamplingRequestLogFilter();
        filter.configure(singletonMap(SamplingRequestLogFilter.OVERRIDE_API_SAMPLES_PER_MIN, "Metadata=10,Produce=6"));

        Iterator<Request> generator = joinAll(Arrays.asList(
                fixedIntervalRequestGenerator(ApiKeys.METADATA, Duration.ofSeconds(1)),
                fixedIntervalRequestGenerator(ApiKeys.PRODUCE, Duration.ofSeconds(5)),
                fixedIntervalRequestGenerator(ApiKeys.FETCH, Duration.ofSeconds(1)))
        ).iterator();

        List<Request> firstMinute = sampleForDuration(filter, generator, Duration.ofMinutes(1))
                .collect(Collectors.toList());
        assertEquals(16, firstMinute.size());

        List<Request> firstMinuteSampledProduceRequests = firstMinute.stream()
                .filter(req -> req.context.header.apiKey() == ApiKeys.PRODUCE)
                .collect(Collectors.toList());
        List<Request> firstMinuteSampledMetadataRequests = firstMinute.stream()
                .filter(req -> req.context.header.apiKey() == ApiKeys.METADATA)
                .collect(Collectors.toList());

        assertEquals(6, firstMinuteSampledProduceRequests.size());
        assertEquals(10, firstMinuteSampledMetadataRequests.size());
        assertSampleInterval(firstMinuteSampledProduceRequests, Duration.ofSeconds(5));
        assertSampleInterval(firstMinuteSampledMetadataRequests, Duration.ofSeconds(1));

        List<Request> secondMinute = sampleForDuration(filter, generator, Duration.ofMinutes(1))
                .collect(Collectors.toList());
        assertEquals(16, secondMinute.size());

        List<Request> secondMinuteSampledProduceRequests = secondMinute.stream()
                .filter(req -> req.context.header.apiKey() == ApiKeys.PRODUCE)
                .collect(Collectors.toList());
        List<Request> secondMinuteSampledMetadataRequests = secondMinute.stream()
                .filter(req -> req.context.header.apiKey() == ApiKeys.METADATA)
                .collect(Collectors.toList());

        assertEquals(6, secondMinuteSampledProduceRequests.size());
        assertEquals(10, secondMinuteSampledMetadataRequests.size());
        assertSampleInterval(secondMinuteSampledProduceRequests, Duration.ofSeconds(10));
        assertSampleInterval(secondMinuteSampledMetadataRequests, Duration.ofSeconds(6));
    }

    @Test
    public void testDisabledApi() {
        SamplingRequestLogFilter filter = new SamplingRequestLogFilter();
        Map<String, Object> configs = new HashMap<>();
        configs.put(SamplingRequestLogFilter.OVERRIDE_API_SAMPLES_PER_MIN, "Metadata=0");
        configs.put(SamplingRequestLogFilter.DEFAULT_SAMPLES_PER_MIN, "10");
        filter.configure(configs);

        Iterator<Request> generator = joinAll(Arrays.asList(
                fixedIntervalRequestGenerator(ApiKeys.METADATA, Duration.ofSeconds(1)),
                fixedIntervalRequestGenerator(ApiKeys.PRODUCE, Duration.ofSeconds(5)),
                fixedIntervalRequestGenerator(ApiKeys.FETCH, Duration.ofSeconds(1)))
        ).iterator();

        List<Request> firstMinute = sampleForDuration(filter, generator, Duration.ofMinutes(1))
                .collect(Collectors.toList());
        assertEquals(10, firstMinute.size());
        assertTrue(firstMinute.stream().allMatch(req -> req.context.header.apiKey() != ApiKeys.METADATA));
    }

    @Test
    public void testOverrideSamplingWithDefault() {
        SamplingRequestLogFilter filter = new SamplingRequestLogFilter();

        Map<String, Object> configs = new HashMap<>();
        configs.put(SamplingRequestLogFilter.OVERRIDE_API_SAMPLES_PER_MIN, "Produce=6");
        configs.put(SamplingRequestLogFilter.DEFAULT_SAMPLES_PER_MIN, "10");
        filter.configure(configs);

        Iterator<Request> generator = joinAll(Arrays.asList(
                fixedIntervalRequestGenerator(ApiKeys.METADATA, Duration.ofSeconds(2)),
                fixedIntervalRequestGenerator(ApiKeys.PRODUCE, Duration.ofSeconds(5)),
                fixedIntervalRequestGenerator(ApiKeys.FETCH, Duration.ofSeconds(1)))
        ).iterator();

        List<Request> firstMinute = sampleForDuration(filter, generator, Duration.ofMinutes(1))
                .collect(Collectors.toList());
        assertEquals(16, firstMinute.size());

        List<Request> secondMinuteSampledProduceRequests = firstMinute.stream()
                .filter(req -> req.context.header.apiKey() == ApiKeys.PRODUCE)
                .collect(Collectors.toList());
        List<Request> secondMinuteSampledDefaultRequests = firstMinute.stream()
                .filter(req -> req.context.header.apiKey() != ApiKeys.PRODUCE)
                .collect(Collectors.toList());

        assertEquals(6, secondMinuteSampledProduceRequests.size());
        assertEquals(10, secondMinuteSampledDefaultRequests.size());
    }

    @Test
    public void testInvalidDefaultConfigs() {
        assertThrowsConfigException(singletonMap(SamplingRequestLogFilter.DEFAULT_SAMPLES_PER_MIN, "blah"));
        assertThrowsConfigException(singletonMap(SamplingRequestLogFilter.DEFAULT_SAMPLES_PER_MIN, "null"));
        assertThrowsConfigException(singletonMap(SamplingRequestLogFilter.DEFAULT_SAMPLES_PER_MIN, "1.3"));

        assertThrowsConfigException(singletonMap(SamplingRequestLogFilter.OVERRIDE_API_SAMPLES_PER_MIN, "MD=5"));
        assertThrowsConfigException(singletonMap(SamplingRequestLogFilter.OVERRIDE_API_SAMPLES_PER_MIN, "Metadata=5,Fetch=dd"));
        assertThrowsConfigException(singletonMap(SamplingRequestLogFilter.OVERRIDE_API_SAMPLES_PER_MIN, "Metadata=5,blah,"));
    }

    private void assertThrowsConfigException(Map<String, ?> configs) {
        SamplingRequestLogFilter filter = new SamplingRequestLogFilter();
        assertThrows(ConfigException.class, () -> filter.validateReconfiguration(configs));
        assertThrows(ConfigException.class, () -> filter.configure(configs));
        assertThrows(ConfigException.class, () -> filter.reconfigure(configs));
    }

    private RequestContext newRequestContext(ApiKeys apiKey) {
        RequestHeader header = new RequestHeader(apiKey, apiKey.latestVersion(), "clientId", 1);
        return new RequestContext(header,
                "cxnId",
                InetAddress.getLoopbackAddress(),
                KafkaPrincipal.ANONYMOUS,
                new ListenerName("PLAINTEXT"),
                SecurityProtocol.PLAINTEXT);
    }


    private Stream<Request> fixedIntervalRequestGenerator(ApiKeys api, Duration interval) {
        AtomicLong nextArrival = new AtomicLong(time.nanoseconds());
        return Stream.generate(() -> {
            Request request = new Request(newRequestContext(api), nextArrival.get());
            nextArrival.set(request.time + interval.toNanos());
            return request;
        });
    }

    private Stream<Request> sampleForDuration(SamplingRequestLogFilter sampler,
                                              Iterator<Request> requestGenerator,
                                              Duration duration) {
        long endTimeMs = time.milliseconds() + duration.toMillis();
        Stream.Builder<Request> streamBuilder = Stream.builder();
        while (time.milliseconds() <= endTimeMs) {
            Request request = requestGenerator.next();
            long requestTimeNanos = Math.max(0, request.time - time.nanoseconds());
            time.sleep(Duration.ofNanos(requestTimeNanos).toMillis());

            if (sampler.shouldLogRequest(request.context, request.time))
                streamBuilder.accept(request);
        }
        return streamBuilder.build();
    }

    private Stream<Long> sampleIntervals(List<Request> samples) {
        Stream.Builder<Long> streamBuilder = Stream.builder();
        Long lastSampleTime = null;
        for (Request sample : samples) {
            if (lastSampleTime != null)
                streamBuilder.accept(sample.time - lastSampleTime);
            lastSampleTime = sample.time;
        }
        return streamBuilder.build();
    }

    private void assertSampleInterval(List<Request> samples, Duration expectedInterval) {
        assertTrue(sampleIntervals(samples)
                .allMatch(intervalNanos -> Duration.ofNanos(intervalNanos).equals(expectedInterval)));
    }

    private Stream<Request> join(Stream<Request> s1, Stream<Request> s2) {
        AtomicReference<Request> next1 = new AtomicReference<>();
        AtomicReference<Request> next2 = new AtomicReference<>();
        Iterator<Request> iter1 = s1.iterator();
        Iterator<Request> iter2 = s2.iterator();

        return Stream.generate(() -> {
            Request r1 = next1.getAndSet(null);
            if (r1 == null)
                r1 = iter1.next();

            Request r2 = next2.getAndSet(null);
            if (r2 == null)
                r2 = iter2.next();

            if (r1.time < r2.time) {
                next2.set(r2);
                return r1;
            } else {
                next1.set(r1);
                return r2;
            }
        });
    }

    private Stream<Request> joinAll(Collection<Stream<Request>> streams) {
        Iterator<Stream<Request>> iter = streams.iterator();

        if (!iter.hasNext())
            return Stream.empty();

        Stream<Request> all = iter.next();
        while (iter.hasNext())
            all = join(all, iter.next());

        return all;
    }

}