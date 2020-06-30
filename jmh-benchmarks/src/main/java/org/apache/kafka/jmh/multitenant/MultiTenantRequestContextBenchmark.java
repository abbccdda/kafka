/*
 Copyright 2020 Confluent Inc.
 */

package org.apache.kafka.jmh.multitenant;

import io.confluent.kafka.multitenant.MultiTenantInterceptorConfig;
import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.MultiTenantRequestContext;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.multitenant.metrics.TenantMetrics;
import java.util.HashMap;
import java.util.Map;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestInternals;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SystemTime;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)

public class MultiTenantRequestContextBenchmark {
    @Param({"500", "1000", "2000", "5000"})
    public static int topicCount;
    @Param({"5", "10", "50"})
    public static int partitionCount;
    @Param({"10", "50", "100"})
    public static int brokerCount;
    @Param({"false", "true"})
    public static boolean isHostNamePrefixEnabled;

    private MultiTenantPrincipal principal = new MultiTenantPrincipal("user",
        new TenantMetadata("tenant", "tenant_cluster_id"));
    private Metrics metrics = new Metrics();
    private TenantMetrics tenantMetrics = new TenantMetrics();
    private MultiTenantRequestContext multiTenantRequestContext;

    @Setup(Level.Trial)
    public void setup() {
        multiTenantRequestContext = createRequestContext();
    }

    private MultiTenantRequestContext createRequestContext() {
        Map<String, Object> configMap  = new HashMap<String, Object>() {{
            put(KafkaConfig.BrokerIdProp(), 1);
            put(KafkaConfig.DefaultReplicationFactorProp(), (short) 1);
            put(KafkaConfig.NumPartitionsProp(), 1);
            put(ConfluentConfigs.MULTITENANT_LISTENER_PREFIX_ENABLE, isHostNamePrefixEnabled);
        }};
        MultiTenantInterceptorConfig config = new MultiTenantInterceptorConfig(configMap);
        RequestHeader header = new RequestHeader(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion(), "clientId", 23);
        MultiTenantRequestContext context = new MultiTenantRequestContext(
                header, "1", null, principal,
                ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT), SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY,
                new SystemTime(), metrics, tenantMetrics, config);

        // to set MultiTenantRequestContext.isMetadataFetchForAllTopics flag
        MetadataRequest metadataRequest = MetadataRequest.Builder.allTopics().build();
        ByteBuffer requestBuffer = toByteBuffer(metadataRequest);
        context.parseRequest(requestBuffer);

        return context;
    }

    private ByteBuffer toByteBuffer(AbstractRequest request) {
        Struct struct = RequestInternals.toStruct(request);
        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);
        buffer.flip();
        return buffer;
    }

    private MetadataResponse prepareResponse() {
        MetadataResponseData responseData = new MetadataResponseData();
        responseData.setThrottleTimeMs(0);

        IntStream.range(0, brokerCount).forEach(brokerId -> {
            responseData.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(brokerId)
                .setHost("host_" + brokerId)
                .setPort(9092 + brokerId)
                .setRack("rack1"));
        });

        responseData.setClusterId("clusterId");
        responseData.setControllerId(1);
        responseData.setClusterAuthorizedOperations(0);

        IntStream.range(0, topicCount).forEach(topicId -> {
            String topicName;
            if (topicId % 5 == 0) {
                topicName = "tenant_" + "topic-" + topicId;
            } else {
                topicName = "othertenant_" + "topic-" + topicId;
            }

            MetadataResponseData.MetadataResponseTopic metadataResponseTopic = new MetadataResponseData.MetadataResponseTopic();
            metadataResponseTopic
                .setErrorCode(Errors.NONE.code())
                .setName(topicName)
                .setIsInternal(false)
                .setTopicAuthorizedOperations(0);

            IntStream.range(0, partitionCount).forEach(partitionId -> {
                metadataResponseTopic.partitions().add(new MetadataResponseData.MetadataResponsePartition()
                    .setErrorCode(Errors.NONE.code())
                    .setPartitionIndex(partitionId)
                    .setLeaderId(1)
                    .setLeaderEpoch(100)
                    .setReplicaNodes(Arrays.asList(0, 1, 2))
                    .setObservers(Collections.emptyList())
                    .setIsrNodes(Arrays.asList(0, 1, 2))
                    .setOfflineReplicas(Collections.emptyList()));
            });

            responseData.topics().add(metadataResponseTopic);
        });

        return new MetadataResponse(responseData);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        metrics.close();
    }

    @Benchmark
    public void testBuildResponseMetadataForMetadata() {
        multiTenantRequestContext.buildResponse(prepareResponse());
    }
}
