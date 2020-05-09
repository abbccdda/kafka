/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResult;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec.TimestampSpec;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.apache.kafka.clients.admin.internals.ConsumerGroupOperationContext;
import org.apache.kafka.clients.admin.internals.MetadataOperationContext;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Confluent;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData.AclCreation;
import org.apache.kafka.common.message.CreateAclsResponseData.AclCreationResult;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData.CreatableRenewers;
import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicConfigs;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData.DeleteAclsFilter;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult;
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsMatchingAcl;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsPartition;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData.DescribableLogDirTopic;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterableConfig;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterableConfigCollection;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestPartition;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopic;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection;
import org.apache.kafka.common.message.RemoveBrokersRequestData.BrokerId;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.ReplicaStatusResponseData.ReplicaStatusPartitionResponse;
import org.apache.kafka.common.message.ReplicaStatusResponseData.ReplicaStatusReplicaResponse;
import org.apache.kafka.common.message.ReplicaStatusResponseData.ReplicaStatusTopicResponse;
import org.apache.kafka.common.message.RemoveBrokersResponseData.RemoveBrokerResponse;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.replica.ReplicaStatus;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AlterClientQuotasRequest;
import org.apache.kafka.common.requests.AlterClientQuotasResponse;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterConfigsResponse;
import org.apache.kafka.common.requests.AlterMirrorsRequest;
import org.apache.kafka.common.requests.AlterMirrorsResponse;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.AlterReplicaLogDirsRequest;
import org.apache.kafka.common.requests.AlterReplicaLogDirsResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ClusterLinkListing;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreateAclsResponse;
import org.apache.kafka.common.requests.CreateClusterLinksRequest;
import org.apache.kafka.common.requests.CreateClusterLinksResponse;
import org.apache.kafka.common.requests.CreateDelegationTokenRequest;
import org.apache.kafka.common.requests.CreateDelegationTokenResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DeleteClusterLinksRequest;
import org.apache.kafka.common.requests.DeleteClusterLinksResponse;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeClientQuotasRequest;
import org.apache.kafka.common.requests.DescribeClientQuotasResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeDelegationTokenRequest;
import org.apache.kafka.common.requests.DescribeDelegationTokenResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.DescribeLogDirsRequest;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.requests.ElectLeadersRequest;
import org.apache.kafka.common.requests.ElectLeadersResponse;
import org.apache.kafka.common.requests.ExpireDelegationTokenRequest;
import org.apache.kafka.common.requests.ExpireDelegationTokenResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListClusterLinksRequest;
import org.apache.kafka.common.requests.ListClusterLinksResponse;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.ListOffsetResponse.PartitionData;
import org.apache.kafka.common.requests.ListPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.ListPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.NewClusterLink;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetDeleteRequest;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RenewDelegationTokenRequest;
import org.apache.kafka.common.requests.RenewDelegationTokenResponse;
import org.apache.kafka.common.requests.ReplicaStatusRequest;
import org.apache.kafka.common.requests.ReplicaStatusResponse;
import org.apache.kafka.common.requests.RemoveBrokersRequest;
import org.apache.kafka.common.requests.RemoveBrokersResponse;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition;
import static org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse;
import static org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse;
import static org.apache.kafka.common.message.ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics;
import static org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingPartitionReassignment;
import static org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingTopicReassignment;
import static org.apache.kafka.common.requests.MetadataRequest.convertToMetadataRequestTopic;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * The default implementation of {@link Admin}. An instance of this class is created by invoking one of the
 * {@code create()} methods in {@code AdminClient}. Users should not refer to this class directly.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class KafkaAdminClient extends AdminClient implements ConfluentAdmin {

    /**
     * The next integer to use to name a KafkaAdminClient which the user hasn't specified an explicit name for.
     */
    private static final AtomicInteger ADMIN_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * The prefix to use for the JMX metrics for this class
     */
    private static final String JMX_PREFIX = "kafka.admin.client";

    /**
     * An invalid shutdown time which indicates that a shutdown has not yet been performed.
     */
    private static final long INVALID_SHUTDOWN_TIME = -1;

    /**
     * Thread name prefix for admin client network thread
     */
    static final String NETWORK_THREAD_PREFIX = "kafka-admin-client-thread";

    private final Logger log;

    /**
     * The default timeout to use for an operation.
     */
    private final int defaultApiTimeoutMs;

    /**
     * The timeout to use for a single request.
     */
    private final int requestTimeoutMs;

    /**
     * The name of this AdminClient instance.
     */
    private final String clientId;

    /**
     * Provides the time.
     */
    private final Time time;

    /**
     * The cluster metadata manager used by the KafkaClient.
     */
    private final AdminMetadataManager metadataManager;

    /**
     * The metrics for this KafkaAdminClient.
     */
    private final Metrics metrics;

    /**
     * The network client to use.
     */
    private final KafkaClient client;

    /**
     * The runnable used in the service thread for this admin client.
     */
    private final AdminClientRunnable runnable;

    /**
     * The network service thread for this admin client.
     */
    private final Thread thread;

    /**
     * During a close operation, this is the time at which we will time out all pending operations
     * and force the RPC thread to exit. If the admin client is not closing, this will be 0.
     */
    private final AtomicLong hardShutdownTimeMs = new AtomicLong(INVALID_SHUTDOWN_TIME);

    /**
     * A factory which creates TimeoutProcessors for the RPC thread.
     */
    private final TimeoutProcessorFactory timeoutProcessorFactory;

    private final int maxRetries;

    private final long retryBackoffMs;

    /**
     * Get or create a list value from a map.
     *
     * @param map   The map to get or create the element from.
     * @param key   The key.
     * @param <K>   The key type.
     * @param <V>   The value type.
     * @return      The list value.
     */
    static <K, V> List<V> getOrCreateListValue(Map<K, List<V>> map, K key) {
        return map.computeIfAbsent(key, k -> new LinkedList<>());
    }

    /**
     * Send an exception to every element in a collection of KafkaFutureImpls.
     *
     * @param futures   The collection of KafkaFutureImpl objects.
     * @param exc       The exception
     * @param <T>       The KafkaFutureImpl result type.
     */
    private static <T> void completeAllExceptionally(Collection<KafkaFutureImpl<T>> futures, Throwable exc) {
        completeAllExceptionally(futures.stream(), exc);
    }

    /**
     * Send an exception to all futures in the provided stream
     *
     * @param futures   The stream of KafkaFutureImpl objects.
     * @param exc       The exception
     * @param <T>       The KafkaFutureImpl result type.
     */
    private static <T> void completeAllExceptionally(Stream<KafkaFutureImpl<T>> futures, Throwable exc) {
        futures.forEach(future -> future.completeExceptionally(exc));
    }

    /**
     * Get the current time remaining before a deadline as an integer.
     *
     * @param now           The current time in milliseconds.
     * @param deadlineMs    The deadline time in milliseconds.
     * @return              The time delta in milliseconds.
     */
    static int calcTimeoutMsRemainingAsInt(long now, long deadlineMs) {
        long deltaMs = deadlineMs - now;
        if (deltaMs > Integer.MAX_VALUE)
            deltaMs = Integer.MAX_VALUE;
        else if (deltaMs < Integer.MIN_VALUE)
            deltaMs = Integer.MIN_VALUE;
        return (int) deltaMs;
    }

    /**
     * Generate the client id based on the configuration.
     *
     * @param config    The configuration
     *
     * @return          The client id
     */
    static String generateClientId(AdminClientConfig config) {
        String clientId = config.getString(AdminClientConfig.CLIENT_ID_CONFIG);
        if (!clientId.isEmpty())
            return clientId;
        return "adminclient-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement();
    }

    /**
     * Get the deadline for a particular call.
     *
     * @param now               The current time in milliseconds.
     * @param optionTimeoutMs   The timeout option given by the user.
     *
     * @return                  The deadline in milliseconds.
     */
    private long calcDeadlineMs(long now, Integer optionTimeoutMs) {
        if (optionTimeoutMs != null)
            return now + Math.max(0, optionTimeoutMs);
        return now + defaultApiTimeoutMs;
    }

    /**
     * Pretty-print an exception.
     *
     * @param throwable     The exception.
     *
     * @return              A compact human-readable string.
     */
    static String prettyPrintException(Throwable throwable) {
        if (throwable == null)
            return "Null exception.";
        if (throwable.getMessage() != null) {
            return throwable.getClass().getSimpleName() + ": " + throwable.getMessage();
        }
        return throwable.getClass().getSimpleName();
    }

    static KafkaAdminClient createInternal(AdminClientConfig config, TimeoutProcessorFactory timeoutProcessorFactory) {
        Metrics metrics = null;
        NetworkClient networkClient = null;
        Time time = Time.SYSTEM;
        String clientId = generateClientId(config);
        ChannelBuilder channelBuilder = null;
        Selector selector = null;
        ApiVersions apiVersions = new ApiVersions();
        LogContext logContext = createLogContext(clientId);

        try {
            // Since we only request node information, it's safe to pass true for allowAutoTopicCreation (and it
            // simplifies communication with older brokers)
            AdminMetadataManager metadataManager = new AdminMetadataManager(logContext,
                config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG));
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                    config.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG),
                    config.getString(AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG));
            metadataManager.update(Cluster.bootstrap(addresses), time.milliseconds());
            List<MetricsReporter> reporters = config.getConfiguredInstances(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class,
                Collections.singletonMap(AdminClientConfig.CLIENT_ID_CONFIG, clientId));
            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(AdminClientConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(AdminClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .recordLevel(Sensor.RecordingLevel.forName(config.getString(AdminClientConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                .tags(metricTags);
            JmxReporter jmxReporter = new JmxReporter(JMX_PREFIX);
            jmxReporter.configure(config.originals());
            reporters.add(jmxReporter);
            metrics = new Metrics(metricConfig, reporters, time);
            String metricGrpPrefix = "admin-client";
            channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext);
            selector = new Selector(config.getLong(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                    metrics, time, metricGrpPrefix, channelBuilder, logContext);
            networkClient = new NetworkClient(
                selector,
                metadataManager.updater(),
                clientId,
                1,
                config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG),
                config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                config.getInt(AdminClientConfig.SEND_BUFFER_CONFIG),
                config.getInt(AdminClientConfig.RECEIVE_BUFFER_CONFIG),
                (int) TimeUnit.HOURS.toMillis(1),
                ClientDnsLookup.forConfig(config.getString(AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG)),
                time,
                true,
                apiVersions,
                logContext);
            return new KafkaAdminClient(config, clientId, time, metadataManager, metrics, networkClient,
                timeoutProcessorFactory, logContext);
        } catch (Throwable exc) {
            closeQuietly(metrics, "Metrics");
            closeQuietly(networkClient, "NetworkClient");
            closeQuietly(selector, "Selector");
            closeQuietly(channelBuilder, "ChannelBuilder");
            throw new KafkaException("Failed to create new KafkaAdminClient", exc);
        }
    }

    static KafkaAdminClient createInternal(AdminClientConfig config,
                                           AdminMetadataManager metadataManager,
                                           KafkaClient client,
                                           Time time) {
        Metrics metrics = null;
        String clientId = generateClientId(config);

        try {
            metrics = new Metrics(new MetricConfig(), new LinkedList<>(), time);
            LogContext logContext = createLogContext(clientId);
            return new KafkaAdminClient(config, clientId, time, metadataManager, metrics,
                client, null, logContext);
        } catch (Throwable exc) {
            closeQuietly(metrics, "Metrics");
            throw new KafkaException("Failed to create new KafkaAdminClient", exc);
        }
    }

    static LogContext createLogContext(String clientId) {
        return new LogContext("[AdminClient clientId=" + clientId + "] ");
    }

    private KafkaAdminClient(AdminClientConfig config,
                             String clientId,
                             Time time,
                             AdminMetadataManager metadataManager,
                             Metrics metrics,
                             KafkaClient client,
                             TimeoutProcessorFactory timeoutProcessorFactory,
                             LogContext logContext) {
        this.clientId = clientId;
        this.log = logContext.logger(KafkaAdminClient.class);
        this.requestTimeoutMs = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.defaultApiTimeoutMs = configureDefaultApiTimeoutMs(config);
        this.time = time;
        this.metadataManager = metadataManager;
        this.metrics = metrics;
        this.client = client;
        this.runnable = new AdminClientRunnable();
        String threadName = NETWORK_THREAD_PREFIX + " | " + clientId;
        this.thread = new KafkaThread(threadName, runnable, true);
        this.timeoutProcessorFactory = (timeoutProcessorFactory == null) ?
            new TimeoutProcessorFactory() : timeoutProcessorFactory;
        this.maxRetries = config.getInt(AdminClientConfig.RETRIES_CONFIG);
        this.retryBackoffMs = config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG);
        config.logUnused();
        AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
        log.debug("Kafka admin client initialized");
        thread.start();
    }

    /**
     * If a default.api.timeout.ms has been explicitly specified, raise an error if it conflicts with request.timeout.ms.
     * If no default.api.timeout.ms has been configured, then set its value as the max of the default and request.timeout.ms. Also we should probably log a warning.
     * Otherwise, use the provided values for both configurations.
     *
     * @param config The configuration
     */
    private int configureDefaultApiTimeoutMs(AdminClientConfig config) {
        int requestTimeoutMs = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
        int defaultApiTimeoutMs = config.getInt(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);

        if (defaultApiTimeoutMs < requestTimeoutMs) {
            if (config.originals().containsKey(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)) {
                throw new ConfigException("The specified value of " + AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG +
                        " must be no smaller than the value of " + AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG + ".");
            } else {
                log.warn("Overriding the default value for {} ({}) with the explicitly configured request timeout {}",
                        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, this.defaultApiTimeoutMs,
                        requestTimeoutMs);
                return requestTimeoutMs;
            }
        }
        return defaultApiTimeoutMs;
    }

    @Override
    public void close(Duration timeout) {
        long waitTimeMs = timeout.toMillis();
        if (waitTimeMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        waitTimeMs = Math.min(TimeUnit.DAYS.toMillis(365), waitTimeMs); // Limit the timeout to a year.
        long now = time.milliseconds();
        long newHardShutdownTimeMs = now + waitTimeMs;
        long prev = INVALID_SHUTDOWN_TIME;
        while (true) {
            if (hardShutdownTimeMs.compareAndSet(prev, newHardShutdownTimeMs)) {
                if (prev == INVALID_SHUTDOWN_TIME) {
                    log.debug("Initiating close operation.");
                } else {
                    log.debug("Moving hard shutdown time forward.");
                }
                client.wakeup(); // Wake the thread, if it is blocked inside poll().
                break;
            }
            prev = hardShutdownTimeMs.get();
            if (prev < newHardShutdownTimeMs) {
                log.debug("Hard shutdown time is already earlier than requested.");
                newHardShutdownTimeMs = prev;
                break;
            }
        }
        if (log.isDebugEnabled()) {
            long deltaMs = Math.max(0, newHardShutdownTimeMs - time.milliseconds());
            log.debug("Waiting for the I/O thread to exit. Hard shutdown in {} ms.", deltaMs);
        }
        try {
            // close() can be called by AdminClient thread when it invokes callback. That will
            // cause deadlock, so check for that condition.
            if (Thread.currentThread() != thread) {
                // Wait for the thread to be joined.
                thread.join(waitTimeMs);
            }
            log.debug("Kafka admin client closed.");
        } catch (InterruptedException e) {
            log.debug("Interrupted while joining I/O thread", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * An interface for providing a node for a call.
     */
    private interface NodeProvider {
        Node provide();
    }

    private class MetadataUpdateNodeIdProvider implements NodeProvider {
        @Override
        public Node provide() {
            return client.leastLoadedNode(time.milliseconds());
        }
    }

    private class ConstantNodeIdProvider implements NodeProvider {
        private final int nodeId;

        ConstantNodeIdProvider(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public Node provide() {
            if (metadataManager.isReady() &&
                    (metadataManager.nodeById(nodeId) != null)) {
                return metadataManager.nodeById(nodeId);
            }
            // If we can't find the node with the given constant ID, we schedule a
            // metadata update and hope it appears.  This behavior is useful for avoiding
            // flaky behavior in tests when the cluster is starting up and not all nodes
            // have appeared.
            metadataManager.requestUpdate();
            return null;
        }
    }

    /**
     * Provides the controller node.
     */
    private class ControllerNodeProvider implements NodeProvider {
        @Override
        public Node provide() {
            if (metadataManager.isReady() &&
                    (metadataManager.controller() != null)) {
                return metadataManager.controller();
            }
            metadataManager.requestUpdate();
            return null;
        }
    }

    /**
     * Provides the least loaded node.
     */
    private class LeastLoadedNodeProvider implements NodeProvider {
        @Override
        public Node provide() {
            if (metadataManager.isReady()) {
                // This may return null if all nodes are busy.
                // In that case, we will postpone node assignment.
                return client.leastLoadedNode(time.milliseconds());
            }
            metadataManager.requestUpdate();
            return null;
        }
    }

    abstract class Call {
        private final boolean internal;
        private final String callName;
        private final long deadlineMs;
        private final NodeProvider nodeProvider;
        private int tries = 0;
        private boolean aborted = false;
        private Node curNode = null;
        private long nextAllowedTryMs = 0;

        Call(boolean internal, String callName, long deadlineMs, NodeProvider nodeProvider) {
            this.internal = internal;
            this.callName = callName;
            this.deadlineMs = deadlineMs;
            this.nodeProvider = nodeProvider;
        }

        Call(String callName, long deadlineMs, NodeProvider nodeProvider) {
            this(false, callName, deadlineMs, nodeProvider);
        }

        protected Node curNode() {
            return curNode;
        }

        /**
         * Handle a failure.
         *
         * Depending on what the exception is and how many times we have already tried, we may choose to
         * fail the Call, or retry it. It is important to print the stack traces here in some cases,
         * since they are not necessarily preserved in ApiVersionException objects.
         *
         * @param now           The current time in milliseconds.
         * @param throwable     The failure exception.
         */
        final void fail(long now, Throwable throwable) {
            if (aborted) {
                // If the call was aborted while in flight due to a timeout, deliver a
                // TimeoutException. In this case, we do not get any more retries - the call has
                // failed. We increment tries anyway in order to display an accurate log message.
                tries++;
                failWithTimeout(now, throwable);
                return;
            }
            // If this is an UnsupportedVersionException that we can retry, do so. Note that a
            // protocol downgrade will not count against the total number of retries we get for
            // this RPC. That is why 'tries' is not incremented.
            if ((throwable instanceof UnsupportedVersionException) &&
                     handleUnsupportedVersionException((UnsupportedVersionException) throwable)) {
                log.debug("{} attempting protocol downgrade and then retry.", this);
                runnable.enqueue(this, now);
                return;
            }
            tries++;
            nextAllowedTryMs = now + retryBackoffMs;

            // If the call has timed out, fail.
            if (calcTimeoutMsRemainingAsInt(now, deadlineMs) < 0) {
                failWithTimeout(now, throwable);
                return;
            }
            // If the exception is not retriable, fail.
            if (!(throwable instanceof RetriableException)) {
                if (log.isDebugEnabled()) {
                    log.debug("{} failed with non-retriable exception after {} attempt(s)", this, tries,
                        new Exception(prettyPrintException(throwable)));
                }
                handleFailure(throwable);
                return;
            }
            // If we are out of retries, fail.
            if (tries > maxRetries) {
                failWithTimeout(now, throwable);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("{} failed: {}. Beginning retry #{}",
                    this, prettyPrintException(throwable), tries);
            }
            runnable.enqueue(this, now);
        }

        private void failWithTimeout(long now, Throwable cause) {
            if (log.isDebugEnabled()) {
                log.debug("{} timed out at {} after {} attempt(s)", this, now, tries,
                    new Exception(prettyPrintException(cause)));
            }
            handleFailure(new TimeoutException(this + " timed out at " + now
                + " after " + tries + " attempt(s)", cause));
        }

        /**
         * Create an AbstractRequest.Builder for this Call.
         *
         * @param timeoutMs The timeout in milliseconds.
         *
         * @return          The AbstractRequest builder.
         */
        @SuppressWarnings("rawtypes")
        abstract AbstractRequest.Builder createRequest(int timeoutMs);

        /**
         * Process the call response.
         *
         * @param abstractResponse  The AbstractResponse.
         *
         */
        abstract void handleResponse(AbstractResponse abstractResponse);

        /**
         * Handle a failure. This will only be called if the failure exception was not
         * retriable, or if we hit a timeout.
         *
         * @param throwable     The exception.
         */
        abstract void handleFailure(Throwable throwable);

        /**
         * Handle an UnsupportedVersionException.
         *
         * @param exception     The exception.
         *
         * @return              True if the exception can be handled; false otherwise.
         */
        boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
            return false;
        }

        @Override
        public String toString() {
            return "Call(callName=" + callName + ", deadlineMs=" + deadlineMs +
                ", tries=" + tries + ", nextAllowedTryMs=" + nextAllowedTryMs + ")";
        }

        public boolean isInternal() {
            return internal;
        }
    }

    static class TimeoutProcessorFactory {
        TimeoutProcessor create(long now) {
            return new TimeoutProcessor(now);
        }
    }

    static class TimeoutProcessor {
        /**
         * The current time in milliseconds.
         */
        private final long now;

        /**
         * The number of milliseconds until the next timeout.
         */
        private int nextTimeoutMs;

        /**
         * Create a new timeout processor.
         *
         * @param now           The current time in milliseconds since the epoch.
         */
        TimeoutProcessor(long now) {
            this.now = now;
            this.nextTimeoutMs = Integer.MAX_VALUE;
        }

        /**
         * Check for calls which have timed out.
         * Timed out calls will be removed and failed.
         * The remaining milliseconds until the next timeout will be updated.
         *
         * @param calls         The collection of calls.
         *
         * @return              The number of calls which were timed out.
         */
        int handleTimeouts(Collection<Call> calls, String msg) {
            int numTimedOut = 0;
            for (Iterator<Call> iter = calls.iterator(); iter.hasNext(); ) {
                Call call = iter.next();
                int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
                if (remainingMs < 0) {
                    call.fail(now, new TimeoutException(msg));
                    iter.remove();
                    numTimedOut++;
                } else {
                    nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
                }
            }
            return numTimedOut;
        }

        /**
         * Check whether a call should be timed out.
         * The remaining milliseconds until the next timeout will be updated.
         *
         * @param call      The call.
         *
         * @return          True if the call should be timed out.
         */
        boolean callHasExpired(Call call) {
            int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
            if (remainingMs < 0)
                return true;
            nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
            return false;
        }

        int nextTimeoutMs() {
            return nextTimeoutMs;
        }
    }

    private final class AdminClientRunnable implements Runnable {
        /**
         * Calls which have not yet been assigned to a node.
         * Only accessed from this thread.
         */
        private final ArrayList<Call> pendingCalls = new ArrayList<>();

        /**
         * Maps nodes to calls that we want to send.
         * Only accessed from this thread.
         */
        private final Map<Node, List<Call>> callsToSend = new HashMap<>();

        /**
         * Maps node ID strings to calls that have been sent.
         * Only accessed from this thread.
         */
        private final Map<String, List<Call>> callsInFlight = new HashMap<>();

        /**
         * Maps correlation IDs to calls that have been sent.
         * Only accessed from this thread.
         */
        private final Map<Integer, Call> correlationIdToCalls = new HashMap<>();

        /**
         * Pending calls. Protected by the object monitor.
         * This will be null only if the thread has shut down.
         */
        private List<Call> newCalls = new LinkedList<>();

        /**
         * Time out the elements in the pendingCalls list which are expired.
         *
         * @param processor     The timeout processor.
         */
        private void timeoutPendingCalls(TimeoutProcessor processor) {
            int numTimedOut = processor.handleTimeouts(pendingCalls, "Timed out waiting for a node assignment.");
            if (numTimedOut > 0)
                log.debug("Timed out {} pending calls.", numTimedOut);
        }

        /**
         * Time out calls which have been assigned to nodes.
         *
         * @param processor     The timeout processor.
         */
        private int timeoutCallsToSend(TimeoutProcessor processor) {
            int numTimedOut = 0;
            for (List<Call> callList : callsToSend.values()) {
                numTimedOut += processor.handleTimeouts(callList,
                    "Timed out waiting to send the call.");
            }
            if (numTimedOut > 0)
                log.debug("Timed out {} call(s) with assigned nodes.", numTimedOut);
            return numTimedOut;
        }

        /**
         * Drain all the calls from newCalls into pendingCalls.
         *
         * This function holds the lock for the minimum amount of time, to avoid blocking
         * users of AdminClient who will also take the lock to add new calls.
         */
        private synchronized void drainNewCalls() {
            if (!newCalls.isEmpty()) {
                pendingCalls.addAll(newCalls);
                newCalls.clear();
            }
        }

        /**
         * Choose nodes for the calls in the pendingCalls list.
         *
         * @param now           The current time in milliseconds.
         * @return              The minimum time until a call is ready to be retried if any of the pending
         *                      calls are backing off after a failure
         */
        private long maybeDrainPendingCalls(long now) {
            long pollTimeout = Long.MAX_VALUE;
            log.trace("Trying to choose nodes for {} at {}", pendingCalls, now);

            Iterator<Call> pendingIter = pendingCalls.iterator();
            while (pendingIter.hasNext()) {
                Call call = pendingIter.next();

                // If the call is being retried, await the proper backoff before finding the node
                if (now < call.nextAllowedTryMs) {
                    pollTimeout = Math.min(pollTimeout, call.nextAllowedTryMs - now);
                } else if (maybeDrainPendingCall(call, now)) {
                    pendingIter.remove();
                }
            }
            return pollTimeout;
        }

        /**
         * Check whether a pending call can be assigned a node. Return true if the pending call was either
         * transferred to the callsToSend collection or if the call was failed. Return false if it
         * should remain pending.
         */
        private boolean maybeDrainPendingCall(Call call, long now) {
            try {
                Node node = call.nodeProvider.provide();
                if (node != null) {
                    log.trace("Assigned {} to node {}", call, node);
                    call.curNode = node;
                    getOrCreateListValue(callsToSend, node).add(call);
                    return true;
                } else {
                    log.trace("Unable to assign {} to a node.", call);
                    return false;
                }
            } catch (Throwable t) {
                // Handle authentication errors while choosing nodes.
                log.debug("Unable to choose node for {}", call, t);
                call.fail(now, t);
                return true;
            }
        }

        /**
         * Send the calls which are ready.
         *
         * @param now                   The current time in milliseconds.
         * @return                      The minimum timeout we need for poll().
         */
        private long sendEligibleCalls(long now) {
            long pollTimeout = Long.MAX_VALUE;
            for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry<Node, List<Call>> entry = iter.next();
                List<Call> calls = entry.getValue();
                if (calls.isEmpty()) {
                    iter.remove();
                    continue;
                }
                Node node = entry.getKey();
                if (!client.ready(node, now)) {
                    long nodeTimeout = client.pollDelayMs(node, now);
                    pollTimeout = Math.min(pollTimeout, nodeTimeout);
                    log.trace("Client is not ready to send to {}. Must delay {} ms", node, nodeTimeout);
                    continue;
                }
                Call call = calls.remove(0);
                int requestTimeoutMs = Math.min(KafkaAdminClient.this.requestTimeoutMs,
                        calcTimeoutMsRemainingAsInt(now, call.deadlineMs));
                AbstractRequest.Builder<?> requestBuilder;
                try {
                    requestBuilder = call.createRequest(requestTimeoutMs);
                } catch (Throwable throwable) {
                    call.fail(now, new KafkaException(String.format(
                        "Internal error sending %s to %s.", call.callName, node)));
                    continue;
                }
                ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now,
                        true, requestTimeoutMs, null);
                log.debug("Sending {} to {}. correlationId={}", requestBuilder, node, clientRequest.correlationId());
                client.send(clientRequest, now);
                getOrCreateListValue(callsInFlight, node.idString()).add(call);
                correlationIdToCalls.put(clientRequest.correlationId(), call);
            }
            return pollTimeout;
        }

        /**
         * Time out expired calls that are in flight.
         *
         * Calls that are in flight may have been partially or completely sent over the wire. They may
         * even be in the process of being processed by the remote server. At the moment, our only option
         * to time them out is to close the entire connection.
         *
         * @param processor         The timeout processor.
         */
        private void timeoutCallsInFlight(TimeoutProcessor processor) {
            int numTimedOut = 0;
            for (Map.Entry<String, List<Call>> entry : callsInFlight.entrySet()) {
                List<Call> contexts = entry.getValue();
                if (contexts.isEmpty())
                    continue;
                String nodeId = entry.getKey();
                // We assume that the first element in the list is the earliest. So it should be the
                // only one we need to check the timeout for.
                Call call = contexts.get(0);
                if (processor.callHasExpired(call)) {
                    if (call.aborted) {
                        log.warn("Aborted call {} is still in callsInFlight.", call);
                    } else {
                        log.debug("Closing connection to {} to time out {}", nodeId, call);
                        call.aborted = true;
                        client.disconnect(nodeId);
                        numTimedOut++;
                        // We don't remove anything from the callsInFlight data structure. Because the connection
                        // has been closed, the calls should be returned by the next client#poll(),
                        // and handled at that point.
                    }
                }
            }
            if (numTimedOut > 0)
                log.debug("Timed out {} call(s) in flight.", numTimedOut);
        }

        /**
         * Handle responses from the server.
         *
         * @param now                   The current time in milliseconds.
         * @param responses             The latest responses from KafkaClient.
         **/
        private void handleResponses(long now, List<ClientResponse> responses) {
            for (ClientResponse response : responses) {
                int correlationId = response.requestHeader().correlationId();

                Call call = correlationIdToCalls.get(correlationId);
                if (call == null) {
                    // If the server returns information about a correlation ID we didn't use yet,
                    // an internal server error has occurred. Close the connection and log an error message.
                    log.error("Internal server error on {}: server returned information about unknown " +
                        "correlation ID {}, requestHeader = {}", response.destination(), correlationId,
                        response.requestHeader());
                    client.disconnect(response.destination());
                    continue;
                }

                // Stop tracking this call.
                correlationIdToCalls.remove(correlationId);
                List<Call> calls = callsInFlight.get(response.destination());
                if ((calls == null) || (!calls.remove(call))) {
                    log.error("Internal server error on {}: ignoring call {} in correlationIdToCall " +
                        "that did not exist in callsInFlight", response.destination(), call);
                    continue;
                }

                // Handle the result of the call. This may involve retrying the call, if we got a
                // retriable exception.
                if (response.versionMismatch() != null) {
                    call.fail(now, response.versionMismatch());
                } else if (response.wasDisconnected()) {
                    AuthenticationException authException = client.authenticationException(call.curNode());
                    if (authException != null) {
                        call.fail(now, authException);
                    } else {
                        call.fail(now, new DisconnectException(String.format(
                            "Cancelled %s request with correlation id %s due to node %s being disconnected",
                            call.callName, correlationId, response.destination())));
                    }
                } else {
                    try {
                        call.handleResponse(response.responseBody());
                        if (log.isTraceEnabled())
                            log.trace("{} got response {}", call,
                                    response.responseBody().toString(response.requestHeader().apiVersion()));
                    } catch (Throwable t) {
                        if (log.isTraceEnabled())
                            log.trace("{} handleResponse failed with {}", call, prettyPrintException(t));
                        call.fail(now, t);
                    }
                }
            }
        }

        /**
         * Unassign calls that have not yet been sent based on some predicate. For example, this
         * is used to reassign the calls that have been assigned to a disconnected node.
         *
         * @param shouldUnassign Condition for reassignment. If the predicate is true, then the calls will
         *                       be put back in the pendingCalls collection and they will be reassigned
         */
        private void unassignUnsentCalls(Predicate<Node> shouldUnassign) {
            for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry<Node, List<Call>> entry = iter.next();
                Node node = entry.getKey();
                List<Call> awaitingCalls = entry.getValue();

                if (awaitingCalls.isEmpty()) {
                    iter.remove();
                } else if (shouldUnassign.test(node)) {
                    pendingCalls.addAll(awaitingCalls);
                    iter.remove();
                }
            }
        }

        private boolean hasActiveExternalCalls(Collection<Call> calls) {
            for (Call call : calls) {
                if (!call.isInternal()) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Return true if there are currently active external calls.
         */
        private boolean hasActiveExternalCalls() {
            if (hasActiveExternalCalls(pendingCalls)) {
                return true;
            }
            for (List<Call> callList : callsToSend.values()) {
                if (hasActiveExternalCalls(callList)) {
                    return true;
                }
            }
            return hasActiveExternalCalls(correlationIdToCalls.values());
        }

        private boolean threadShouldExit(long now, long curHardShutdownTimeMs) {
            if (!hasActiveExternalCalls()) {
                log.trace("All work has been completed, and the I/O thread is now exiting.");
                return true;
            }
            if (now >= curHardShutdownTimeMs) {
                log.info("Forcing a hard I/O thread shutdown. Requests in progress will be aborted.");
                return true;
            }
            log.debug("Hard shutdown in {} ms.", curHardShutdownTimeMs - now);
            return false;
        }

        @Override
        public void run() {
            log.trace("Thread starting");
            try {
                processRequests();
            } finally {
                AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);

                int numTimedOut = 0;
                TimeoutProcessor timeoutProcessor = new TimeoutProcessor(Long.MAX_VALUE);
                synchronized (this) {
                    numTimedOut += timeoutProcessor.handleTimeouts(newCalls, "The AdminClient thread has exited.");
                    newCalls = null;
                }
                numTimedOut += timeoutProcessor.handleTimeouts(pendingCalls, "The AdminClient thread has exited.");
                numTimedOut += timeoutCallsToSend(timeoutProcessor);
                numTimedOut += timeoutProcessor.handleTimeouts(correlationIdToCalls.values(),
                        "The AdminClient thread has exited.");
                if (numTimedOut > 0) {
                    log.debug("Timed out {} remaining operation(s).", numTimedOut);
                }
                closeQuietly(client, "KafkaClient");
                closeQuietly(metrics, "Metrics");
                log.debug("Exiting AdminClientRunnable thread.");
            }
        }

        private void processRequests() {
            long now = time.milliseconds();
            while (true) {
                // Copy newCalls into pendingCalls.
                drainNewCalls();

                // Check if the AdminClient thread should shut down.
                long curHardShutdownTimeMs = hardShutdownTimeMs.get();
                if ((curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) && threadShouldExit(now, curHardShutdownTimeMs))
                    break;

                // Handle timeouts.
                TimeoutProcessor timeoutProcessor = timeoutProcessorFactory.create(now);
                timeoutPendingCalls(timeoutProcessor);
                timeoutCallsToSend(timeoutProcessor);
                timeoutCallsInFlight(timeoutProcessor);

                long pollTimeout = Math.min(1200000, timeoutProcessor.nextTimeoutMs());
                if (curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) {
                    pollTimeout = Math.min(pollTimeout, curHardShutdownTimeMs - now);
                }

                // Choose nodes for our pending calls.
                pollTimeout = Math.min(pollTimeout, maybeDrainPendingCalls(now));
                long metadataFetchDelayMs = metadataManager.metadataFetchDelayMs(now);
                if (metadataFetchDelayMs == 0) {
                    metadataManager.transitionToUpdatePending(now);
                    Call metadataCall = makeMetadataCall(now);
                    // Create a new metadata fetch call and add it to the end of pendingCalls.
                    // Assign a node for just the new call (we handled the other pending nodes above).

                    if (!maybeDrainPendingCall(metadataCall, now))
                        pendingCalls.add(metadataCall);
                }
                pollTimeout = Math.min(pollTimeout, sendEligibleCalls(now));

                if (metadataFetchDelayMs > 0) {
                    pollTimeout = Math.min(pollTimeout, metadataFetchDelayMs);
                }

                // Ensure that we use a small poll timeout if there are pending calls which need to be sent
                if (!pendingCalls.isEmpty())
                    pollTimeout = Math.min(pollTimeout, retryBackoffMs);

                // Wait for network responses.
                log.trace("Entering KafkaClient#poll(timeout={})", pollTimeout);
                List<ClientResponse> responses = client.poll(pollTimeout, now);
                log.trace("KafkaClient#poll retrieved {} response(s)", responses.size());

                // unassign calls to disconnected nodes
                unassignUnsentCalls(client::connectionFailed);

                // Update the current time and handle the latest responses.
                now = time.milliseconds();
                handleResponses(now, responses);
            }
        }

        /**
         * Queue a call for sending.
         *
         * If the AdminClient thread has exited, this will fail. Otherwise, it will succeed (even
         * if the AdminClient is shutting down). This function should called when retrying an
         * existing call.
         *
         * @param call      The new call object.
         * @param now       The current time in milliseconds.
         */
        void enqueue(Call call, long now) {
            if (call.tries > maxRetries) {
                log.debug("Max retries {} for {} reached", maxRetries, call);
                call.fail(time.milliseconds(), new TimeoutException());
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("Queueing {} with a timeout {} ms from now.", call, call.deadlineMs - now);
            }
            boolean accepted = false;
            synchronized (this) {
                if (newCalls != null) {
                    newCalls.add(call);
                    accepted = true;
                }
            }
            if (accepted) {
                client.wakeup(); // wake the thread if it is in poll()
            } else {
                log.debug("The AdminClient thread has exited. Timing out {}.", call);
                call.fail(Long.MAX_VALUE, new TimeoutException("The AdminClient thread has exited."));
            }
        }

        /**
         * Initiate a new call.
         *
         * This will fail if the AdminClient is scheduled to shut down.
         *
         * @param call      The new call object.
         * @param now       The current time in milliseconds.
         */
        void call(Call call, long now) {
            if (hardShutdownTimeMs.get() != INVALID_SHUTDOWN_TIME) {
                log.debug("The AdminClient is not accepting new calls. Timing out {}.", call);
                call.fail(Long.MAX_VALUE, new TimeoutException("The AdminClient thread is not accepting new calls."));
            } else {
                enqueue(call, now);
            }
        }

        /**
         * Create a new metadata call.
         */
        private Call makeMetadataCall(long now) {
            return new Call(true, "fetchMetadata", calcDeadlineMs(now, requestTimeoutMs),
                    new MetadataUpdateNodeIdProvider()) {
                @Override
                public MetadataRequest.Builder createRequest(int timeoutMs) {
                    // Since this only requests node information, it's safe to pass true
                    // for allowAutoTopicCreation (and it simplifies communication with
                    // older brokers)
                    return new MetadataRequest.Builder(new MetadataRequestData()
                        .setTopics(Collections.emptyList())
                        .setAllowAutoTopicCreation(true));
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    MetadataResponse response = (MetadataResponse) abstractResponse;
                    long now = time.milliseconds();
                    metadataManager.update(response.cluster(), now);

                    // Unassign all unsent requests after a metadata refresh to allow for a new
                    // destination to be selected from the new metadata
                    unassignUnsentCalls(node -> true);
                }

                @Override
                public void handleFailure(Throwable e) {
                    metadataManager.updateFailed(e);
                }
            };
        }
    }

    /**
     * Returns true if a topic name cannot be represented in an RPC.  This function does NOT check
     * whether the name is too long, contains invalid characters, etc.  It is better to enforce
     * those policies on the server, so that they can be changed in the future if needed.
     */
    private static boolean topicNameIsUnrepresentable(String topicName) {
        return topicName == null || topicName.isEmpty();
    }

    private static boolean groupIdIsUnrepresentable(String groupId) {
        return groupId == null;
    }

    //for testing
    int numPendingCalls() {
        return runnable.pendingCalls.size();
    }

    @Override
    public CreateTopicsResult createTopics(final Collection<NewTopic> newTopics,
                                           final CreateTopicsOptions options) {
        final Map<String, KafkaFutureImpl<TopicMetadataAndConfig>> topicFutures = new HashMap<>(newTopics.size());
        final CreatableTopicCollection topics = new CreatableTopicCollection();
        for (NewTopic newTopic : newTopics) {
            if (topicNameIsUnrepresentable(newTopic.name())) {
                KafkaFutureImpl<TopicMetadataAndConfig> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                    newTopic.name() + "' cannot be represented in a request."));
                topicFutures.put(newTopic.name(), future);
            } else if (!topicFutures.containsKey(newTopic.name())) {
                topicFutures.put(newTopic.name(), new KafkaFutureImpl<>());
                topics.add(newTopic.convertToCreatableTopic());
            }
        }
        final long now = time.milliseconds();
        Call call = new Call("createTopics", calcDeadlineMs(now, options.timeoutMs()),
            new ControllerNodeProvider()) {

            @Override
            public CreateTopicsRequest.Builder createRequest(int timeoutMs) {
                return new CreateTopicsRequest.Builder(
                    new CreateTopicsRequestData().
                        setTopics(topics).
                        setTimeoutMs(timeoutMs).
                        setValidateOnly(options.shouldValidateOnly()));
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                CreateTopicsResponse response = (CreateTopicsResponse) abstractResponse;
                // Check for controller change
                for (Errors error : response.errorCounts().keySet()) {
                    if (error == Errors.NOT_CONTROLLER) {
                        metadataManager.clearController();
                        metadataManager.requestUpdate();
                        throw error.exception();
                    }
                }
                // Handle server responses for particular topics.
                for (CreatableTopicResult result : response.data().topics()) {
                    KafkaFutureImpl<TopicMetadataAndConfig> future = topicFutures.get(result.name());
                    if (future == null) {
                        log.warn("Server response mentioned unknown topic {}", result.name());
                    } else {
                        ApiError error = new ApiError(
                            Errors.forCode(result.errorCode()), result.errorMessage());
                        ApiException exception = error.exception();
                        if (exception != null) {
                            future.completeExceptionally(exception);
                        } else {
                            TopicMetadataAndConfig topicMetadataAndConfig;
                            if (result.topicConfigErrorCode() != Errors.NONE.code()) {
                                topicMetadataAndConfig = new TopicMetadataAndConfig(Errors.forCode(result.topicConfigErrorCode()).exception());
                            } else if (result.numPartitions() == CreateTopicsResult.UNKNOWN) {
                                topicMetadataAndConfig = new TopicMetadataAndConfig(new UnsupportedVersionException(
                                        "Topic metadata and configs in CreateTopics response not supported"));
                            } else {
                                List<CreatableTopicConfigs> configs = result.configs();
                                Config topicConfig = new Config(configs.stream()
                                        .map(config -> new ConfigEntry(config.configName(),
                                                config.value(),
                                                configSource(DescribeConfigsResponse.ConfigSource.forId(config.configSource())),
                                                config.isSensitive(),
                                                config.readOnly(),
                                                Collections.emptyList()))
                                        .collect(Collectors.toSet()));
                                topicMetadataAndConfig = new TopicMetadataAndConfig(result.numPartitions(),
                                        result.replicationFactor(),
                                        topicConfig);
                            }
                            future.complete(topicMetadataAndConfig);
                        }
                    }
                }
                // The server should send back a response for every topic. But do a sanity check anyway.
                for (Map.Entry<String, KafkaFutureImpl<TopicMetadataAndConfig>> entry : topicFutures.entrySet()) {
                    KafkaFutureImpl<TopicMetadataAndConfig> future = entry.getValue();
                    if (!future.isDone()) {
                        future.completeExceptionally(new ApiException("The server response did not " +
                            "contain a reference to node " + entry.getKey()));
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(topicFutures.values(), throwable);
            }
        };
        if (!topics.isEmpty()) {
            runnable.call(call, now);
        }
        return new CreateTopicsResult(new HashMap<>(topicFutures));
    }

    @Override
    public DeleteTopicsResult deleteTopics(Collection<String> topicNames,
                                           DeleteTopicsOptions options) {
        final Map<String, KafkaFutureImpl<Void>> topicFutures = new HashMap<>(topicNames.size());
        final List<String> validTopicNames = new ArrayList<>(topicNames.size());
        for (String topicName : topicNames) {
            if (topicNameIsUnrepresentable(topicName)) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                    topicName + "' cannot be represented in a request."));
                topicFutures.put(topicName, future);
            } else if (!topicFutures.containsKey(topicName)) {
                topicFutures.put(topicName, new KafkaFutureImpl<>());
                validTopicNames.add(topicName);
            }
        }
        final long now = time.milliseconds();
        Call call = new Call("deleteTopics", calcDeadlineMs(now, options.timeoutMs()),
            new ControllerNodeProvider()) {

            @Override
            DeleteTopicsRequest.Builder createRequest(int timeoutMs) {
                return new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
                        .setTopicNames(validTopicNames)
                        .setTimeoutMs(timeoutMs));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                DeleteTopicsResponse response = (DeleteTopicsResponse) abstractResponse;
                // Check for controller change
                for (Errors error : response.errorCounts().keySet()) {
                    if (error == Errors.NOT_CONTROLLER) {
                        metadataManager.clearController();
                        metadataManager.requestUpdate();
                        throw error.exception();
                    }
                }
                // Handle server responses for particular topics.
                for (DeletableTopicResult result : response.data().responses()) {
                    KafkaFutureImpl<Void> future = topicFutures.get(result.name());
                    if (future == null) {
                        log.warn("Server response mentioned unknown topic {}", result.name());
                    } else {
                        ApiException exception = Errors.forCode(result.errorCode()).exception();
                        if (exception != null) {
                            future.completeExceptionally(exception);
                        } else {
                            future.complete(null);
                        }
                    }
                }
                // The server should send back a response for every topic. But do a sanity check anyway.
                for (Map.Entry<String, KafkaFutureImpl<Void>> entry : topicFutures.entrySet()) {
                    KafkaFutureImpl<Void> future = entry.getValue();
                    if (!future.isDone()) {
                        future.completeExceptionally(new ApiException("The server response did not " +
                            "contain a reference to node " + entry.getKey()));
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(topicFutures.values(), throwable);
            }
        };
        if (!validTopicNames.isEmpty()) {
            runnable.call(call, now);
        }
        return new DeleteTopicsResult(new HashMap<>(topicFutures));
    }

    @Override
    public ListTopicsResult listTopics(final ListTopicsOptions options) {
        final KafkaFutureImpl<Map<String, TopicListing>> topicListingFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("listTopics", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                return MetadataRequest.Builder.allTopics();
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                Map<String, TopicListing> topicListing = new HashMap<>();
                for (MetadataResponse.TopicMetadata topicMetadata : response.topicMetadata()) {
                    String topicName = topicMetadata.topic();
                    boolean isInternal = topicMetadata.isInternal();
                    if (!topicMetadata.isInternal() || options.shouldListInternal())
                        topicListing.put(topicName, new TopicListing(topicName, isInternal));
                }
                topicListingFuture.complete(topicListing);
            }

            @Override
            void handleFailure(Throwable throwable) {
                topicListingFuture.completeExceptionally(throwable);
            }
        }, now);
        return new ListTopicsResult(topicListingFuture);
    }

    @Override
    public DescribeTopicsResult describeTopics(final Collection<String> topicNames, DescribeTopicsOptions options) {
        final Map<String, KafkaFutureImpl<TopicDescription>> topicFutures = new HashMap<>(topicNames.size());
        final ArrayList<String> topicNamesList = new ArrayList<>();
        for (String topicName : topicNames) {
            if (topicNameIsUnrepresentable(topicName)) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                    topicName + "' cannot be represented in a request."));
                topicFutures.put(topicName, future);
            } else if (!topicFutures.containsKey(topicName)) {
                topicFutures.put(topicName, new KafkaFutureImpl<>());
                topicNamesList.add(topicName);
            }
        }
        final long now = time.milliseconds();
        Call call = new Call("describeTopics", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            private boolean supportsDisablingTopicCreation = true;

            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                if (supportsDisablingTopicCreation)
                    return new MetadataRequest.Builder(new MetadataRequestData()
                        .setTopics(convertToMetadataRequestTopic(topicNamesList))
                        .setAllowAutoTopicCreation(false)
                        .setIncludeTopicAuthorizedOperations(options.includeAuthorizedOperations()));
                else
                    return MetadataRequest.Builder.allTopics();
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                // Handle server responses for particular topics.
                Cluster cluster = response.cluster();
                Map<String, Errors> errors = response.errors();
                for (Map.Entry<String, KafkaFutureImpl<TopicDescription>> entry : topicFutures.entrySet()) {
                    String topicName = entry.getKey();
                    KafkaFutureImpl<TopicDescription> future = entry.getValue();
                    Errors topicError = errors.get(topicName);
                    if (topicError != null) {
                        future.completeExceptionally(topicError.exception());
                        continue;
                    }
                    if (!cluster.topics().contains(topicName)) {
                        future.completeExceptionally(new UnknownTopicOrPartitionException("Topic " + topicName + " not found."));
                        continue;
                    }
                    boolean isInternal = cluster.internalTopics().contains(topicName);
                    List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topicName);
                    List<TopicPartitionInfo> partitions = new ArrayList<>(partitionInfos.size());
                    for (PartitionInfo partitionInfo : partitionInfos) {
                        TopicPartitionInfo topicPartitionInfo = TopicPartitionInfo.ofReplicasAndObservers(
                            partitionInfo.partition(),
                            leader(partitionInfo),
                            Arrays.asList(partitionInfo.replicas()),
                            Arrays.asList(partitionInfo.observers()),
                            Arrays.asList(partitionInfo.inSyncReplicas()));
                        partitions.add(topicPartitionInfo);
                    }
                    partitions.sort(Comparator.comparingInt(TopicPartitionInfo::partition));
                    TopicDescription topicDescription = new TopicDescription(topicName, isInternal, partitions,
                        validAclOperations(response.topicAuthorizedOperations(topicName).get()));
                    future.complete(topicDescription);
                }
            }

            private Node leader(PartitionInfo partitionInfo) {
                if (partitionInfo.leader() == null || partitionInfo.leader().id() == Node.noNode().id())
                    return null;
                return partitionInfo.leader();
            }

            @Override
            boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
                if (supportsDisablingTopicCreation) {
                    supportsDisablingTopicCreation = false;
                    return true;
                }
                return false;
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(topicFutures.values(), throwable);
            }
        };
        if (!topicNamesList.isEmpty()) {
            runnable.call(call, now);
        }
        return new DescribeTopicsResult(new HashMap<>(topicFutures));
    }

    @Override
    public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        final KafkaFutureImpl<Collection<Node>> describeClusterFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<Node> controllerFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<String> clusterIdFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<Set<AclOperation>> authorizedOperationsFuture = new KafkaFutureImpl<>();

        final long now = time.milliseconds();
        runnable.call(new Call("listNodes", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                // Since this only requests node information, it's safe to pass true for allowAutoTopicCreation (and it
                // simplifies communication with older brokers)
                return new MetadataRequest.Builder(new MetadataRequestData()
                    .setTopics(Collections.emptyList())
                    .setAllowAutoTopicCreation(true)
                    .setIncludeClusterAuthorizedOperations(options.includeAuthorizedOperations()));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                describeClusterFuture.complete(response.brokers());
                controllerFuture.complete(controller(response));
                clusterIdFuture.complete(response.clusterId());
                authorizedOperationsFuture.complete(
                        validAclOperations(response.clusterAuthorizedOperations()));
            }

            private Node controller(MetadataResponse response) {
                if (response.controller() == null || response.controller().id() == MetadataResponse.NO_CONTROLLER_ID)
                    return null;
                return response.controller();
            }

            @Override
            void handleFailure(Throwable throwable) {
                describeClusterFuture.completeExceptionally(throwable);
                controllerFuture.completeExceptionally(throwable);
                clusterIdFuture.completeExceptionally(throwable);
                authorizedOperationsFuture.completeExceptionally(throwable);
            }
        }, now);

        return new DescribeClusterResult(describeClusterFuture, controllerFuture, clusterIdFuture,
            authorizedOperationsFuture);
    }

    @Override
    public DescribeAclsResult describeAcls(final AclBindingFilter filter, DescribeAclsOptions options) {
        if (filter.isUnknown()) {
            KafkaFutureImpl<Collection<AclBinding>> future = new KafkaFutureImpl<>();
            future.completeExceptionally(new InvalidRequestException("The AclBindingFilter " +
                    "must not contain UNKNOWN elements."));
            return new DescribeAclsResult(future);
        }
        final long now = time.milliseconds();
        final KafkaFutureImpl<Collection<AclBinding>> future = new KafkaFutureImpl<>();
        runnable.call(new Call("describeAcls", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            DescribeAclsRequest.Builder createRequest(int timeoutMs) {
                return new DescribeAclsRequest.Builder(filter);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                DescribeAclsResponse response = (DescribeAclsResponse) abstractResponse;
                if (response.error().isFailure()) {
                    future.completeExceptionally(response.error().exception());
                } else {
                    future.complete(DescribeAclsResponse.aclBindings(response.acls()));
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        }, now);
        return new DescribeAclsResult(future);
    }


    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        return createCentralizedAcls(acls, options, null, Node.noNode().id());
    }


    @Confluent
    @Override
    public CreateAclsResult createCentralizedAcls(Collection<AclBinding> acls,
                                                  CreateAclsOptions options,
                                                  String clusterId,
                                                  int writerBrokerId) {
        final long now = time.milliseconds();
        final Map<AclBinding, KafkaFutureImpl<Void>> futures = new HashMap<>();
        final List<AclCreation> aclCreations = new ArrayList<>();
        final List<AclBinding> aclBindingsSent = new ArrayList<>();
        for (AclBinding acl : acls) {
            if (futures.get(acl) == null) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                futures.put(acl, future);
                String indefinite = acl.toFilter().findIndefiniteField();
                if (indefinite == null) {
                    aclCreations.add(CreateAclsRequest.aclCreation(acl));
                    aclBindingsSent.add(acl);
                } else {
                    future.completeExceptionally(new InvalidRequestException("Invalid ACL creation: " +
                        indefinite));
                }
            }
        }
        final CreateAclsRequestData data = new CreateAclsRequestData().setCreations(aclCreations);
        NodeProvider nodeProvider = writerBrokerId == Node.noNode().id() ?
             new LeastLoadedNodeProvider() : new ConstantNodeIdProvider(writerBrokerId);
        runnable.call(new Call("createAcls", calcDeadlineMs(now, options.timeoutMs()),
            nodeProvider) {

            @Override
            CreateAclsRequest.Builder createRequest(int timeoutMs) {
                CreateAclsRequest.Builder builder = new CreateAclsRequest.Builder(data);
                if (clusterId != null) {
                    return builder.setClusterId(clusterId);
                } else
                    return builder;
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                CreateAclsResponse response = (CreateAclsResponse) abstractResponse;
                List<AclCreationResult> responses = response.results();
                Iterator<AclCreationResult> iter = responses.iterator();
                for (AclBinding aclBinding : aclBindingsSent) {
                    KafkaFutureImpl<Void> future = futures.get(aclBinding);
                    if (!iter.hasNext()) {
                        future.completeExceptionally(new UnknownServerException(
                            "The broker reported no creation result for the given ACL: " + aclBinding));
                    } else {
                        AclCreationResult creation = iter.next();
                        Errors error = Errors.forCode(creation.errorCode());
                        ApiError apiError = new ApiError(error, creation.errorMessage());
                        if (apiError.isFailure())
                            future.completeExceptionally(apiError.exception());
                        else
                            future.complete(null);
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);
        return new CreateAclsResult(new HashMap<>(futures));
    }

    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
        return deleteCentralizedAcls(filters, options, null, Node.noNode().id());
    }

    @Confluent
    @Override
    public DeleteAclsResult deleteCentralizedAcls(Collection<AclBindingFilter> filters,
                                                  DeleteAclsOptions options,
                                                  String clusterId,
                                                  int writerBrokerId) {
        final long now = time.milliseconds();
        final Map<AclBindingFilter, KafkaFutureImpl<FilterResults>> futures = new HashMap<>();
        final List<AclBindingFilter> aclBindingFiltersSent = new ArrayList<>();
        final List<DeleteAclsFilter> deleteAclsFilters = new ArrayList<>();
        for (AclBindingFilter filter : filters) {
            if (futures.get(filter) == null) {
                aclBindingFiltersSent.add(filter);
                deleteAclsFilters.add(DeleteAclsRequest.deleteAclsFilter(filter));
                futures.put(filter, new KafkaFutureImpl<>());
            }
        }
        NodeProvider nodeProvider = writerBrokerId == Node.noNode().id() ?
            new LeastLoadedNodeProvider() : new ConstantNodeIdProvider(writerBrokerId);
        final DeleteAclsRequestData data = new DeleteAclsRequestData().setFilters(deleteAclsFilters);
        runnable.call(new Call("deleteAcls", calcDeadlineMs(now, options.timeoutMs()),
            nodeProvider) {

            @Override
            DeleteAclsRequest.Builder createRequest(int timeoutMs) {
                DeleteAclsRequest.Builder builder = new DeleteAclsRequest.Builder(data);
                if (clusterId != null) {
                    return builder.setClusterId(clusterId);
                } else
                    return builder;
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                DeleteAclsResponse response = (DeleteAclsResponse) abstractResponse;
                List<DeleteAclsResponseData.DeleteAclsFilterResult> results = response.filterResults();
                Iterator<DeleteAclsResponseData.DeleteAclsFilterResult> iter = results.iterator();
                for (AclBindingFilter bindingFilter : aclBindingFiltersSent) {
                    KafkaFutureImpl<FilterResults> future = futures.get(bindingFilter);
                    if (!iter.hasNext()) {
                        future.completeExceptionally(new UnknownServerException(
                            "The broker reported no deletion result for the given filter."));
                    } else {
                        DeleteAclsFilterResult filterResult = iter.next();
                        ApiError error = new ApiError(Errors.forCode(filterResult.errorCode()), filterResult.errorMessage());
                        if (error.isFailure()) {
                            future.completeExceptionally(error.exception());
                        } else {
                            List<FilterResult> filterResults = new ArrayList<>();
                            for (DeleteAclsMatchingAcl matchingAcl : filterResult.matchingAcls()) {
                                ApiError aclError = new ApiError(Errors.forCode(matchingAcl.errorCode()),
                                    matchingAcl.errorMessage());
                                AclBinding aclBinding = DeleteAclsResponse.aclBinding(matchingAcl);
                                filterResults.add(new FilterResult(aclBinding, aclError.exception()));
                            }
                            future.complete(new FilterResults(filterResults));
                        }
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);
        return new DeleteAclsResult(new HashMap<>(futures));
    }

    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> configResources, final DescribeConfigsOptions options) {
        final Map<ConfigResource, KafkaFutureImpl<Config>> unifiedRequestFutures = new HashMap<>();
        final Map<ConfigResource, KafkaFutureImpl<Config>> brokerFutures = new HashMap<>(configResources.size());

        // The BROKER resources which we want to describe.  We must make a separate DescribeConfigs
        // request for every BROKER resource we want to describe.
        final Collection<ConfigResource> brokerResources = new ArrayList<>();

        // The non-BROKER resources which we want to describe.  These resources can be described by a
        // single, unified DescribeConfigs request.
        final Collection<ConfigResource> unifiedRequestResources = new ArrayList<>(configResources.size());

        for (ConfigResource resource : configResources) {
            if (dependsOnSpecificNode(resource)) {
                brokerFutures.put(resource, new KafkaFutureImpl<>());
                brokerResources.add(resource);
            } else {
                unifiedRequestFutures.put(resource, new KafkaFutureImpl<>());
                unifiedRequestResources.add(resource);
            }
        }

        final long now = time.milliseconds();
        if (!unifiedRequestResources.isEmpty()) {
            runnable.call(new Call("describeConfigs", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {

                @Override
                DescribeConfigsRequest.Builder createRequest(int timeoutMs) {
                    return new DescribeConfigsRequest.Builder(unifiedRequestResources)
                            .includeSynonyms(options.includeSynonyms());
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    DescribeConfigsResponse response = (DescribeConfigsResponse) abstractResponse;
                    for (Map.Entry<ConfigResource, KafkaFutureImpl<Config>> entry : unifiedRequestFutures.entrySet()) {
                        ConfigResource configResource = entry.getKey();
                        KafkaFutureImpl<Config> future = entry.getValue();
                        DescribeConfigsResponse.Config config = response.config(configResource);
                        if (config == null) {
                            future.completeExceptionally(new UnknownServerException(
                                "Malformed broker response: missing config for " + configResource));
                            continue;
                        }
                        if (config.error().isFailure()) {
                            future.completeExceptionally(config.error().exception());
                            continue;
                        }
                        List<ConfigEntry> configEntries = new ArrayList<>();
                        for (DescribeConfigsResponse.ConfigEntry configEntry : config.entries()) {
                            configEntries.add(new ConfigEntry(configEntry.name(),
                                    configEntry.value(), configSource(configEntry.source()),
                                    configEntry.isSensitive(), configEntry.isReadOnly(),
                                    configSynonyms(configEntry)));
                        }
                        future.complete(new Config(configEntries));
                    }
                }

                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(unifiedRequestFutures.values(), throwable);
                }
            }, now);
        }

        for (Map.Entry<ConfigResource, KafkaFutureImpl<Config>> entry : brokerFutures.entrySet()) {
            final KafkaFutureImpl<Config> brokerFuture = entry.getValue();
            final ConfigResource resource = entry.getKey();
            final int nodeId = Integer.parseInt(resource.name());
            runnable.call(new Call("describeBrokerConfigs", calcDeadlineMs(now, options.timeoutMs()),
                    new ConstantNodeIdProvider(nodeId)) {

                @Override
                DescribeConfigsRequest.Builder createRequest(int timeoutMs) {
                    return new DescribeConfigsRequest.Builder(Collections.singleton(resource))
                            .includeSynonyms(options.includeSynonyms());
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    DescribeConfigsResponse response = (DescribeConfigsResponse) abstractResponse;
                    DescribeConfigsResponse.Config config = response.configs().get(resource);

                    if (config == null) {
                        brokerFuture.completeExceptionally(new UnknownServerException(
                            "Malformed broker response: missing config for " + resource));
                        return;
                    }
                    if (config.error().isFailure())
                        brokerFuture.completeExceptionally(config.error().exception());
                    else {
                        List<ConfigEntry> configEntries = new ArrayList<>();
                        for (DescribeConfigsResponse.ConfigEntry configEntry : config.entries()) {
                            configEntries.add(new ConfigEntry(configEntry.name(), configEntry.value(),
                                configSource(configEntry.source()), configEntry.isSensitive(), configEntry.isReadOnly(),
                                configSynonyms(configEntry)));
                        }
                        brokerFuture.complete(new Config(configEntries));
                    }
                }

                @Override
                void handleFailure(Throwable throwable) {
                    brokerFuture.completeExceptionally(throwable);
                }
            }, now);
        }
        final Map<ConfigResource, KafkaFuture<Config>> allFutures = new HashMap<>();
        allFutures.putAll(brokerFutures);
        allFutures.putAll(unifiedRequestFutures);
        return new DescribeConfigsResult(allFutures);
    }

    private List<ConfigEntry.ConfigSynonym> configSynonyms(DescribeConfigsResponse.ConfigEntry configEntry) {
        List<ConfigEntry.ConfigSynonym> synonyms = new ArrayList<>(configEntry.synonyms().size());
        for (DescribeConfigsResponse.ConfigSynonym synonym : configEntry.synonyms()) {
            synonyms.add(new ConfigEntry.ConfigSynonym(synonym.name(), synonym.value(), configSource(synonym.source())));
        }
        return synonyms;
    }

    private ConfigEntry.ConfigSource configSource(DescribeConfigsResponse.ConfigSource source) {
        ConfigEntry.ConfigSource configSource;
        switch (source) {
            case TOPIC_CONFIG:
                configSource = ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG;
                break;
            case DYNAMIC_BROKER_CONFIG:
                configSource = ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG;
                break;
            case DYNAMIC_DEFAULT_BROKER_CONFIG:
                configSource = ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG;
                break;
            case STATIC_BROKER_CONFIG:
                configSource = ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG;
                break;
            case DYNAMIC_BROKER_LOGGER_CONFIG:
                configSource = ConfigEntry.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG;
                break;
            case DEFAULT_CONFIG:
                configSource = ConfigEntry.ConfigSource.DEFAULT_CONFIG;
                break;
            default:
                throw new IllegalArgumentException("Unexpected config source " + source);
        }
        return configSource;
    }

    @Override
    @Deprecated
    public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, final AlterConfigsOptions options) {
        final Map<ConfigResource, KafkaFutureImpl<Void>> allFutures = new HashMap<>();
        // We must make a separate AlterConfigs request for every BROKER resource we want to alter
        // and send the request to that specific broker. Other resources are grouped together into
        // a single request that may be sent to any broker.
        final Collection<ConfigResource> unifiedRequestResources = new ArrayList<>();

        for (ConfigResource resource : configs.keySet()) {
            if (dependsOnSpecificNode(resource)) {
                NodeProvider nodeProvider = new ConstantNodeIdProvider(Integer.parseInt(resource.name()));
                allFutures.putAll(alterConfigs(configs, options, Collections.singleton(resource), nodeProvider));
            } else
                unifiedRequestResources.add(resource);
        }
        if (!unifiedRequestResources.isEmpty())
          allFutures.putAll(alterConfigs(configs, options, unifiedRequestResources, new LeastLoadedNodeProvider()));
        return new AlterConfigsResult(new HashMap<>(allFutures));
    }

    private Map<ConfigResource, KafkaFutureImpl<Void>> alterConfigs(Map<ConfigResource, Config> configs,
                                                                    final AlterConfigsOptions options,
                                                                    Collection<ConfigResource> resources,
                                                                    NodeProvider nodeProvider) {
        final Map<ConfigResource, KafkaFutureImpl<Void>> futures = new HashMap<>();
        final Map<ConfigResource, AlterConfigsRequest.Config> requestMap = new HashMap<>(resources.size());
        for (ConfigResource resource : resources) {
            List<AlterConfigsRequest.ConfigEntry> configEntries = new ArrayList<>();
            for (ConfigEntry configEntry: configs.get(resource).entries())
                configEntries.add(new AlterConfigsRequest.ConfigEntry(configEntry.name(), configEntry.value()));
            requestMap.put(resource, new AlterConfigsRequest.Config(configEntries));
            futures.put(resource, new KafkaFutureImpl<>());
        }

        final long now = time.milliseconds();
        runnable.call(new Call("alterConfigs", calcDeadlineMs(now, options.timeoutMs()), nodeProvider) {

            @Override
            public AlterConfigsRequest.Builder createRequest(int timeoutMs) {
                return new AlterConfigsRequest.Builder(requestMap, options.shouldValidateOnly());
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                AlterConfigsResponse response = (AlterConfigsResponse) abstractResponse;
                for (Map.Entry<ConfigResource, KafkaFutureImpl<Void>> entry : futures.entrySet()) {
                    KafkaFutureImpl<Void> future = entry.getValue();
                    ApiException exception = response.errors().get(entry.getKey()).exception();
                    if (exception != null) {
                        future.completeExceptionally(exception);
                    } else {
                        future.complete(null);
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);
        return futures;
    }

    @Override
    public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs,
                                                                 final AlterConfigsOptions options) {
        final Map<ConfigResource, KafkaFutureImpl<Void>> allFutures = new HashMap<>();
        // We must make a separate AlterConfigs request for every BROKER resource we want to alter
        // and send the request to that specific broker. Other resources are grouped together into
        // a single request that may be sent to any broker.
        final Collection<ConfigResource> unifiedRequestResources = new ArrayList<>();

        for (ConfigResource resource : configs.keySet()) {
            if (dependsOnSpecificNode(resource)) {
                NodeProvider nodeProvider = new ConstantNodeIdProvider(Integer.parseInt(resource.name()));
                allFutures.putAll(incrementalAlterConfigs(configs, options, Collections.singleton(resource), nodeProvider));
            } else
                unifiedRequestResources.add(resource);
        }
        if (!unifiedRequestResources.isEmpty())
            allFutures.putAll(incrementalAlterConfigs(configs, options, unifiedRequestResources, new LeastLoadedNodeProvider()));

        return new AlterConfigsResult(new HashMap<>(allFutures));
    }

    private Map<ConfigResource, KafkaFutureImpl<Void>> incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs,
                                                                    final AlterConfigsOptions options,
                                                                    Collection<ConfigResource> resources,
                                                                    NodeProvider nodeProvider) {
        final Map<ConfigResource, KafkaFutureImpl<Void>> futures = new HashMap<>();
        for (ConfigResource resource : resources)
            futures.put(resource, new KafkaFutureImpl<>());

        final long now = time.milliseconds();
        runnable.call(new Call("incrementalAlterConfigs", calcDeadlineMs(now, options.timeoutMs()), nodeProvider) {

            @Override
            public IncrementalAlterConfigsRequest.Builder createRequest(int timeoutMs) {
                return new IncrementalAlterConfigsRequest.Builder(
                        toIncrementalAlterConfigsRequestData(resources, configs, options.shouldValidateOnly()));
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                IncrementalAlterConfigsResponse response = (IncrementalAlterConfigsResponse) abstractResponse;
                Map<ConfigResource, ApiError> errors = IncrementalAlterConfigsResponse.fromResponseData(response.data());
                for (Map.Entry<ConfigResource, KafkaFutureImpl<Void>> entry : futures.entrySet()) {
                    KafkaFutureImpl<Void> future = entry.getValue();
                    ApiException exception = errors.get(entry.getKey()).exception();
                    if (exception != null) {
                        future.completeExceptionally(exception);
                    } else {
                        future.complete(null);
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);
        return futures;
    }

    private IncrementalAlterConfigsRequestData toIncrementalAlterConfigsRequestData(final Collection<ConfigResource> resources,
                                                                                    final Map<ConfigResource, Collection<AlterConfigOp>> configs,
                                                                                    final boolean validateOnly) {
        IncrementalAlterConfigsRequestData requestData = new IncrementalAlterConfigsRequestData();
        requestData.setValidateOnly(validateOnly);
        for (ConfigResource resource : resources) {
            AlterableConfigCollection alterableConfigSet = new AlterableConfigCollection();
            for (AlterConfigOp configEntry : configs.get(resource))
                alterableConfigSet.add(new AlterableConfig().
                        setName(configEntry.configEntry().name()).
                        setValue(configEntry.configEntry().value()).
                        setConfigOperation(configEntry.opType().id()));

            AlterConfigsResource alterConfigsResource = new AlterConfigsResource();
            alterConfigsResource.setResourceType(resource.type().id()).
                    setResourceName(resource.name()).setConfigs(alterableConfigSet);
            requestData.resources().add(alterConfigsResource);
        }
        return requestData;
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment, final AlterReplicaLogDirsOptions options) {
        final Map<TopicPartitionReplica, KafkaFutureImpl<Void>> futures = new HashMap<>(replicaAssignment.size());

        for (TopicPartitionReplica replica : replicaAssignment.keySet())
            futures.put(replica, new KafkaFutureImpl<>());

        Map<Integer, Map<TopicPartition, String>> replicaAssignmentByBroker = new HashMap<>();
        for (Map.Entry<TopicPartitionReplica, String> entry: replicaAssignment.entrySet()) {
            TopicPartitionReplica replica = entry.getKey();
            String logDir = entry.getValue();
            int brokerId = replica.brokerId();
            TopicPartition topicPartition = new TopicPartition(replica.topic(), replica.partition());
            if (!replicaAssignmentByBroker.containsKey(brokerId))
                replicaAssignmentByBroker.put(brokerId, new HashMap<>());
            replicaAssignmentByBroker.get(brokerId).put(topicPartition, logDir);
        }

        final long now = time.milliseconds();
        for (Map.Entry<Integer, Map<TopicPartition, String>> entry: replicaAssignmentByBroker.entrySet()) {
            final int brokerId = entry.getKey();
            final Map<TopicPartition, String> assignment = entry.getValue();

            runnable.call(new Call("alterReplicaLogDirs", calcDeadlineMs(now, options.timeoutMs()),
                new ConstantNodeIdProvider(brokerId)) {

                @Override
                public AlterReplicaLogDirsRequest.Builder createRequest(int timeoutMs) {
                    return new AlterReplicaLogDirsRequest.Builder(assignment);
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    AlterReplicaLogDirsResponse response = (AlterReplicaLogDirsResponse) abstractResponse;
                    for (Map.Entry<TopicPartition, Errors> responseEntry: response.responses().entrySet()) {
                        TopicPartition tp = responseEntry.getKey();
                        Errors error = responseEntry.getValue();
                        TopicPartitionReplica replica = new TopicPartitionReplica(tp.topic(), tp.partition(), brokerId);
                        KafkaFutureImpl<Void> future = futures.get(replica);
                        if (future == null) {
                            handleFailure(new IllegalStateException(
                                "The partition " + tp + " in the response from broker " + brokerId + " is not in the request"));
                        } else if (error == Errors.NONE) {
                            future.complete(null);
                        } else {
                            future.completeExceptionally(error.exception());
                        }
                    }
                }
                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(futures.values(), throwable);
                }
            }, now);
        }

        return new AlterReplicaLogDirsResult(new HashMap<>(futures));
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
        final Map<Integer, KafkaFutureImpl<Map<String, DescribeLogDirsResponse.LogDirInfo>>> futures = new HashMap<>(brokers.size());

        for (Integer brokerId: brokers) {
            futures.put(brokerId, new KafkaFutureImpl<>());
        }

        final long now = time.milliseconds();
        for (final Integer brokerId: brokers) {
            runnable.call(new Call("describeLogDirs", calcDeadlineMs(now, options.timeoutMs()),
                new ConstantNodeIdProvider(brokerId)) {

                @Override
                public DescribeLogDirsRequest.Builder createRequest(int timeoutMs) {
                    // Query selected partitions in all log directories
                    return new DescribeLogDirsRequest.Builder(new DescribeLogDirsRequestData().setTopics(null));
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    DescribeLogDirsResponse response = (DescribeLogDirsResponse) abstractResponse;
                    KafkaFutureImpl<Map<String, DescribeLogDirsResponse.LogDirInfo>> future = futures.get(brokerId);
                    if (response.logDirInfos().size() > 0) {
                        future.complete(response.logDirInfos());
                    } else {
                        // response.logDirInfos() will be empty if and only if the user is not authorized to describe clsuter resource.
                        future.completeExceptionally(Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
                    }
                }
                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(futures.values(), throwable);
                }
            }, now);
        }

        return new DescribeLogDirsResult(new HashMap<>(futures));
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options) {
        final Map<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> futures = new HashMap<>(replicas.size());

        for (TopicPartitionReplica replica : replicas) {
            futures.put(replica, new KafkaFutureImpl<>());
        }

        Map<Integer, DescribeLogDirsRequestData> partitionsByBroker = new HashMap<>();

        for (TopicPartitionReplica replica: replicas) {
            DescribeLogDirsRequestData requestData = partitionsByBroker.computeIfAbsent(replica.brokerId(),
                brokerId -> new DescribeLogDirsRequestData());
            DescribableLogDirTopic describableLogDirTopic = requestData.topics().find(replica.topic());
            if (describableLogDirTopic == null) {
                List<Integer> partitionIndex = new ArrayList<>();
                partitionIndex.add(replica.partition());
                describableLogDirTopic = new DescribableLogDirTopic().setTopic(replica.topic())
                        .setPartitionIndex(partitionIndex);
                requestData.topics().add(describableLogDirTopic);
            } else {
                describableLogDirTopic.partitionIndex().add(replica.partition());
            }
        }

        final long now = time.milliseconds();
        for (Map.Entry<Integer, DescribeLogDirsRequestData> entry: partitionsByBroker.entrySet()) {
            final int brokerId = entry.getKey();
            final DescribeLogDirsRequestData topicPartitions = entry.getValue();
            final Map<TopicPartition, ReplicaLogDirInfo> replicaDirInfoByPartition = new HashMap<>();
            for (DescribableLogDirTopic topicPartition: topicPartitions.topics()) {
                for (Integer partitionId : topicPartition.partitionIndex()) {
                    replicaDirInfoByPartition.put(new TopicPartition(topicPartition.topic(), partitionId), new ReplicaLogDirInfo());
                }
            }

            runnable.call(new Call("describeReplicaLogDirs", calcDeadlineMs(now, options.timeoutMs()),
                new ConstantNodeIdProvider(brokerId)) {

                @Override
                public DescribeLogDirsRequest.Builder createRequest(int timeoutMs) {
                    // Query selected partitions in all log directories
                    return new DescribeLogDirsRequest.Builder(topicPartitions);
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    DescribeLogDirsResponse response = (DescribeLogDirsResponse) abstractResponse;
                    for (Map.Entry<String, DescribeLogDirsResponse.LogDirInfo> responseEntry: response.logDirInfos().entrySet()) {
                        String logDir = responseEntry.getKey();
                        DescribeLogDirsResponse.LogDirInfo logDirInfo = responseEntry.getValue();

                        // No replica info will be provided if the log directory is offline
                        if (logDirInfo.error == Errors.KAFKA_STORAGE_ERROR)
                            continue;
                        if (logDirInfo.error != Errors.NONE)
                            handleFailure(new IllegalStateException(
                                "The error " + logDirInfo.error + " for log directory " + logDir + " in the response from broker " + brokerId + " is illegal"));

                        for (Map.Entry<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicaInfoEntry: logDirInfo.replicaInfos.entrySet()) {
                            TopicPartition tp = replicaInfoEntry.getKey();
                            DescribeLogDirsResponse.ReplicaInfo replicaInfo = replicaInfoEntry.getValue();
                            ReplicaLogDirInfo replicaLogDirInfo = replicaDirInfoByPartition.get(tp);
                            if (replicaLogDirInfo == null) {
                                handleFailure(new IllegalStateException(
                                    "The partition " + tp + " in the response from broker " + brokerId + " is not in the request"));
                            } else if (replicaInfo.isFuture) {
                                replicaDirInfoByPartition.put(tp, new ReplicaLogDirInfo(replicaLogDirInfo.getCurrentReplicaLogDir(),
                                                                                        replicaLogDirInfo.getCurrentReplicaOffsetLag(),
                                                                                        logDir,
                                                                                        replicaInfo.offsetLag));
                            } else {
                                replicaDirInfoByPartition.put(tp, new ReplicaLogDirInfo(logDir,
                                                                                        replicaInfo.offsetLag,
                                                                                        replicaLogDirInfo.getFutureReplicaLogDir(),
                                                                                        replicaLogDirInfo.getFutureReplicaOffsetLag()));
                            }
                        }
                    }

                    for (Map.Entry<TopicPartition, ReplicaLogDirInfo> entry: replicaDirInfoByPartition.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        KafkaFutureImpl<ReplicaLogDirInfo> future = futures.get(new TopicPartitionReplica(tp.topic(), tp.partition(), brokerId));
                        future.complete(entry.getValue());
                    }
                }
                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(futures.values(), throwable);
                }
            }, now);
        }

        return new DescribeReplicaLogDirsResult(new HashMap<>(futures));
    }

    @Override
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions,
                                                   final CreatePartitionsOptions options) {
        final Map<String, KafkaFutureImpl<Void>> futures = new HashMap<>(newPartitions.size());
        for (String topic : newPartitions.keySet()) {
            futures.put(topic, new KafkaFutureImpl<>());
        }

        final List<CreatePartitionsTopic> topics = newPartitions.entrySet().stream()
                .map(partitionsEntry -> {
                    NewPartitions newPartition = partitionsEntry.getValue();
                    List<List<Integer>> newAssignments = newPartition.assignments();
                    List<CreatePartitionsAssignment> assignments = newAssignments == null ? null :
                            newAssignments.stream()
                            .map(brokerIds -> new CreatePartitionsAssignment().setBrokerIds(brokerIds))
                            .collect(Collectors.toList());
                    return new CreatePartitionsTopic()
                            .setName(partitionsEntry.getKey())
                            .setCount(newPartition.totalCount())
                            .setAssignments(assignments);
                })
                .collect(Collectors.toList());

        final long now = time.milliseconds();
        runnable.call(new Call("createPartitions", calcDeadlineMs(now, options.timeoutMs()),
                new ControllerNodeProvider()) {

            @Override
            public CreatePartitionsRequest.Builder createRequest(int timeoutMs) {
                CreatePartitionsRequestData requestData = new CreatePartitionsRequestData()
                        .setTopics(topics)
                        .setValidateOnly(options.validateOnly())
                        .setTimeoutMs(timeoutMs);

                return new CreatePartitionsRequest.Builder(requestData);
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                CreatePartitionsResponse response = (CreatePartitionsResponse) abstractResponse;
                // Check for controller change
                for (CreatePartitionsTopicResult topicResult: response.data().results()) {
                    Errors error = Errors.forCode(topicResult.errorCode());
                    if (error == Errors.NOT_CONTROLLER) {
                        metadataManager.clearController();
                        metadataManager.requestUpdate();
                        throw error.exception();
                    }
                }
                for (CreatePartitionsTopicResult topicResult: response.data().results()) {
                    Errors error = Errors.forCode(topicResult.errorCode());
                    KafkaFutureImpl<Void> future = futures.get(topicResult.name());
                    if (error == Errors.NONE) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(error.exception(topicResult.errorMessage()));
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);
        return new CreatePartitionsResult(new HashMap<>(futures));
    }

    @Override
    public DeleteRecordsResult deleteRecords(final Map<TopicPartition, RecordsToDelete> recordsToDelete,
                                             final DeleteRecordsOptions options) {

        // requests need to be sent to partitions leader nodes so ...
        // ... from the provided map it's needed to create more maps grouping topic/partition per leader

        final Map<TopicPartition, KafkaFutureImpl<DeletedRecords>> futures = new HashMap<>(recordsToDelete.size());
        for (TopicPartition topicPartition: recordsToDelete.keySet()) {
            futures.put(topicPartition, new KafkaFutureImpl<>());
        }

        // preparing topics list for asking metadata about them
        final Set<String> topics = new HashSet<>();
        for (TopicPartition topicPartition: recordsToDelete.keySet()) {
            topics.add(topicPartition.topic());
        }

        final long nowMetadata = time.milliseconds();
        final long deadline = calcDeadlineMs(nowMetadata, options.timeoutMs());
        // asking for topics metadata for getting partitions leaders
        runnable.call(new Call("topicsMetadata", deadline,
                new LeastLoadedNodeProvider()) {

            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                return new MetadataRequest.Builder(new MetadataRequestData()
                    .setTopics(convertToMetadataRequestTopic(topics))
                    .setAllowAutoTopicCreation(false));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;

                Map<String, Errors> errors = response.errors();
                Cluster cluster = response.cluster();

                // Group topic partitions by leader
                Map<Node, Map<String, DeleteRecordsTopic>> leaders = new HashMap<>();
                for (Map.Entry<TopicPartition, RecordsToDelete> entry: recordsToDelete.entrySet()) {
                    TopicPartition topicPartition = entry.getKey();
                    KafkaFutureImpl<DeletedRecords> future = futures.get(topicPartition);

                    // Fail partitions with topic errors
                    Errors topicError = errors.get(topicPartition.topic());
                    if (errors.containsKey(topicPartition.topic())) {
                        future.completeExceptionally(topicError.exception());
                    } else {
                        Node node = cluster.leaderFor(topicPartition);
                        if (node != null) {
                            Map<String, DeleteRecordsTopic> deletionsForLeader = leaders.computeIfAbsent(
                                    node, key -> new HashMap<>());
                            DeleteRecordsTopic deleteRecords = deletionsForLeader.get(topicPartition.topic());
                            if (deleteRecords == null) {
                                deleteRecords = new DeleteRecordsTopic()
                                        .setName(topicPartition.topic());
                                deletionsForLeader.put(topicPartition.topic(), deleteRecords);
                            }
                            deleteRecords.partitions().add(new DeleteRecordsPartition()
                                    .setPartitionIndex(topicPartition.partition())
                                    .setOffset(entry.getValue().beforeOffset()));
                        } else {
                            future.completeExceptionally(Errors.LEADER_NOT_AVAILABLE.exception());
                        }
                    }
                }

                final long deleteRecordsCallTimeMs = time.milliseconds();

                for (final Map.Entry<Node, Map<String, DeleteRecordsTopic>> entry : leaders.entrySet()) {
                    final Map<String, DeleteRecordsTopic> partitionDeleteOffsets = entry.getValue();
                    final int brokerId = entry.getKey().id();

                    runnable.call(new Call("deleteRecords", deadline,
                            new ConstantNodeIdProvider(brokerId)) {

                        @Override
                        DeleteRecordsRequest.Builder createRequest(int timeoutMs) {
                            return new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData()
                                    .setTimeoutMs(timeoutMs)
                                    .setTopics(new ArrayList<>(partitionDeleteOffsets.values())));
                        }

                        @Override
                        void handleResponse(AbstractResponse abstractResponse) {
                            DeleteRecordsResponse response = (DeleteRecordsResponse) abstractResponse;
                            for (DeleteRecordsTopicResult topicResult: response.data().topics()) {
                                for (DeleteRecordsResponseData.DeleteRecordsPartitionResult partitionResult : topicResult.partitions()) {
                                    KafkaFutureImpl<DeletedRecords> future = futures.get(new TopicPartition(topicResult.name(), partitionResult.partitionIndex()));
                                    if (partitionResult.errorCode() == Errors.NONE.code()) {
                                        future.complete(new DeletedRecords(partitionResult.lowWatermark()));
                                    } else {
                                        future.completeExceptionally(Errors.forCode(partitionResult.errorCode()).exception());
                                    }
                                }
                            }
                        }

                        @Override
                        void handleFailure(Throwable throwable) {
                            Stream<KafkaFutureImpl<DeletedRecords>> callFutures =
                                    partitionDeleteOffsets.values().stream().flatMap(
                                        recordsToDelete ->
                                                recordsToDelete.partitions().stream().map(partitionsToDelete ->
                                                    new TopicPartition(recordsToDelete.name(), partitionsToDelete.partitionIndex()))
                                    ).map(futures::get);
                            completeAllExceptionally(callFutures, throwable);
                        }
                    }, deleteRecordsCallTimeMs);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, nowMetadata);

        return new DeleteRecordsResult(new HashMap<>(futures));
    }

    @Override
    public CreateDelegationTokenResult createDelegationToken(final CreateDelegationTokenOptions options) {
        final KafkaFutureImpl<DelegationToken> delegationTokenFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        List<CreatableRenewers> renewers = new ArrayList<>();
        for (KafkaPrincipal principal : options.renewers()) {
            renewers.add(new CreatableRenewers()
                    .setPrincipalName(principal.getName())
                    .setPrincipalType(principal.getPrincipalType()));
        }
        runnable.call(new Call("createDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            CreateDelegationTokenRequest.Builder createRequest(int timeoutMs) {
                return new CreateDelegationTokenRequest.Builder(
                        new CreateDelegationTokenRequestData()
                            .setRenewers(renewers)
                            .setMaxLifetimeMs(options.maxlifeTimeMs()));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                CreateDelegationTokenResponse response = (CreateDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    delegationTokenFuture.completeExceptionally(response.error().exception());
                } else {
                    CreateDelegationTokenResponseData data = response.data();
                    TokenInformation tokenInfo =  new TokenInformation(data.tokenId(), new KafkaPrincipal(data.principalType(), data.principalName()),
                        options.renewers(), data.issueTimestampMs(), data.maxTimestampMs(), data.expiryTimestampMs());
                    DelegationToken token = new DelegationToken(tokenInfo, data.hmac());
                    delegationTokenFuture.complete(token);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                delegationTokenFuture.completeExceptionally(throwable);
            }
        }, now);

        return new CreateDelegationTokenResult(delegationTokenFuture);
    }

    @Override
    public RenewDelegationTokenResult renewDelegationToken(final byte[] hmac, final RenewDelegationTokenOptions options) {
        final KafkaFutureImpl<Long>  expiryTimeFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("renewDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            RenewDelegationTokenRequest.Builder createRequest(int timeoutMs) {
                return new RenewDelegationTokenRequest.Builder(
                        new RenewDelegationTokenRequestData()
                        .setHmac(hmac)
                        .setRenewPeriodMs(options.renewTimePeriodMs()));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                RenewDelegationTokenResponse response = (RenewDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    expiryTimeFuture.completeExceptionally(response.error().exception());
                } else {
                    expiryTimeFuture.complete(response.expiryTimestamp());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                expiryTimeFuture.completeExceptionally(throwable);
            }
        }, now);

        return new RenewDelegationTokenResult(expiryTimeFuture);
    }

    @Override
    public ExpireDelegationTokenResult expireDelegationToken(final byte[] hmac, final ExpireDelegationTokenOptions options) {
        final KafkaFutureImpl<Long>  expiryTimeFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("expireDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            ExpireDelegationTokenRequest.Builder createRequest(int timeoutMs) {
                return new ExpireDelegationTokenRequest.Builder(
                        new ExpireDelegationTokenRequestData()
                            .setHmac(hmac)
                            .setExpiryTimePeriodMs(options.expiryTimePeriodMs()));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                ExpireDelegationTokenResponse response = (ExpireDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    expiryTimeFuture.completeExceptionally(response.error().exception());
                } else {
                    expiryTimeFuture.complete(response.expiryTimestamp());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                expiryTimeFuture.completeExceptionally(throwable);
            }
        }, now);

        return new ExpireDelegationTokenResult(expiryTimeFuture);
    }

    @Override
    public DescribeDelegationTokenResult describeDelegationToken(final DescribeDelegationTokenOptions options) {
        final KafkaFutureImpl<List<DelegationToken>>  tokensFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("describeDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            DescribeDelegationTokenRequest.Builder createRequest(int timeoutMs) {
                return new DescribeDelegationTokenRequest.Builder(options.owners());
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                DescribeDelegationTokenResponse response = (DescribeDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    tokensFuture.completeExceptionally(response.error().exception());
                } else {
                    tokensFuture.complete(response.tokens());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                tokensFuture.completeExceptionally(throwable);
            }
        }, now);

        return new DescribeDelegationTokenResult(tokensFuture);
    }

    private void rescheduleFindCoordinatorTask(ConsumerGroupOperationContext<?, ?> context, Supplier<Call> nextCall, Call failedCall) {
        log.info("Node {} is no longer the Coordinator. Retrying with new coordinator.",
                context.node().orElse(null));
        // Requeue the task so that we can try with new coordinator
        context.setNode(null);

        Call call = nextCall.get();
        call.tries = failedCall.tries + 1;
        call.nextAllowedTryMs = calculateNextAllowedRetryMs();

        Call findCoordinatorCall = getFindCoordinatorCall(context, nextCall);
        runnable.call(findCoordinatorCall, time.milliseconds());
    }

    private void rescheduleMetadataTask(MetadataOperationContext<?, ?> context, Supplier<List<Call>> nextCalls) {
        log.info("Retrying to fetch metadata.");
        // Requeue the task so that we can re-attempt fetching metadata
        context.setResponse(Optional.empty());
        Call metadataCall = getMetadataCall(context, nextCalls);
        runnable.call(metadataCall, time.milliseconds());
    }

    private static <T> Map<String, KafkaFutureImpl<T>> createFutures(Collection<String> groupIds) {
        return new HashSet<>(groupIds).stream().collect(
            Collectors.toMap(groupId -> groupId,
                groupId -> {
                    if (groupIdIsUnrepresentable(groupId)) {
                        KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
                        future.completeExceptionally(new InvalidGroupIdException("The given group id '" +
                                groupId + "' cannot be represented in a request."));
                        return future;
                    } else {
                        return new KafkaFutureImpl<>();
                    }
                }
            ));
    }

    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(final Collection<String> groupIds,
                                                               final DescribeConsumerGroupsOptions options) {

        final Map<String, KafkaFutureImpl<ConsumerGroupDescription>> futures = createFutures(groupIds);

        // TODO: KAFKA-6788, we should consider grouping the request per coordinator and send one request with a list of
        // all consumer groups this coordinator host
        for (final Map.Entry<String, KafkaFutureImpl<ConsumerGroupDescription>> entry : futures.entrySet()) {
            // skip sending request for those futures that already failed.
            if (entry.getValue().isCompletedExceptionally())
                continue;

            final String groupId = entry.getKey();

            final long startFindCoordinatorMs = time.milliseconds();
            final long deadline = calcDeadlineMs(startFindCoordinatorMs, options.timeoutMs());
            ConsumerGroupOperationContext<ConsumerGroupDescription, DescribeConsumerGroupsOptions> context =
                    new ConsumerGroupOperationContext<>(groupId, options, deadline, futures.get(groupId));
            Call findCoordinatorCall = getFindCoordinatorCall(context,
                () -> getDescribeConsumerGroupsCall(context));
            runnable.call(findCoordinatorCall, startFindCoordinatorMs);
        }

        return new DescribeConsumerGroupsResult(new HashMap<>(futures));
    }

    /**
     * Returns a {@code Call} object to fetch the coordinator for a consumer group id. Takes another Call
     * parameter to schedule action that need to be taken using the coordinator. The param is a Supplier
     * so that it can be lazily created, so that it can use the results of find coordinator call in its
     * construction.
     *
     * @param <T> The type of return value of the KafkaFuture, like ConsumerGroupDescription, Void etc.
     * @param <O> The type of configuration option, like DescribeConsumerGroupsOptions, ListConsumerGroupsOptions etc
     */
    private <T, O extends AbstractOptions<O>> Call getFindCoordinatorCall(ConsumerGroupOperationContext<T, O> context,
                                                                          Supplier<Call> nextCall) {
        return new Call("findCoordinator", context.deadline(), new LeastLoadedNodeProvider()) {
            @Override
            FindCoordinatorRequest.Builder createRequest(int timeoutMs) {
                return new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                                .setKeyType(CoordinatorType.GROUP.id())
                                .setKey(context.groupId()));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                final FindCoordinatorResponse response = (FindCoordinatorResponse) abstractResponse;

                if (handleGroupRequestError(response.error(), context.future()))
                    return;

                context.setNode(response.node());

                runnable.call(nextCall.get(), time.milliseconds());
            }

            @Override
            void handleFailure(Throwable throwable) {
                context.future().completeExceptionally(throwable);
            }
        };
    }

    private Call getDescribeConsumerGroupsCall(
            ConsumerGroupOperationContext<ConsumerGroupDescription, DescribeConsumerGroupsOptions> context) {
        return new Call("describeConsumerGroups",
                context.deadline(),
                new ConstantNodeIdProvider(context.node().get().id())) {
            @Override
            DescribeGroupsRequest.Builder createRequest(int timeoutMs) {
                return new DescribeGroupsRequest.Builder(
                    new DescribeGroupsRequestData()
                        .setGroups(Collections.singletonList(context.groupId()))
                        .setIncludeAuthorizedOperations(context.options().includeAuthorizedOperations()));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                final DescribeGroupsResponse response = (DescribeGroupsResponse) abstractResponse;

                List<DescribedGroup> describedGroups = response.data().groups();
                if (describedGroups.isEmpty()) {
                    context.future().completeExceptionally(
                            new InvalidGroupIdException("No consumer group found for GroupId: " + context.groupId()));
                    return;
                }

                if (describedGroups.size() > 1 ||
                        !describedGroups.get(0).groupId().equals(context.groupId())) {
                    String ids = Arrays.toString(describedGroups.stream().map(DescribedGroup::groupId).toArray());
                    context.future().completeExceptionally(new InvalidGroupIdException(
                            "DescribeConsumerGroup request for GroupId: " + context.groupId() + " returned " + ids));
                    return;
                }

                final DescribedGroup describedGroup = describedGroups.get(0);

                // If coordinator changed since we fetched it, retry
                if (ConsumerGroupOperationContext.hasCoordinatorMoved(response)) {
                    Call call = getDescribeConsumerGroupsCall(context);
                    rescheduleFindCoordinatorTask(context, () -> call, this);
                    return;
                }

                final Errors groupError = Errors.forCode(describedGroup.errorCode());
                if (handleGroupRequestError(groupError, context.future()))
                    return;

                final String protocolType = describedGroup.protocolType();
                if (protocolType.equals(ConsumerProtocol.PROTOCOL_TYPE) || protocolType.isEmpty()) {
                    final List<DescribedGroupMember> members = describedGroup.members();
                    final List<MemberDescription> memberDescriptions = new ArrayList<>(members.size());
                    final Set<AclOperation> authorizedOperations = validAclOperations(describedGroup.authorizedOperations());
                    for (DescribedGroupMember groupMember : members) {
                        Set<TopicPartition> partitions = Collections.emptySet();
                        if (groupMember.memberAssignment().length > 0) {
                            final Assignment assignment = ConsumerProtocol.
                                deserializeAssignment(ByteBuffer.wrap(groupMember.memberAssignment()));
                            partitions = new HashSet<>(assignment.partitions());
                        }
                        final MemberDescription memberDescription = new MemberDescription(
                                groupMember.memberId(),
                                Optional.ofNullable(groupMember.groupInstanceId()),
                                groupMember.clientId(),
                                groupMember.clientHost(),
                                new MemberAssignment(partitions));
                        memberDescriptions.add(memberDescription);
                    }
                    final ConsumerGroupDescription consumerGroupDescription =
                        new ConsumerGroupDescription(context.groupId(), protocolType.isEmpty(),
                            memberDescriptions,
                            describedGroup.protocolData(),
                            ConsumerGroupState.parse(describedGroup.groupState()),
                            context.node().get(),
                            authorizedOperations);
                    context.future().complete(consumerGroupDescription);
                } else {
                    context.future().completeExceptionally(new IllegalArgumentException(
                        String.format("GroupId %s is not a consumer group (%s).",
                            context.groupId(), protocolType)));
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                context.future().completeExceptionally(throwable);
            }
        };
    }

    /**
     * Returns a {@code Call} object to fetch the cluster metadata. Takes a List of Calls
     * parameter to schedule actions that need to be taken using the metadata. The param is a Supplier
     * so that it can be lazily created, so that it can use the results of the metadata call in its
     * construction.
     *
     * @param <T> The type of return value of the KafkaFuture, like ListOffsetsResultInfo, etc.
     * @param <O> The type of configuration option, like ListOffsetsOptions, etc
     */
    private <T, O extends AbstractOptions<O>> Call getMetadataCall(MetadataOperationContext<T, O> context,
                                                                   Supplier<List<Call>> nextCalls) {
        return new Call("metadata", context.deadline(), new LeastLoadedNodeProvider()) {
            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                return new MetadataRequest.Builder(new MetadataRequestData()
                    .setTopics(convertToMetadataRequestTopic(context.topics()))
                    .setAllowAutoTopicCreation(false));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                MetadataOperationContext.handleMetadataErrors(response);

                context.setResponse(Optional.of(response));

                for (Call call : nextCalls.get()) {
                    runnable.call(call, time.milliseconds());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                for (KafkaFutureImpl<T> future : context.futures().values()) {
                    future.completeExceptionally(throwable);
                }
            }
        };
    }

    private Set<AclOperation> validAclOperations(final int authorizedOperations) {
        if (authorizedOperations == MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED) {
            return null;
        }
        return Utils.from32BitField(authorizedOperations)
            .stream()
            .map(AclOperation::fromCode)
            .filter(operation -> operation != AclOperation.UNKNOWN
                && operation != AclOperation.ALL
                && operation != AclOperation.ANY)
            .collect(Collectors.toSet());
    }

    private boolean handleGroupRequestError(Errors error, KafkaFutureImpl<?> future) {
        if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.COORDINATOR_NOT_AVAILABLE) {
            throw error.exception();
        } else if (error != Errors.NONE) {
            future.completeExceptionally(error.exception());
            return true;
        }
        return false;
    }

    private final static class ListConsumerGroupsResults {
        private final List<Throwable> errors;
        private final HashMap<String, ConsumerGroupListing> listings;
        private final HashSet<Node> remaining;
        private final KafkaFutureImpl<Collection<Object>> future;

        ListConsumerGroupsResults(Collection<Node> leaders,
                                  KafkaFutureImpl<Collection<Object>> future) {
            this.errors = new ArrayList<>();
            this.listings = new HashMap<>();
            this.remaining = new HashSet<>(leaders);
            this.future = future;
            tryComplete();
        }

        synchronized void addError(Throwable throwable, Node node) {
            ApiError error = ApiError.fromThrowable(throwable);
            if (error.message() == null || error.message().isEmpty()) {
                errors.add(error.error().exception("Error listing groups on " + node));
            } else {
                errors.add(error.error().exception("Error listing groups on " + node + ": " + error.message()));
            }
        }

        synchronized void addListing(ConsumerGroupListing listing) {
            listings.put(listing.groupId(), listing);
        }

        synchronized void tryComplete(Node leader) {
            remaining.remove(leader);
            tryComplete();
        }

        private synchronized void tryComplete() {
            if (remaining.isEmpty()) {
                ArrayList<Object> results = new ArrayList<>(listings.values());
                results.addAll(errors);
                future.complete(results);
            }
        }
    }

    @Override
    public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
        final KafkaFutureImpl<Collection<Object>> all = new KafkaFutureImpl<>();
        final long nowMetadata = time.milliseconds();
        final long deadline = calcDeadlineMs(nowMetadata, options.timeoutMs());
        runnable.call(new Call("findAllBrokers", deadline, new LeastLoadedNodeProvider()) {
            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                return new MetadataRequest.Builder(new MetadataRequestData()
                    .setTopics(Collections.emptyList())
                    .setAllowAutoTopicCreation(true));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse metadataResponse = (MetadataResponse) abstractResponse;
                Collection<Node> nodes = metadataResponse.brokers();
                if (nodes.isEmpty())
                    throw new StaleMetadataException("Metadata fetch failed due to missing broker list");

                HashSet<Node> allNodes = new HashSet<>(nodes);
                final ListConsumerGroupsResults results = new ListConsumerGroupsResults(allNodes, all);

                for (final Node node : allNodes) {
                    final long nowList = time.milliseconds();
                    runnable.call(new Call("listConsumerGroups", deadline, new ConstantNodeIdProvider(node.id())) {
                        @Override
                        ListGroupsRequest.Builder createRequest(int timeoutMs) {
                            return new ListGroupsRequest.Builder(new ListGroupsRequestData());
                        }

                        private void maybeAddConsumerGroup(ListGroupsResponseData.ListedGroup group) {
                            String protocolType = group.protocolType();
                            if (protocolType.equals(ConsumerProtocol.PROTOCOL_TYPE) || protocolType.isEmpty()) {
                                final String groupId = group.groupId();
                                final ConsumerGroupListing groupListing = new ConsumerGroupListing(groupId, protocolType.isEmpty());
                                results.addListing(groupListing);
                            }
                        }

                        @Override
                        void handleResponse(AbstractResponse abstractResponse) {
                            final ListGroupsResponse response = (ListGroupsResponse) abstractResponse;
                            synchronized (results) {
                                Errors error = Errors.forCode(response.data().errorCode());
                                if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.COORDINATOR_NOT_AVAILABLE) {
                                    throw error.exception();
                                } else if (error != Errors.NONE) {
                                    results.addError(error.exception(), node);
                                } else {
                                    for (ListGroupsResponseData.ListedGroup group : response.data().groups()) {
                                        maybeAddConsumerGroup(group);
                                    }
                                }
                                results.tryComplete(node);
                            }
                        }

                        @Override
                        void handleFailure(Throwable throwable) {
                            synchronized (results) {
                                results.addError(throwable, node);
                                results.tryComplete(node);
                            }
                        }
                    }, nowList);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                KafkaException exception = new KafkaException("Failed to find brokers to send ListGroups", throwable);
                all.complete(Collections.singletonList(exception));
            }
        }, nowMetadata);

        return new ListConsumerGroupsResult(all);
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(final String groupId,
                                                                   final ListConsumerGroupOffsetsOptions options) {
        final KafkaFutureImpl<Map<TopicPartition, OffsetAndMetadata>> groupOffsetListingFuture = new KafkaFutureImpl<>();
        final long startFindCoordinatorMs = time.milliseconds();
        final long deadline = calcDeadlineMs(startFindCoordinatorMs, options.timeoutMs());

        ConsumerGroupOperationContext<Map<TopicPartition, OffsetAndMetadata>, ListConsumerGroupOffsetsOptions> context =
                new ConsumerGroupOperationContext<>(groupId, options, deadline, groupOffsetListingFuture);

        Call findCoordinatorCall = getFindCoordinatorCall(context,
            () -> getListConsumerGroupOffsetsCall(context));
        runnable.call(findCoordinatorCall, startFindCoordinatorMs);

        return new ListConsumerGroupOffsetsResult(groupOffsetListingFuture);
    }

    private Call getListConsumerGroupOffsetsCall(ConsumerGroupOperationContext<Map<TopicPartition, OffsetAndMetadata>,
            ListConsumerGroupOffsetsOptions> context) {
        return new Call("listConsumerGroupOffsets", context.deadline(),
                new ConstantNodeIdProvider(context.node().get().id())) {
            @Override
            OffsetFetchRequest.Builder createRequest(int timeoutMs) {
                // Set the flag to false as for admin client request,
                // we don't need to wait for any pending offset state to clear.
                return new OffsetFetchRequest.Builder(context.groupId(), false, context.options().topicPartitions(), false);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                final OffsetFetchResponse response = (OffsetFetchResponse) abstractResponse;
                final Map<TopicPartition, OffsetAndMetadata> groupOffsetsListing = new HashMap<>();

                // If coordinator changed since we fetched it, retry
                if (ConsumerGroupOperationContext.hasCoordinatorMoved(response)) {
                    Call call = getListConsumerGroupOffsetsCall(context);
                    rescheduleFindCoordinatorTask(context, () -> call, this);
                    return;
                }

                if (handleGroupRequestError(response.error(), context.future()))
                    return;

                for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry :
                    response.responseData().entrySet()) {
                    final TopicPartition topicPartition = entry.getKey();
                    OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                    final Errors error = partitionData.error;

                    if (error == Errors.NONE) {
                        final Long offset = partitionData.offset;
                        final String metadata = partitionData.metadata;
                        final Optional<Integer> leaderEpoch = partitionData.leaderEpoch;
                        // Negative offset indicates that the group has no committed offset for this partition
                        if (offset < 0) {
                            groupOffsetsListing.put(topicPartition, null);
                        } else {
                            groupOffsetsListing.put(topicPartition, new OffsetAndMetadata(offset, leaderEpoch, metadata));
                        }
                    } else {
                        log.warn("Skipping return offset for {} due to error {}.", topicPartition, error);
                    }
                }
                context.future().complete(groupOffsetsListing);
            }

            @Override
            void handleFailure(Throwable throwable) {
                context.future().completeExceptionally(throwable);
            }
        };
    }

    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options) {

        final Map<String, KafkaFutureImpl<Void>> futures = createFutures(groupIds);

        // TODO: KAFKA-6788, we should consider grouping the request per coordinator and send one request with a list of
        // all consumer groups this coordinator host
        for (final String groupId : groupIds) {
            // skip sending request for those futures that already failed.
            final KafkaFutureImpl<Void> future = futures.get(groupId);
            if (future.isCompletedExceptionally())
                continue;

            final long startFindCoordinatorMs = time.milliseconds();
            final long deadline = calcDeadlineMs(startFindCoordinatorMs, options.timeoutMs());
            ConsumerGroupOperationContext<Void, DeleteConsumerGroupsOptions> context =
                    new ConsumerGroupOperationContext<>(groupId, options, deadline, future);
            Call findCoordinatorCall = getFindCoordinatorCall(context,
                () -> getDeleteConsumerGroupsCall(context));
            runnable.call(findCoordinatorCall, startFindCoordinatorMs);
        }

        return new DeleteConsumerGroupsResult(new HashMap<>(futures));
    }

    private Call getDeleteConsumerGroupsCall(ConsumerGroupOperationContext<Void, DeleteConsumerGroupsOptions> context) {
        return new Call("deleteConsumerGroups", context.deadline(), new ConstantNodeIdProvider(context.node().get().id())) {

            @Override
            DeleteGroupsRequest.Builder createRequest(int timeoutMs) {
                return new DeleteGroupsRequest.Builder(
                    new DeleteGroupsRequestData()
                        .setGroupsNames(Collections.singletonList(context.groupId()))
                );
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                final DeleteGroupsResponse response = (DeleteGroupsResponse) abstractResponse;

                // If coordinator changed since we fetched it, retry
                if (ConsumerGroupOperationContext.hasCoordinatorMoved(response)) {
                    Call call = getDeleteConsumerGroupsCall(context);
                    rescheduleFindCoordinatorTask(context, () -> call, this);
                    return;
                }

                final Errors groupError = response.get(context.groupId());
                if (handleGroupRequestError(groupError, context.future()))
                    return;

                context.future().complete(null);
            }

            @Override
            void handleFailure(Throwable throwable) {
                context.future().completeExceptionally(throwable);
            }
        };
    }

    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(
            String groupId,
            Set<TopicPartition> partitions,
            DeleteConsumerGroupOffsetsOptions options) {
        final KafkaFutureImpl<Map<TopicPartition, Errors>> future = new KafkaFutureImpl<>();

        if (groupIdIsUnrepresentable(groupId)) {
            future.completeExceptionally(new InvalidGroupIdException("The given group id '" +
                groupId + "' cannot be represented in a request."));
            return new DeleteConsumerGroupOffsetsResult(future, partitions);
        }

        final long startFindCoordinatorMs = time.milliseconds();
        final long deadline = calcDeadlineMs(startFindCoordinatorMs, options.timeoutMs());
        ConsumerGroupOperationContext<Map<TopicPartition, Errors>, DeleteConsumerGroupOffsetsOptions> context =
            new ConsumerGroupOperationContext<>(groupId, options, deadline, future);

        Call findCoordinatorCall = getFindCoordinatorCall(context,
            () -> getDeleteConsumerGroupOffsetsCall(context, partitions));
        runnable.call(findCoordinatorCall, startFindCoordinatorMs);

        return new DeleteConsumerGroupOffsetsResult(future, partitions);
    }

    private Call getDeleteConsumerGroupOffsetsCall(
            ConsumerGroupOperationContext<Map<TopicPartition, Errors>, DeleteConsumerGroupOffsetsOptions> context,
            Set<TopicPartition> partitions) {
        return new Call("deleteConsumerGroupOffsets", context.deadline(), new ConstantNodeIdProvider(context.node().get().id())) {

            @Override
            OffsetDeleteRequest.Builder createRequest(int timeoutMs) {
                final OffsetDeleteRequestTopicCollection topics = new OffsetDeleteRequestTopicCollection();

                partitions.stream().collect(Collectors.groupingBy(TopicPartition::topic)).forEach((topic, topicPartitions) -> {
                    topics.add(
                        new OffsetDeleteRequestTopic()
                        .setName(topic)
                        .setPartitions(topicPartitions.stream()
                            .map(tp -> new OffsetDeleteRequestPartition().setPartitionIndex(tp.partition()))
                            .collect(Collectors.toList())
                        )
                    );
                });

                return new OffsetDeleteRequest.Builder(
                    new OffsetDeleteRequestData()
                        .setGroupId(context.groupId())
                        .setTopics(topics)
                );
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                final OffsetDeleteResponse response = (OffsetDeleteResponse) abstractResponse;

                // If coordinator changed since we fetched it, retry
                if (ConsumerGroupOperationContext.hasCoordinatorMoved(response)) {
                    Call call = getDeleteConsumerGroupOffsetsCall(context, partitions);
                    rescheduleFindCoordinatorTask(context, () -> call, this);
                    return;
                }

                // If the error is an error at the group level, the future is failed with it
                final Errors groupError = Errors.forCode(response.data.errorCode());
                if (handleGroupRequestError(groupError, context.future()))
                    return;

                final Map<TopicPartition, Errors> partitions = new HashMap<>();
                response.data.topics().forEach(topic -> topic.partitions().forEach(partition -> partitions.put(
                    new TopicPartition(topic.name(), partition.partitionIndex()),
                    Errors.forCode(partition.errorCode())))
                );

                context.future().complete(partitions);
            }

            @Override
            void handleFailure(Throwable throwable) {
                context.future().completeExceptionally(throwable);
            }
        };
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    @Override
    public ElectLeadersResult electLeaders(
            final ElectionType electionType,
            final Set<TopicPartition> topicPartitions,
            ElectLeadersOptions options) {
        final KafkaFutureImpl<Map<TopicPartition, Optional<Throwable>>> electionFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("electLeaders", calcDeadlineMs(now, options.timeoutMs()),
                new ControllerNodeProvider()) {

            @Override
            public ElectLeadersRequest.Builder createRequest(int timeoutMs) {
                return new ElectLeadersRequest.Builder(electionType, topicPartitions, timeoutMs);
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                ElectLeadersResponse response = (ElectLeadersResponse) abstractResponse;
                Map<TopicPartition, Optional<Throwable>> result = ElectLeadersResponse.electLeadersResult(response.data());

                // For version == 0 then errorCode would be 0 which maps to Errors.NONE
                Errors error = Errors.forCode(response.data().errorCode());
                if (error != Errors.NONE) {
                    electionFuture.completeExceptionally(error.exception());
                    return;
                }

                electionFuture.complete(result);
            }

            @Override
            void handleFailure(Throwable throwable) {
                electionFuture.completeExceptionally(throwable);
            }
        }, now);

        return new ElectLeadersResult(electionFuture);
    }

    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(
            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments,
            AlterPartitionReassignmentsOptions options) {
        final Map<TopicPartition, KafkaFutureImpl<Void>> futures = new HashMap<>();
        final Map<String, Map<Integer, Optional<NewPartitionReassignment>>> topicsToReassignments = new TreeMap<>();
        for (Map.Entry<TopicPartition, Optional<NewPartitionReassignment>> entry : reassignments.entrySet()) {
            String topic = entry.getKey().topic();
            int partition = entry.getKey().partition();
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            Optional<NewPartitionReassignment> reassignment = entry.getValue();
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
            futures.put(topicPartition, future);

            if (topicNameIsUnrepresentable(topic)) {
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                        topic + "' cannot be represented in a request."));
            } else if (topicPartition.partition() < 0) {
                future.completeExceptionally(new InvalidTopicException("The given partition index " +
                        topicPartition.partition() + " is not valid."));
            } else {
                Map<Integer, Optional<NewPartitionReassignment>> partitionReassignments =
                        topicsToReassignments.get(topicPartition.topic());
                if (partitionReassignments == null) {
                    partitionReassignments = new TreeMap<>();
                    topicsToReassignments.put(topic, partitionReassignments);
                }

                partitionReassignments.put(partition, reassignment);
            }
        }

        final long now = time.milliseconds();
        Call call = new Call("alterPartitionReassignments", calcDeadlineMs(now, options.timeoutMs()),
                new ControllerNodeProvider()) {

            @Override
            public AlterPartitionReassignmentsRequest.Builder createRequest(int timeoutMs) {
                AlterPartitionReassignmentsRequestData data =
                        new AlterPartitionReassignmentsRequestData();
                for (Map.Entry<String, Map<Integer, Optional<NewPartitionReassignment>>> entry :
                        topicsToReassignments.entrySet()) {
                    String topicName = entry.getKey();
                    Map<Integer, Optional<NewPartitionReassignment>> partitionsToReassignments = entry.getValue();

                    List<ReassignablePartition> reassignablePartitions = new ArrayList<>();
                    for (Map.Entry<Integer, Optional<NewPartitionReassignment>> partitionEntry :
                            partitionsToReassignments.entrySet()) {
                        int partitionIndex = partitionEntry.getKey();
                        Optional<NewPartitionReassignment> reassignment = partitionEntry.getValue();

                        ReassignablePartition reassignablePartition = new ReassignablePartition()
                                .setPartitionIndex(partitionIndex)
                                .setReplicas(reassignment.map(NewPartitionReassignment::targetReplicas).orElse(null))
                                .setObservers(reassignment.map(NewPartitionReassignment::targetObservers).orElse(Collections.emptyList()));
                        reassignablePartitions.add(reassignablePartition);
                    }

                    ReassignableTopic reassignableTopic = new ReassignableTopic()
                            .setName(topicName)
                            .setPartitions(reassignablePartitions);
                    data.topics().add(reassignableTopic);
                }
                data.setTimeoutMs(timeoutMs);
                return new AlterPartitionReassignmentsRequest.Builder(data);
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                AlterPartitionReassignmentsResponse response = (AlterPartitionReassignmentsResponse) abstractResponse;
                Map<TopicPartition, ApiException> errors = new HashMap<>();
                int receivedResponsesCount = 0;

                Errors topLevelError = Errors.forCode(response.data().errorCode());
                switch (topLevelError) {
                    case NONE:
                        receivedResponsesCount += validateTopicResponses(response.data().responses(), errors);
                        break;
                    case NOT_CONTROLLER:
                        handleNotControllerError(topLevelError);
                        break;
                    default:
                        for (ReassignableTopicResponse topicResponse : response.data().responses()) {
                            String topicName = topicResponse.name();
                            for (ReassignablePartitionResponse partition : topicResponse.partitions()) {
                                errors.put(
                                        new TopicPartition(topicName, partition.partitionIndex()),
                                        new ApiError(topLevelError, topLevelError.message()).exception()
                                );
                                receivedResponsesCount += 1;
                            }
                        }
                        break;
                }

                assertResponseCountMatch(errors, receivedResponsesCount);
                for (Map.Entry<TopicPartition, ApiException> entry : errors.entrySet()) {
                    ApiException exception = entry.getValue();
                    if (exception == null)
                        futures.get(entry.getKey()).complete(null);
                    else
                        futures.get(entry.getKey()).completeExceptionally(exception);
                }
            }

            private void assertResponseCountMatch(Map<TopicPartition, ApiException> errors, int receivedResponsesCount) {
                int expectedResponsesCount = topicsToReassignments.values().stream().mapToInt(Map::size).sum();
                if (errors.values().stream().noneMatch(Objects::nonNull) && receivedResponsesCount != expectedResponsesCount) {
                    String quantifier = receivedResponsesCount > expectedResponsesCount ? "many" : "less";
                    throw new UnknownServerException("The server returned too " + quantifier + " results." +
                        "Expected " + expectedResponsesCount + " but received " + receivedResponsesCount);
                }
            }

            private int validateTopicResponses(List<ReassignableTopicResponse> topicResponses,
                                               Map<TopicPartition, ApiException> errors) {
                int receivedResponsesCount = 0;

                for (ReassignableTopicResponse topicResponse : topicResponses) {
                    String topicName = topicResponse.name();
                    for (ReassignablePartitionResponse partResponse : topicResponse.partitions()) {
                        Errors partitionError = Errors.forCode(partResponse.errorCode());

                        TopicPartition tp = new TopicPartition(topicName, partResponse.partitionIndex());
                        if (partitionError == Errors.NONE) {
                            errors.put(tp, null);
                        } else {
                            errors.put(tp, new ApiError(partitionError, partResponse.errorMessage()).exception());
                        }
                        receivedResponsesCount += 1;
                    }
                }

                return receivedResponsesCount;
            }

            @Override
            void handleFailure(Throwable throwable) {
                for (KafkaFutureImpl<Void> future : futures.values()) {
                    future.completeExceptionally(throwable);
                }
            }
        };
        if (!topicsToReassignments.isEmpty()) {
            runnable.call(call, now);
        }
        return new AlterPartitionReassignmentsResult(new HashMap<>(futures));
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions,
                                                                       ListPartitionReassignmentsOptions options) {
        final KafkaFutureImpl<Map<TopicPartition, PartitionReassignment>> partitionReassignmentsFuture = new KafkaFutureImpl<>();
        if (partitions.isPresent()) {
            for (TopicPartition tp : partitions.get()) {
                String topic = tp.topic();
                int partition = tp.partition();
                if (topicNameIsUnrepresentable(topic)) {
                    partitionReassignmentsFuture.completeExceptionally(new InvalidTopicException("The given topic name '"
                            + topic + "' cannot be represented in a request."));
                } else if (partition < 0) {
                    partitionReassignmentsFuture.completeExceptionally(new InvalidTopicException("The given partition index " +
                            partition + " is not valid."));
                }
                if (partitionReassignmentsFuture.isCompletedExceptionally())
                    return new ListPartitionReassignmentsResult(partitionReassignmentsFuture);
            }
        }
        final long now = time.milliseconds();
        runnable.call(new Call("listPartitionReassignments", calcDeadlineMs(now, options.timeoutMs()),
            new ControllerNodeProvider()) {

            @Override
            ListPartitionReassignmentsRequest.Builder createRequest(int timeoutMs) {
                ListPartitionReassignmentsRequestData listData = new ListPartitionReassignmentsRequestData();
                listData.setTimeoutMs(timeoutMs);

                if (partitions.isPresent()) {
                    Map<String, ListPartitionReassignmentsTopics> reassignmentTopicByTopicName = new HashMap<>();

                    for (TopicPartition tp : partitions.get()) {
                        if (!reassignmentTopicByTopicName.containsKey(tp.topic()))
                            reassignmentTopicByTopicName.put(tp.topic(), new ListPartitionReassignmentsTopics().setName(tp.topic()));

                        reassignmentTopicByTopicName.get(tp.topic()).partitionIndexes().add(tp.partition());
                    }

                    listData.setTopics(new ArrayList<>(reassignmentTopicByTopicName.values()));
                }
                return new ListPartitionReassignmentsRequest.Builder(listData);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                ListPartitionReassignmentsResponse response = (ListPartitionReassignmentsResponse) abstractResponse;
                Errors error = Errors.forCode(response.data().errorCode());
                switch (error) {
                    case NONE:
                        break;
                    case NOT_CONTROLLER:
                        handleNotControllerError(error);
                        break;
                    default:
                        partitionReassignmentsFuture.completeExceptionally(new ApiError(error, response.data().errorMessage()).exception());
                        break;
                }
                Map<TopicPartition, PartitionReassignment> reassignmentMap = new HashMap<>();

                for (OngoingTopicReassignment topicReassignment : response.data().topics()) {
                    String topicName = topicReassignment.name();
                    for (OngoingPartitionReassignment partitionReassignment : topicReassignment.partitions()) {
                        reassignmentMap.put(
                            new TopicPartition(topicName, partitionReassignment.partitionIndex()),
                            new PartitionReassignment(
                                    partitionReassignment.replicas(),
                                    partitionReassignment.observers(),
                                    partitionReassignment.addingReplicas(),
                                    partitionReassignment.removingReplicas())
                        );
                    }
                }

                partitionReassignmentsFuture.complete(reassignmentMap);
            }

            @Override
            void handleFailure(Throwable throwable) {
                partitionReassignmentsFuture.completeExceptionally(throwable);
            }
        }, now);

        return new ListPartitionReassignmentsResult(partitionReassignmentsFuture);
    }

    @Confluent
    @Override
    public ReplicaStatusResult replicaStatus(Set<TopicPartition> partitions, ReplicaStatusOptions options) {
      final Map<TopicPartition, KafkaFutureImpl<List<ReplicaStatus>>> result = new HashMap<>();
      if (partitions.isEmpty()) {
        return new ReplicaStatusResult(Collections.unmodifiableMap(result));
      }

      final Set<String> topics = new HashSet<>();
      for (TopicPartition partition : partitions) {
        topics.add(partition.topic());
        result.put(partition, new KafkaFutureImpl<>());
      }

      // Retrieve the topic metadata for partition leader information.
      final long nowMetadata = time.milliseconds();
      final long deadline = calcDeadlineMs(nowMetadata, options.timeoutMs());
      runnable.call(new Call("topicsMetadata", deadline, new LeastLoadedNodeProvider()) {
        @Override
        AbstractRequest.Builder createRequest(int timeoutMs) {
          return new MetadataRequest.Builder(new MetadataRequestData()
              .setTopics(convertToMetadataRequestTopic(topics))
              .setAllowAutoTopicCreation(false));
        }

        @Override
        void handleResponse(AbstractResponse abstractResponse) {
          MetadataResponse response = (MetadataResponse) abstractResponse;

          Map<String, Errors> errors = response.errors();
          Cluster cluster = response.cluster();

          final Map<Node, Set<TopicPartition>> leaderPartitions = new HashMap<>();
          for (Map.Entry<TopicPartition, KafkaFutureImpl<List<ReplicaStatus>>> entry : result.entrySet()) {
            // Ignore requests for topics that failed with an error.
            Errors error = errors.get(entry.getKey().topic());
            if (error != null) {
              entry.getValue().completeExceptionally(error.exception());
              continue;
            }

            PartitionInfo partitionInfo = cluster.partition(entry.getKey());
            if (partitionInfo == null) {
              entry.getValue().completeExceptionally(
                  new UnknownTopicOrPartitionException("Partition " + entry.getKey() + " not found."));
              continue;
            }

            if (partitionInfo.leader() == null) {
              entry.getValue().completeExceptionally(
                  new LeaderNotAvailableException("Leader for partition " + entry.getKey() + " not found."));
              continue;
            }

            leaderPartitions.computeIfAbsent(partitionInfo.leader(), k -> new HashSet<>()).add(entry.getKey());
          }

          for (final Map.Entry<Node, Set<TopicPartition>> entry : leaderPartitions.entrySet()) {
            final long nowStatus = time.milliseconds();
            runnable.call(new Call("replicaStatus", deadline, new ConstantNodeIdProvider(entry.getKey().id())) {
              @Override
              AbstractRequest.Builder createRequest(int timeoutMs) {
                return new ReplicaStatusRequest.Builder(entry.getValue());
              }

              @Override
              void handleResponse(AbstractResponse abstractResponse) {
                ReplicaStatusResponse response = (ReplicaStatusResponse) abstractResponse;

                for (ReplicaStatusTopicResponse topicResponse : response.data().topics()) {
                  for (ReplicaStatusPartitionResponse partitionResponse : topicResponse.partitions()) {
                    TopicPartition topicPartition =
                        new TopicPartition(topicResponse.name(), partitionResponse.partitionIndex());
                    KafkaFutureImpl<List<ReplicaStatus>> future = result.get(topicPartition);
                    Objects.requireNonNull(future, "Replica status future must not be null for " + topicPartition);

                    Errors error = Errors.forCode(partitionResponse.errorCode());
                    if (error != Errors.NONE) {
                      future.completeExceptionally(error.exception());
                      continue;
                    }
                    List<ReplicaStatus> replicas = new ArrayList<ReplicaStatus>();
                    for (ReplicaStatusReplicaResponse replicaResponse : partitionResponse.replicas()) {
                      replicas.add(new ReplicaStatus(
                          replicaResponse.id(),
                          replicaResponse.isLeader(),
                          replicaResponse.isObserver(),
                          replicaResponse.isIsrEligible(),
                          replicaResponse.isInIsr(),
                          replicaResponse.isCaughtUp(),
                          replicaResponse.logStartOffset(),
                          replicaResponse.logEndOffset(),
                          replicaResponse.lastCaughtUpTimeMs(),
                          replicaResponse.lastFetchTimeMs()));
                    }
                    future.complete(replicas);
                  }
                }
              }

              @Override
              void handleFailure(Throwable throwable) {
                for (TopicPartition partition : entry.getValue()) {
                  result.get(partition).completeExceptionally(throwable);
                }
              }
            }, nowStatus);
          }
        }

        @Override
        void handleFailure(Throwable throwable) {
          completeAllExceptionally(result.values(), throwable);
        }
      }, nowMetadata);

      return new ReplicaStatusResult(Collections.unmodifiableMap(result));
    }

    private long calculateNextAllowedRetryMs() {
        return time.milliseconds() + retryBackoffMs;
    }

    private void handleNotControllerError(Errors error) throws ApiException {
        metadataManager.clearController();
        metadataManager.requestUpdate();
        throw error.exception();
    }

    /**
     * Returns a boolean indicating whether the resource needs to go to a specific node
     */
    private boolean dependsOnSpecificNode(ConfigResource resource) {
        return (resource.type() == ConfigResource.Type.BROKER && !resource.isDefault())
                || resource.type() == ConfigResource.Type.BROKER_LOGGER;
    }

    @Override
    public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId,
                                                                               RemoveMembersFromConsumerGroupOptions options) {
        final long startFindCoordinatorMs = time.milliseconds();
        final long deadline = calcDeadlineMs(startFindCoordinatorMs, options.timeoutMs());

        KafkaFutureImpl<Map<MemberIdentity, Errors>> future = new KafkaFutureImpl<>();

        ConsumerGroupOperationContext<Map<MemberIdentity, Errors>, RemoveMembersFromConsumerGroupOptions> context =
            new ConsumerGroupOperationContext<>(groupId, options, deadline, future);

        Call findCoordinatorCall = getFindCoordinatorCall(context,
            () -> getRemoveMembersFromGroupCall(context));
        runnable.call(findCoordinatorCall, startFindCoordinatorMs);

        return new RemoveMembersFromConsumerGroupResult(future, options.members());
    }

    private Call getRemoveMembersFromGroupCall(ConsumerGroupOperationContext<Map<MemberIdentity, Errors>, RemoveMembersFromConsumerGroupOptions> context) {
        return new Call("leaveGroup",
                        context.deadline(),
                        new ConstantNodeIdProvider(context.node().get().id())) {
            @Override
            LeaveGroupRequest.Builder createRequest(int timeoutMs) {
                return new LeaveGroupRequest.Builder(context.groupId(),
                                                     context.options().members().stream().map(
                                                         MemberToRemove::toMemberIdentity).collect(Collectors.toList()));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                final LeaveGroupResponse response = (LeaveGroupResponse) abstractResponse;

                // If coordinator changed since we fetched it, retry
                if (ConsumerGroupOperationContext.hasCoordinatorMoved(response)) {
                    Call call = getRemoveMembersFromGroupCall(context);
                    rescheduleFindCoordinatorTask(context, () -> call, this);
                    return;
                }

                if (handleGroupRequestError(response.topLevelError(), context.future()))
                    return;

                final Map<MemberIdentity, Errors> memberErrors = new HashMap<>();
                for (MemberResponse memberResponse : response.memberResponses()) {
                    // We set member.id to empty here explicitly, so that the lookup will succeed as user doesn't
                    // know the exact member.id.
                    memberErrors.put(new MemberIdentity()
                                         .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID)
                                         .setGroupInstanceId(memberResponse.groupInstanceId()),
                                     Errors.forCode(memberResponse.errorCode()));
                }
                context.future().complete(memberErrors);
            }

            @Override
            void handleFailure(Throwable throwable) {
                context.future().completeExceptionally(throwable);
            }
        };
    }

    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId,
                                                        Map<TopicPartition, OffsetAndMetadata> offsets,
                                                        AlterConsumerGroupOffsetsOptions options) {
        final KafkaFutureImpl<Map<TopicPartition, Errors>> future = new KafkaFutureImpl<>();

        final long startFindCoordinatorMs = time.milliseconds();
        final long deadline = calcDeadlineMs(startFindCoordinatorMs, options.timeoutMs());

        ConsumerGroupOperationContext<Map<TopicPartition, Errors>, AlterConsumerGroupOffsetsOptions> context =
                new ConsumerGroupOperationContext<>(groupId, options, deadline, future);

        Call findCoordinatorCall = getFindCoordinatorCall(context,
            () -> KafkaAdminClient.this.getAlterConsumerGroupOffsetsCall(context, offsets));
        runnable.call(findCoordinatorCall, startFindCoordinatorMs);

        return new AlterConsumerGroupOffsetsResult(future);
    }

    private Call getAlterConsumerGroupOffsetsCall(ConsumerGroupOperationContext<Map<TopicPartition, Errors>,
                                                  AlterConsumerGroupOffsetsOptions> context,
                                                  Map<TopicPartition, OffsetAndMetadata> offsets) {

        return new Call("commitOffsets", context.deadline(), new ConstantNodeIdProvider(context.node().get().id())) {

            @Override
            OffsetCommitRequest.Builder createRequest(int timeoutMs) {
                List<OffsetCommitRequestTopic> topics = new ArrayList<>();
                Map<String, List<OffsetCommitRequestPartition>> offsetData = new HashMap<>();
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                    String topic = entry.getKey().topic();
                    OffsetAndMetadata oam = entry.getValue();
                    offsetData.compute(topic, (key, value) -> {
                        if (value == null) {
                            value = new ArrayList<>();
                        }
                        OffsetCommitRequestPartition partition = new OffsetCommitRequestPartition()
                                .setCommittedOffset(oam.offset())
                                .setCommittedLeaderEpoch(oam.leaderEpoch().orElse(-1))
                                .setCommittedMetadata(oam.metadata())
                                .setPartitionIndex(entry.getKey().partition());
                        value.add(partition);
                        return value;
                    });
                }
                for (Map.Entry<String, List<OffsetCommitRequestPartition>> entry : offsetData.entrySet()) {
                    OffsetCommitRequestTopic topic = new OffsetCommitRequestTopic()
                            .setName(entry.getKey())
                            .setPartitions(entry.getValue());
                    topics.add(topic);
                }
                OffsetCommitRequestData data = new OffsetCommitRequestData()
                    .setGroupId(context.groupId())
                    .setTopics(topics);
                return new OffsetCommitRequest.Builder(data);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                final OffsetCommitResponse response = (OffsetCommitResponse) abstractResponse;

                // If coordinator changed since we fetched it, retry
                if (ConsumerGroupOperationContext.hasCoordinatorMoved(response)) {
                    Call call = getAlterConsumerGroupOffsetsCall(context, offsets);
                    rescheduleFindCoordinatorTask(context, () -> call, this);
                    return;
                }

                // If there is a coordinator error, retry
                for (OffsetCommitResponseTopic topic : response.data().topics()) {
                    for (OffsetCommitResponsePartition partition : topic.partitions()) {
                        Errors error = Errors.forCode(partition.errorCode());
                        if (ConsumerGroupOperationContext.shouldRefreshCoordinator(error)) {
                            Call call = getAlterConsumerGroupOffsetsCall(context, offsets);
                            rescheduleFindCoordinatorTask(context, () -> call, this);
                            return;
                        }
                    }
                }

                final Map<TopicPartition, Errors> partitions = new HashMap<>();
                for (OffsetCommitResponseTopic topic : response.data().topics()) {
                    for (OffsetCommitResponsePartition partition : topic.partitions()) {
                        TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                        Errors error = Errors.forCode(partition.errorCode());
                        partitions.put(tp, error);
                    }
                }
                context.future().complete(partitions);
            }

            @Override
            void handleFailure(Throwable throwable) {
                context.future().completeExceptionally(throwable);
            }
        };
    }

    @Override
    public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets,
                                         ListOffsetsOptions options) {

        // preparing topics list for asking metadata about them
        final Map<TopicPartition, KafkaFutureImpl<ListOffsetsResultInfo>> futures = new HashMap<>(topicPartitionOffsets.size());
        final Set<String> topics = new HashSet<>();
        for (TopicPartition topicPartition : topicPartitionOffsets.keySet()) {
            topics.add(topicPartition.topic());
            futures.put(topicPartition, new KafkaFutureImpl<>());
        }

        final long nowMetadata = time.milliseconds();
        final long deadline = calcDeadlineMs(nowMetadata, options.timeoutMs());

        MetadataOperationContext<ListOffsetsResultInfo, ListOffsetsOptions> context =
                new MetadataOperationContext<>(topics, options, deadline, futures);

        Call metadataCall = getMetadataCall(context,
            () -> KafkaAdminClient.this.getListOffsetsCalls(context, topicPartitionOffsets, futures));
        runnable.call(metadataCall, nowMetadata);

        return new ListOffsetsResult(new HashMap<>(futures));
    }

    private List<Call> getListOffsetsCalls(MetadataOperationContext<ListOffsetsResultInfo, ListOffsetsOptions> context,
                                           Map<TopicPartition, OffsetSpec> topicPartitionOffsets,
                                           Map<TopicPartition, KafkaFutureImpl<ListOffsetsResultInfo>> futures) {

        MetadataResponse mr = context.response().orElseThrow(() -> new IllegalStateException("No Metadata response"));
        List<Call> calls = new ArrayList<>();
        // grouping topic partitions per leader
        Map<Node, Map<TopicPartition, ListOffsetRequest.PartitionData>> leaders = new HashMap<>();

        for (Map.Entry<TopicPartition, OffsetSpec> entry: topicPartitionOffsets.entrySet()) {

            OffsetSpec offsetSpec = entry.getValue();
            TopicPartition tp = entry.getKey();
            KafkaFutureImpl<ListOffsetsResultInfo> future = futures.get(tp);
            long offsetQuery = (offsetSpec instanceof TimestampSpec)
                    ? ((TimestampSpec) offsetSpec).timestamp()
                    : (offsetSpec instanceof OffsetSpec.EarliestSpec)
                        ? ListOffsetRequest.EARLIEST_TIMESTAMP
                        : ListOffsetRequest.LATEST_TIMESTAMP;
            // avoid sending listOffsets request for topics with errors
            if (!mr.errors().containsKey(tp.topic())) {
                Node node = mr.cluster().leaderFor(tp);
                if (node != null) {
                    Map<TopicPartition, ListOffsetRequest.PartitionData> leadersOnNode = leaders.computeIfAbsent(node, k -> new HashMap<>());
                    leadersOnNode.put(tp, new ListOffsetRequest.PartitionData(offsetQuery, Optional.empty()));
                } else {
                    future.completeExceptionally(Errors.LEADER_NOT_AVAILABLE.exception());
                }
            } else {
                future.completeExceptionally(mr.errors().get(tp.topic()).exception());
            }
        }

        for (final Map.Entry<Node, Map<TopicPartition, ListOffsetRequest.PartitionData>> entry: leaders.entrySet()) {
            final int brokerId = entry.getKey().id();
            final Map<TopicPartition, ListOffsetRequest.PartitionData> partitionsToQuery = entry.getValue();

            calls.add(new Call("listOffsets on broker " + brokerId, context.deadline(), new ConstantNodeIdProvider(brokerId)) {

                @Override
                ListOffsetRequest.Builder createRequest(int timeoutMs) {
                    return ListOffsetRequest.Builder
                            .forConsumer(true, context.options().isolationLevel())
                            .setTargetTimes(partitionsToQuery);
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    ListOffsetResponse response = (ListOffsetResponse) abstractResponse;
                    Map<TopicPartition, OffsetSpec> retryTopicPartitionOffsets = new HashMap<>();

                    for (Entry<TopicPartition, PartitionData> result : response.responseData().entrySet()) {
                        TopicPartition tp = result.getKey();
                        PartitionData partitionData = result.getValue();

                        KafkaFutureImpl<ListOffsetsResultInfo> future = futures.get(tp);
                        Errors error = partitionData.error;
                        OffsetSpec offsetRequestSpec = topicPartitionOffsets.get(tp);
                        if (offsetRequestSpec == null) {
                            future.completeExceptionally(new KafkaException("Unexpected topic partition " + tp + " in broker response!"));
                        } else if (MetadataOperationContext.shouldRefreshMetadata(error)) {
                            retryTopicPartitionOffsets.put(tp, offsetRequestSpec);
                        } else if (error == Errors.NONE) {
                            future.complete(new ListOffsetsResultInfo(partitionData.offset, partitionData.timestamp, partitionData.leaderEpoch));
                        } else {
                            future.completeExceptionally(error.exception());
                        }
                    }

                    if (!retryTopicPartitionOffsets.isEmpty()) {
                        Set<String> retryTopics = retryTopicPartitionOffsets.keySet().stream().map(
                            TopicPartition::topic).collect(Collectors.toSet());
                        MetadataOperationContext<ListOffsetsResultInfo, ListOffsetsOptions> retryContext =
                            new MetadataOperationContext<>(retryTopics, context.options(), context.deadline(), futures);
                        rescheduleMetadataTask(retryContext, () -> getListOffsetsCalls(retryContext, retryTopicPartitionOffsets, futures));
                    }
                }

                @Override
                void handleFailure(Throwable throwable) {
                    for (TopicPartition tp : entry.getValue().keySet()) {
                        KafkaFutureImpl<ListOffsetsResultInfo> future = futures.get(tp);
                        future.completeExceptionally(throwable);
                    }
                }
            });
        }
        return calls;
    }

    @Override
    public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter, DescribeClientQuotasOptions options) {
        KafkaFutureImpl<Map<ClientQuotaEntity, Map<String, Double>>> future = new KafkaFutureImpl<>();

        final long now = time.milliseconds();
        runnable.call(new Call("describeClientQuotas", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {

                @Override
                DescribeClientQuotasRequest.Builder createRequest(int timeoutMs) {
                    return new DescribeClientQuotasRequest.Builder(filter);
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    DescribeClientQuotasResponse response = (DescribeClientQuotasResponse) abstractResponse;
                    response.complete(future);
                }

                @Override
                void handleFailure(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            }, now);

        return new DescribeClientQuotasResult(future);
    }

    @Override
    public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) {
        Map<ClientQuotaEntity, KafkaFutureImpl<Void>> futures = new HashMap<>(entries.size());
        for (ClientQuotaAlteration entry : entries) {
            futures.put(entry.entity(), new KafkaFutureImpl<>());
        }

        final long now = time.milliseconds();
        runnable.call(new Call("alterClientQuotas", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {

                @Override
                AlterClientQuotasRequest.Builder createRequest(int timeoutMs) {
                    return new AlterClientQuotasRequest.Builder(entries, options.validateOnly());
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    AlterClientQuotasResponse response = (AlterClientQuotasResponse) abstractResponse;
                    response.complete(futures);
                }

                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(futures.values(), throwable);
                }
            }, now);

        return new AlterClientQuotasResult(Collections.unmodifiableMap(futures));
    }

    private boolean brokerIdIsUnrepresentable(Integer brokerId) {
        return brokerId < 0;
    }

    @Confluent
    @Override
    public RemoveBrokersResult removeBrokers(List<Integer> brokersToRemove, RemoveBrokersOptions options) {
        final Map<Integer, KafkaFutureImpl<Void>> removeBrokerFutures = new HashMap<>(brokersToRemove.size());
        final Map<Integer, KafkaFutureImpl<Void>> invalidRemoveBrokerFutures = new HashMap<>();
        for (Integer brokerToRemove: brokersToRemove) {
            if (brokerToRemove == null) {
                continue;
            }
            if (brokerIdIsUnrepresentable(brokerToRemove)) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidRequestException("The given broker id '" +
                        brokerToRemove + "' is invalid and cannot be represented in a request"));
                invalidRemoveBrokerFutures.put(brokerToRemove, future);
            } else if (removeBrokerFutures.containsKey(brokerToRemove)) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidRequestException("The given broker id '" +
                        brokerToRemove + "' is duplicate and cannot be represented in a request"));
                invalidRemoveBrokerFutures.put(brokerToRemove, future);
            } else {
                removeBrokerFutures.put(brokerToRemove, new KafkaFutureImpl<>());
            }
        }

        if (!invalidRemoveBrokerFutures.isEmpty()) {
            for (Map.Entry<Integer, KafkaFutureImpl<Void>> removeBrokerFuture : removeBrokerFutures.entrySet()) {
                KafkaFutureImpl<Void> future = removeBrokerFuture.getValue();
                future.completeExceptionally(new InvalidRequestException("The request contains " +
                        "some invalid or duplicate broker ids"));
                invalidRemoveBrokerFutures.putIfAbsent(removeBrokerFuture.getKey(), future);
            }
            return new RemoveBrokersResult(new HashMap<>(invalidRemoveBrokerFutures));
        }

        final long now = time.milliseconds();
        Call call = new Call("removeBrokers", calcDeadlineMs(now, options.timeoutMs),
                new ControllerNodeProvider()) {

            @Override
            RemoveBrokersRequest.Builder createRequest(int timeoutMs) {
                Set<BrokerId> brokerIds = brokersToRemove.stream().map(brokerId -> new BrokerId().setBrokerId(brokerId)).
                        collect(Collectors.toSet());
                return new RemoveBrokersRequest.Builder(brokerIds);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                RemoveBrokersResponse response = (RemoveBrokersResponse) abstractResponse;
                // Check for top level error.
                Errors topLevelError = Errors.forCode(response.data().errorCode());
                switch (topLevelError) {
                    case NONE:
                        break;
                    case NOT_CONTROLLER:
                        handleNotControllerError(topLevelError);
                        break;
                    default:
                        completeAllExceptionally(removeBrokerFutures.values(), topLevelError.exception());
                        return;
                }

                // Handle server responses for brokers.
                for (RemoveBrokerResponse removeBrokerResponse : response.data().brokersToRemove()) {
                    KafkaFutureImpl<Void> future = removeBrokerFutures.get(removeBrokerResponse.brokerId());
                    if (future == null) {
                        log.warn("Server response mentioned unknown broker id {}", removeBrokerResponse.brokerId());
                    } else {
                        Errors error = Errors.forCode(removeBrokerResponse.errorCode());
                        if (error == Errors.NONE) {
                            future.complete(null);
                        } else {
                            future.completeExceptionally(error.exception());
                        }
                    }
                }

                // The server should send back a response for every brokerId. But do a sanity check anyway.
                for (Map.Entry<Integer, KafkaFutureImpl<Void>> entry : removeBrokerFutures.entrySet()) {
                    KafkaFutureImpl<Void> future = entry.getValue();
                    if (!future.isDone()) {
                        future.completeExceptionally(new ApiException("The server response did not contain a " +
                                "reference to broker " + entry.getKey()));
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(removeBrokerFutures.values(), throwable);
            }
        };

        runnable.call(call, now);

        return new RemoveBrokersResult(new HashMap<>(removeBrokerFutures));
    }

    @Confluent
    @Override
    public CreateClusterLinksResult createClusterLinks(Collection<NewClusterLink> clusterLinks, CreateClusterLinksOptions options) {
        Map<String, KafkaFutureImpl<Void>> results = new HashMap<>(clusterLinks.size());
        for (NewClusterLink clusterLink : clusterLinks) {
            results.put(clusterLink.linkName(), new KafkaFutureImpl<>());
        }

        final long now = time.milliseconds();
        runnable.call(new Call("createClusterLinks", calcDeadlineMs(now, options.timeoutMs()),
                               new ControllerNodeProvider()) {

                @Override
                CreateClusterLinksRequest.Builder createRequest(int timeoutMs) {
                    return new CreateClusterLinksRequest.Builder(clusterLinks, options.validateOnly(), options.validateLink(), timeoutMs);
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    CreateClusterLinksResponse response = (CreateClusterLinksResponse) abstractResponse;
                    if (response.errorCounts().getOrDefault(Errors.NOT_CONTROLLER, 0) > 0) {
                      handleNotControllerError(Errors.NOT_CONTROLLER);
                    }
                    response.complete(results);
                }

                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(results.values(), throwable);
                }
            }, now);

        return new CreateClusterLinksResult(Collections.unmodifiableMap(results));
    }

    @Confluent
    @Override
    public ListClusterLinksResult listClusterLinks(ListClusterLinksOptions options) {
        KafkaFutureImpl<Collection<ClusterLinkListing>> result = new KafkaFutureImpl<>();

        final long now = time.milliseconds();
        runnable.call(new Call("listClusterLinks", calcDeadlineMs(now, options.timeoutMs()),
                               new ControllerNodeProvider()) {

                @Override
                ListClusterLinksRequest.Builder createRequest(int timeoutMs) {
                    return new ListClusterLinksRequest.Builder();
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    ListClusterLinksResponse response = (ListClusterLinksResponse) abstractResponse;
                    if (response.errorCounts().getOrDefault(Errors.NOT_CONTROLLER, 0) > 0) {
                        handleNotControllerError(Errors.NOT_CONTROLLER);
                    }
                    response.complete(result);
                }

                @Override
                void handleFailure(Throwable throwable) {
                    result.completeExceptionally(throwable);
                }
            }, now);

        return new ListClusterLinksResult(result);
    }

    @Confluent
    @Override
    public DeleteClusterLinksResult deleteClusterLinks(Collection<String> linkNames, DeleteClusterLinksOptions options) {
        Map<String, KafkaFutureImpl<Void>> results = new HashMap<>(linkNames.size());
        for (String linkName : linkNames) {
            results.put(linkName, new KafkaFutureImpl<>());
        }

        final long now = time.milliseconds();
        runnable.call(new Call("deleteClusterLinks", calcDeadlineMs(now, options.timeoutMs()),
                new ControllerNodeProvider()) {

                @Override
                DeleteClusterLinksRequest.Builder createRequest(int timeoutMs) {
                    return new DeleteClusterLinksRequest.Builder(linkNames, options.validateOnly(), options.force());
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    DeleteClusterLinksResponse response = (DeleteClusterLinksResponse) abstractResponse;
                    if (response.errorCounts().getOrDefault(Errors.NOT_CONTROLLER, 0) > 0) {
                      handleNotControllerError(Errors.NOT_CONTROLLER);
                    }
                    response.complete(results);
                }

                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(results.values(), throwable);
                }
            }, now);

        return new DeleteClusterLinksResult(Collections.unmodifiableMap(results));
    }

    @Confluent
    @Override
    public AlterMirrorsResult alterMirrors(List<AlterMirrorsRequest.Op> ops, AlterMirrorsOptions options) {
        List<KafkaFutureImpl<AlterMirrorsResponse.Result>> results = new ArrayList<>(ops.size());
        for (AlterMirrorsRequest.Op op : ops) {
            results.add(new KafkaFutureImpl<>());
        }

        final long now = time.milliseconds();
        runnable.call(new Call("alterMirrors", calcDeadlineMs(now, options.timeoutMs()),
                new ControllerNodeProvider()) {

                @Override
                AlterMirrorsRequest.Builder createRequest(int timeoutMs) {
                    return new AlterMirrorsRequest.Builder(ops, options.validateOnly(), timeoutMs);
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    AlterMirrorsResponse response = (AlterMirrorsResponse) abstractResponse;
                    if (response.errorCounts().getOrDefault(Errors.NOT_CONTROLLER, 0) > 0) {
                      handleNotControllerError(Errors.NOT_CONTROLLER);
                    }
                    response.complete(results);
                }

                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(results, throwable);
                }
            }, now);

        return new AlterMirrorsResult(Collections.unmodifiableList(results));
    }

    /**
     * Get a sub level error when the request is in batch. If given key was not found,
     * return an {@link IllegalArgumentException}.
     */
    static <K> Throwable getSubLevelError(Map<K, Errors> subLevelErrors, K subKey, String keyNotFoundMsg) {
        if (!subLevelErrors.containsKey(subKey)) {
            return new IllegalArgumentException(keyNotFoundMsg);
        } else {
            return subLevelErrors.get(subKey).exception();
        }
    }
}
