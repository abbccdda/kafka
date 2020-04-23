// (Copyright) [2017 - 2020] Confluent, Inc.

package io.confluent.kafka.multitenant;

import io.confluent.kafka.multitenant.metrics.TenantMetrics;
import io.confluent.kafka.multitenant.metrics.TenantMetrics.MetricsRequestContext;
import io.confluent.kafka.multitenant.quota.TenantPartitionAssignor;
import io.confluent.kafka.multitenant.schema.MultiTenantApis;
import io.confluent.kafka.multitenant.schema.TenantContext;
import io.confluent.kafka.multitenant.schema.TransformableType;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignmentCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfigCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicConfigs;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResultCollection;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestInternals;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MultiTenantRequestContext extends RequestContext {
  private static final Logger log = LoggerFactory.getLogger(MultiTenantRequestContext.class);

  final TenantContext tenantContext;
  private static final int BASE_HEADER_SIZE;

  private final Metrics metrics;
  private final TenantMetrics tenantMetrics;
  private final MetricsRequestContext metricsRequestContext;
  private final TenantPartitionAssignor partitionAssignor;
  private final short defaultReplicationFactor;
  private final int defaultNumPartitions;
  private final Time time;
  private final long startNanos;
  private boolean isMetadataFetchForAllTopics;
  private boolean isJoinConsumerGroup = false;
  private PatternType describeAclsPatternType;
  private boolean requestParsingFailed = false;
  private ApiException tenantApiException;

  static {
    // Actual header size is this base size + length of client-id
    BASE_HEADER_SIZE = new RequestHeader(ApiKeys.PRODUCE, (short) 0, "", 0).toStruct().sizeOf();
  }

  public MultiTenantRequestContext(RequestHeader header,
                                   String connectionId,
                                   InetAddress clientAddress,
                                   KafkaPrincipal principal,
                                   ListenerName listenerName,
                                   SecurityProtocol securityProtocol,
                                   ClientInformation clientInformation,
                                   Time time,
                                   Metrics metrics,
                                   TenantMetrics tenantMetrics,
                                   TenantPartitionAssignor partitionAssignor,
                                   short defaultReplicationFactor,
                                   int defaultNumPartitions) {
    super(header, connectionId, clientAddress, principal, listenerName, securityProtocol, clientInformation);

    if (!(principal instanceof MultiTenantPrincipal)) {
      throw new IllegalArgumentException("Unexpected principal type " + principal);
    }

    this.tenantContext = new TenantContext((MultiTenantPrincipal) principal);
    this.metrics = metrics;
    this.tenantMetrics = tenantMetrics;
    this.metricsRequestContext = new MetricsRequestContext(
        (MultiTenantPrincipal) principal, header.clientId(), header.apiKey());
    this.partitionAssignor = partitionAssignor;
    this.defaultReplicationFactor = defaultReplicationFactor;
    this.defaultNumPartitions = defaultNumPartitions;
    this.time = time;
    this.startNanos = time.nanoseconds();
  }

  @Override
  public RequestAndSize parseRequest(ByteBuffer buffer) {
    final long requestTimestampMs = time.milliseconds();
    updateRequestMetrics(buffer, requestTimestampMs);
    if (isUnsupportedApiVersionsRequest()) {
      return super.parseRequest(buffer);
    }

    ApiKeys api = header.apiKey();
    short apiVersion = header.apiVersion();
    log.trace("Parsing request of type {} with version {}", api, apiVersion);
    if (!MultiTenantApis.isApiAllowed(header.apiKey())) {
      tenantApiException = Errors.CLUSTER_AUTHORIZATION_FAILED.exception();
    }

    try {
      TransformableType<TenantContext> schema = MultiTenantApis.requestSchema(api, apiVersion);
      Struct struct = (Struct) schema.read(buffer, tenantContext);
      AbstractRequest body = AbstractRequest.parseRequest(api, apiVersion, struct);
      try {
        if (body instanceof MetadataRequest) {
          isMetadataFetchForAllTopics = ((MetadataRequest) body).isAllTopics();
        } else if (body instanceof CreateAclsRequest) {
          body = transformCreateAclsRequest((CreateAclsRequest) body);
        } else if (body instanceof DescribeAclsRequest) {
          body = transformDescribeAclsRequest((DescribeAclsRequest) body);
        } else if (body instanceof DeleteAclsRequest) {
          body = transformDeleteAclsRequest((DeleteAclsRequest) body);
        } else if (body instanceof CreateTopicsRequest) {
          body = transformCreateTopicsRequest((CreateTopicsRequest) body, apiVersion);
        } else if (body instanceof CreatePartitionsRequest) {
          body = transformCreatePartitionsRequest((CreatePartitionsRequest) body);
        } else if (body instanceof ProduceRequest) {
          updatePartitionBytesInMetrics((ProduceRequest) body, requestTimestampMs);
        } else if (body instanceof AlterConfigsRequest) {
          body = transformAlterConfigsRequest((AlterConfigsRequest) body, apiVersion);
        } else if (body instanceof IncrementalAlterConfigsRequest) {
          body = transformIncrementalAlterConfigsRequest((IncrementalAlterConfigsRequest) body, apiVersion);
        } else if (body instanceof JoinGroupRequest) {
          body = transformJoinGroupRequest((JoinGroupRequest) body);
        } else if (body instanceof WriteTxnMarkersRequest) {
          body = transformWriteTxnMarkersRequest((WriteTxnMarkersRequest) body);
        }

      } catch (InvalidRequestException e) {
        // We couldn't transform the request. Save the tenant request exception and intercept later
        tenantApiException = e;
      }
      return new RequestAndSize(body, struct.sizeOf());
    } catch (ApiException e) {
      requestParsingFailed = true;
      throw e;
    } catch (Throwable ex) {
      requestParsingFailed = true;
      throw new InvalidRequestException("Error getting request for apiKey: " + api
          + ", apiVersion: " + header.apiVersion()
          + ", connectionId: " + connectionId
          + ", listenerName: " + listenerName
          + ", principal: " + principal, ex);
    }
  }

  @Override
  public boolean shouldIntercept() {
    return tenantApiException != null;
  }

  @Override
  public AbstractResponse intercept(AbstractRequest request, int throttleTimeMs) {
    return request.getErrorResponse(throttleTimeMs, tenantApiException);
  }

  @Override
  public Send buildResponse(AbstractResponse body) {
    final long responseTimestampMs = time.milliseconds();
    if (requestParsingFailed) {
      // Since we did not successfully parse the inbound request, the response should
      // not be transformed.
      return super.buildResponse(body);
    }

    if (isUnsupportedApiVersionsRequest()) {
      Send response = super.buildResponse(body);
      updateResponseMetrics(body, response, responseTimestampMs);
      return response;
    }

    ApiKeys api = header.apiKey();
    short apiVersion = header.apiVersion();
    ResponseHeader responseHeader = header.toResponseHeader();

    if (body instanceof FetchResponse) {
      // Fetch responses are unique in that they skip the usual path through the Struct object in
      // order to enable zero-copy transfer. We obviously don't want to lose this, so we do an
      // in-place transformation of the returned topic partitions.
      @SuppressWarnings("unchecked")
      Send response = transformFetchResponse((FetchResponse<MemoryRecords>) body, apiVersion, responseHeader);
      updateResponseMetrics(body, response, responseTimestampMs);
      updatePartitionBytesOutMetrics((FetchResponse) body, responseTimestampMs);
      return response;
    } else {
      // Since the Metadata and ListGroups APIs allow users to fetch metadata for all topics or
      // groups in the cluster, we have to filter out the metadata from other tenants.
      @SuppressWarnings("unchecked")
      AbstractResponse filteredResponse = body;
      if (body instanceof MetadataResponse && isMetadataFetchForAllTopics) {
        filteredResponse = filteredMetadataResponse((MetadataResponse) body);
      } else if (body instanceof ListGroupsResponse) {
        filteredResponse = filteredListGroupsResponse((ListGroupsResponse) body);
      } else if (body instanceof DescribeConfigsResponse
              && !tenantContext.principal.tenantMetadata().allowDescribeBrokerConfigs) {
        filteredResponse = filteredDescribeConfigsResponse((DescribeConfigsResponse) body);
      } else if (body instanceof DescribeAclsResponse) {
        filteredResponse = filteredDescribeAclsResponse((DescribeAclsResponse) body);
      } else if (body instanceof DeleteAclsResponse) {
        filteredResponse = transformDeleteAclsResponse((DeleteAclsResponse) body);
      } else if (body instanceof CreateTopicsResponse) {
        filteredResponse = filteredCreateTopicsResponse((CreateTopicsResponse) body);
      } else if (body instanceof JoinGroupResponse) {
        filteredResponse = transformJoinGroupResponse((JoinGroupResponse) body);
      } else if (body instanceof DescribeGroupsResponse) {
        filteredResponse = transformDescribeGroupsResponse((DescribeGroupsResponse) body);
      } else if (body instanceof WriteTxnMarkersResponse) {
        filteredResponse = transformWriteTxnMarkersResponse((WriteTxnMarkersResponse) body);
      }

      TransformableType<TenantContext> schema = MultiTenantApis.responseSchema(api, apiVersion);
      Struct responseHeaderStruct = responseHeader.toStruct();
      Struct responseBodyStruct = RequestInternals.toStruct(filteredResponse, apiVersion);

      ByteBuffer buffer = ByteBuffer.allocate(responseHeaderStruct.sizeOf()
          + schema.sizeOf(responseBodyStruct, tenantContext));
      responseHeaderStruct.writeTo(buffer);
      schema.write(buffer, responseBodyStruct, tenantContext);
      buffer.flip();
      Send response = new NetworkSend(connectionId, buffer);
      updateResponseMetrics(body, response, responseTimestampMs);
      return response;
    }
  }

  private AbstractRequest transformCreateTopicsRequest(CreateTopicsRequest topicsRequest,
                                                       short version) {
    final CreatableTopicCollection topics = topicsRequest.data().topics();
    final CreatableTopicCollection updatedTopicSet = new CreatableTopicCollection();

    for (CreateTopicsRequestData.CreatableTopic topicDetails : topics) {
      removeFilteredConfigs(topicDetails);
      topicDetails.setName(tenantContext.addTenantPrefix(topicDetails.name()));
      updatedTopicSet.add(new CreatableTopic().setConfigs(topicDetails.configs()).setAssignments(topicDetails.assignments())
              .setReplicationFactor(topicDetails.replicationFactor()).setNumPartitions(topicDetails.numPartitions())
              .setName(topicDetails.name()));
    }

    final boolean overrideAssignments = partitionAssignor != null;
    if (overrideAssignments) {
      final Map<String, List<List<Integer>>> assignments = newAssignments(updatedTopicSet);

      for (CreatableTopic topicDetails : updatedTopicSet) {
        List<List<Integer>> assignment = assignments.getOrDefault(topicDetails.name(),
                Collections.emptyList());
        final CreatableReplicaAssignmentCollection newAssignments = new CreatableReplicaAssignmentCollection();
        for (int i = 0; i < assignment.size(); i++) {
          newAssignments.add(new CreatableReplicaAssignment()
                  .setPartitionIndex(i)
                  .setBrokerIds(assignment.get(i)));
        }
        topicDetails.setAssignments(newAssignments);
        if (!newAssignments.isEmpty()) {
          topicDetails.setNumPartitions(CreateTopicsRequest.NO_NUM_PARTITIONS);
          topicDetails.setReplicationFactor(CreateTopicsRequest.NO_REPLICATION_FACTOR);
        }
      }
    }

    return new CreateTopicsRequest.Builder(
            new CreateTopicsRequestData()
                    .setTopics(updatedTopicSet)
                    .setTimeoutMs(topicsRequest.data().timeoutMs())
                    .setValidateOnly(topicsRequest.data().validateOnly()))
            .build(version);
  }

  private void removeFilteredConfigs(CreatableTopic topicDetails) {
    // validate configs
    CreateableTopicConfigCollection filteredConfigs = new CreateableTopicConfigCollection();
    for (CreateableTopicConfig config: topicDetails.configs()) {
      if (allowConfigInRequest(config.name())) {
        filteredConfigs.add(new CreateableTopicConfig().setValue(config.value()).setName(config.name()));
      }
    }
    topicDetails.setConfigs(filteredConfigs);
  }

  private Map<String, List<List<Integer>>> newAssignments(CreateTopicsRequestData.CreatableTopicCollection topics) {
    Map<String, TenantPartitionAssignor.TopicInfo> topicInfos = new HashMap<>();

    for (CreateTopicsRequestData.CreatableTopic topicDetails : topics) {
      int partitions = topicDetails.numPartitions() == CreateTopicsRequest.NO_NUM_PARTITIONS ?
          defaultNumPartitions : topicDetails.numPartitions();
      short replication = topicDetails.replicationFactor() == CreateTopicsRequest.NO_REPLICATION_FACTOR ?
          defaultReplicationFactor : topicDetails.replicationFactor();
      if (!topicDetails.assignments().isEmpty()) {
        log.debug("Overriding replica assignments provided in CreateTopicsRequest");
        partitions = topicDetails.assignments().size();
        replication = (short) topicDetails.assignments().iterator().next().brokerIds().size();
      }
      if (partitions <= 0) {
        throw new InvalidRequestException("Invalid partition count " + partitions);
      }
      if (replication <= 0) {
        throw new InvalidRequestException("Invalid replication factor " + replication);
      }
      topicInfos.put(topicDetails.name(),
              new TenantPartitionAssignor.TopicInfo(partitions, replication, 0));
    }

    return partitionAssignor.assignPartitionsForNewTopics(
        tenantContext.principal.tenantMetadata().tenantName, topicInfos);
  }

  private AlterConfigsRequest transformAlterConfigsRequest(AlterConfigsRequest alterConfigsRequest,
                                                       short version) {
    Map<ConfigResource, AlterConfigsRequest.Config> configs = alterConfigsRequest.configs();
    Map<ConfigResource, AlterConfigsRequest.Config> transformedConfigs = new HashMap<>(0);

    for (Map.Entry<ConfigResource, AlterConfigsRequest.Config> resourceConfigEntry : configs.entrySet()) {
      // Only transform topic configs
      if (resourceConfigEntry.getKey().type() != ConfigResource.Type.TOPIC) {
        transformedConfigs.put(resourceConfigEntry.getKey(), resourceConfigEntry.getValue());
        continue;
      }

      List<AlterConfigsRequest.ConfigEntry> filteredConfigs = new ArrayList<>();
      for (AlterConfigsRequest.ConfigEntry configEntry : resourceConfigEntry.getValue().entries()) {
        if (allowConfigInRequest(configEntry.name())) {
          filteredConfigs.add(configEntry);
        }
      }

      transformedConfigs.put(resourceConfigEntry.getKey(), new AlterConfigsRequest.Config(filteredConfigs));
    }

    return new AlterConfigsRequest.Builder(transformedConfigs, alterConfigsRequest.validateOnly()).build(version);
  }

  private IncrementalAlterConfigsRequest transformIncrementalAlterConfigsRequest(
          IncrementalAlterConfigsRequest incrementalAlterConfigsRequestRequest,
          short version) {
    Map<ConfigResource, IncrementalAlterConfigsRequestData.AlterableConfigCollection> configs =
            incrementalAlterConfigsRequestRequest.data().resources().stream().collect(Collectors.toMap(
                    alterConfigsResource ->
                            new ConfigResource(
                                    ConfigResource.Type.forId(alterConfigsResource.resourceType()),
                                    alterConfigsResource.resourceName()),
                    IncrementalAlterConfigsRequestData.AlterConfigsResource::configs
    ));

    IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection transformedConfigs =
            new IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection();

    for (Map.Entry<ConfigResource, IncrementalAlterConfigsRequestData.AlterableConfigCollection> resourceConfigEntry : configs.entrySet()) {
      // Only transform topic configs
      if (resourceConfigEntry.getKey().type() != ConfigResource.Type.TOPIC) {
        transformedConfigs.add(new IncrementalAlterConfigsRequestData.AlterConfigsResource()
                .setResourceType(resourceConfigEntry.getKey().type().id())
                .setResourceName(resourceConfigEntry.getKey().name())
                .setConfigs(resourceConfigEntry.getValue()));
        continue;
      }

      IncrementalAlterConfigsRequestData.AlterableConfigCollection filteredConfigs =
              new IncrementalAlterConfigsRequestData.AlterableConfigCollection();
      for (IncrementalAlterConfigsRequestData.AlterableConfig configEntry : resourceConfigEntry.getValue().valuesSet()) {
        if (allowConfigInRequest(configEntry.name())) {
          filteredConfigs.add(new IncrementalAlterConfigsRequestData.AlterableConfig()
                  .setConfigOperation(configEntry.configOperation())
                  .setName(configEntry.name())
                  .setValue(configEntry.value()));
        }
      }

      transformedConfigs.add(new IncrementalAlterConfigsRequestData.AlterConfigsResource()
              .setResourceType(resourceConfigEntry.getKey().type().id())
              .setResourceName(resourceConfigEntry.getKey().name())
              .setConfigs(filteredConfigs));
    }

    return new IncrementalAlterConfigsRequest.Builder(
            new IncrementalAlterConfigsRequestData()
                    .setResources(transformedConfigs)
                    .setValidateOnly(incrementalAlterConfigsRequestRequest.data().validateOnly()))
            .build(version);
  }

  // To preserve compatibility with clients that perform config updates (for example, Replicator mirroring
  // topic configs from the source cluster), remove non-updateable configs prior to config policy validation.
  // For configs with a range of allowable values leave the configs in the request
  // and let them fail the config policy, rather than changing their values.
  private boolean allowConfigInRequest(String key) {
    if (MultiTenantConfigRestrictions.UPDATABLE_TOPIC_CONFIGS.contains(key) ||
            key.equals(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)) {
      log.trace("Allowing config {} in the request because it is updateable", key);
      return true;
    }

    log.info("Altering config property {} is disallowed, ignoring config.", key);
    return false;
  }

  private AbstractRequest transformCreatePartitionsRequest(CreatePartitionsRequest partitionsRequest) {
    for (CreatePartitionsTopic create: partitionsRequest.data().topics()) {
      List<CreatePartitionsRequestData.CreatePartitionsAssignment> assignment = create.assignments();
      if (assignment != null && !assignment.isEmpty()) {
        log.debug("Overriding replica assignments provided in CreatePartitionsRequest");
      }
      create.setName(tenantContext.addTenantPrefix(create.name()));
    }

    if (partitionAssignor != null) {
      String tenant = tenantContext.principal.tenantMetadata().tenantName;
      Map<String, Integer> partitionCounts = partitionsRequest
              .data()
              .topics()
              .stream()
              .collect(Collectors.toMap(CreatePartitionsTopic::name, CreatePartitionsTopic::count));

      Map<String, List<List<Integer>>> assignments = partitionAssignor.assignPartitionsForExistingTopics(tenant, partitionCounts);
      for (CreatePartitionsTopic create: partitionsRequest.data().topics()) {
          List<CreatePartitionsRequestData.CreatePartitionsAssignment> updated =
                           assignments
                          .get(create.name())
                          .stream()
                          .map(brokers -> new CreatePartitionsRequestData.CreatePartitionsAssignment().setBrokerIds(brokers))
                          .collect(Collectors.toList());
        create.setAssignments(updated);
      }
    }

    return partitionsRequest;
  }

  private JoinGroupRequest transformJoinGroupRequest(JoinGroupRequest joinGroupRequest) {
    if (joinGroupRequest.data().protocolType().equals(ConsumerProtocol.PROTOCOL_TYPE)) {
      isJoinConsumerGroup = true;

      JoinGroupRequestProtocolCollection protocols = joinGroupRequest.data().protocols();
      for (JoinGroupRequestProtocol protocol : protocols) {
        protocol.setMetadata(transformSubscription(protocol.metadata(), true));
      }
    }

    return joinGroupRequest;
  }

  private JoinGroupResponse transformJoinGroupResponse(JoinGroupResponse response) {
    if (isJoinConsumerGroup && response.isLeader()) {
      for (JoinGroupResponseMember member : response.data().members()) {
        member.setMetadata(transformSubscription(member.metadata(), false));
      }
    }

    return response;
  }

  private DescribeGroupsResponse transformDescribeGroupsResponse(DescribeGroupsResponse response) {
    for (DescribedGroup group : response.data().groups()) {
      if (group.protocolType().equals(ConsumerProtocol.PROTOCOL_TYPE)) {
        for (DescribedGroupMember member : group.members()) {
          member.setMemberMetadata(transformSubscription(member.memberMetadata(), false));
        }
      }
    }

    return response;
  }

  /**
   * Transforms a subscription to either add or remove the tenant prefix. All subscriptions
   * are parsed with the version 0 of the consumer protocol, which prefixes all the newer
   * versions, and trailing bytes are copied over.
   */
  private byte[] transformSubscription(byte[] metadata, boolean addPrefix) {
    // Don't do anything if there is not metadata
    if (metadata.length == 0) {
      return metadata;
    }

    // Parse the subscription
    ByteBuffer buffer = ByteBuffer.wrap(metadata);
    short ver = ConsumerProtocol.deserializeVersion(buffer);
    Struct struct = ConsumerProtocol.SUBSCRIPTION_V0.read(buffer);

    // Transform the topics
    Object[] topics = struct.getArray(ConsumerProtocol.TOPICS_KEY_NAME);
    int bytesDelta = 0;
    for (int i = 0; i < topics.length; i++) {
      String topic = (String) topics[i];
      if (addPrefix) {
        topics[i] = tenantContext.addTenantPrefix(topic);
        bytesDelta += tenantContext.prefixSizeInBytes();
      } else {
        // Active groups may not have the prefix yet.
        if (tenantContext.hasTenantPrefix(topic)) {
          topics[i] = tenantContext.removeTenantPrefix((String) topics[i]);
          bytesDelta -= tenantContext.prefixSizeInBytes();
        }
      }
    }
    struct.set(ConsumerProtocol.TOPICS_KEY_NAME, topics);

    // Serialize the subscription
    ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() + bytesDelta);
    ConsumerProtocol.CONSUMER_PROTOCOL_HEADER_SCHEMA.write(newBuffer,
        new Struct(ConsumerProtocol.CONSUMER_PROTOCOL_HEADER_SCHEMA).set(ConsumerProtocol.VERSION_KEY_NAME, ver));
    ConsumerProtocol.SUBSCRIPTION_V0.write(newBuffer, struct);
    // Copy trailing bytes
    newBuffer.put(buffer);
    newBuffer.flip();

    return newBuffer.array();
  }

  private Send transformFetchResponse(FetchResponse<MemoryRecords> fetchResponse, short version,
                                      ResponseHeader header) {
    LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitionData =
        fetchResponse.responseData();
    LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> transformedPartitionData =
        new LinkedHashMap<>(partitionData.size());
    for (Map.Entry<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> entry : partitionData.entrySet()) {
      TopicPartition partition = entry.getKey();
      transformedPartitionData.put(tenantContext.removeTenantPrefix(partition), entry.getValue());
    }
    FetchResponse<MemoryRecords> copy = new FetchResponse<>(fetchResponse.error(), transformedPartitionData,
        fetchResponse.throttleTimeMs(), fetchResponse.sessionId());
    return RequestInternals.toSend(copy, version, connectionId, header);
  }

  private MetadataResponse filteredMetadataResponse(MetadataResponse response) {
    response.data().topics().removeIf(topic -> !tenantContext.hasTenantPrefix(topic.name()));
    return response;
  }

  private ListGroupsResponse filteredListGroupsResponse(ListGroupsResponse response) {
    List<ListGroupsResponseData.ListedGroup> filteredGroups = new ArrayList<>();
    for (ListGroupsResponseData.ListedGroup group : response.data().groups()) {
      if (tenantContext.hasTenantPrefix(group.groupId())) {
        filteredGroups.add(group);
      }
    }
    ListGroupsResponseData data = new ListGroupsResponseData();
    data.setThrottleTimeMs(response.throttleTimeMs());
    data.setErrorCode(response.data().errorCode());
    data.setGroups(filteredGroups);
    return new ListGroupsResponse(data);
  }

  private DescribeConfigsResponse filteredDescribeConfigsResponse(
                                  DescribeConfigsResponse response) {
    Map<ConfigResource, DescribeConfigsResponse.Config> configs = response.configs();
    Map<ConfigResource, DescribeConfigsResponse.Config> filteredConfigs = new HashMap<>();
    for (Map.Entry<ConfigResource, DescribeConfigsResponse.Config> entry : configs.entrySet()) {
      ConfigResource resource = entry.getKey();
      DescribeConfigsResponse.Config config = entry.getValue();
      Set<DescribeConfigsResponse.ConfigEntry> filteredEntries = config.entries().stream()
          .filter(ce -> {
            if (resource.type() == ConfigResource.Type.BROKER) {
              return MultiTenantConfigRestrictions.VISIBLE_BROKER_CONFIGS.contains(ce.name());
            } else if (resource.type() == ConfigResource.Type.TOPIC) {
              return MultiTenantConfigRestrictions.visibleTopicConfig(ce.name());
            }

            return false;
          })
          // For topic configs that are not updatable, set readOnly to true
          .map(configEntry -> resource.type() == ConfigResource.Type.TOPIC &&
                  MultiTenantConfigRestrictions.UPDATABLE_TOPIC_CONFIGS.contains(configEntry.name()) ?
              configEntry :
              new DescribeConfigsResponse.ConfigEntry(configEntry.name(), configEntry.value(), configEntry.source(),
                  configEntry.isSensitive(), true, configEntry.synonyms())
          )
          .collect(Collectors.toSet());
      filteredConfigs.put(
          resource,
          new DescribeConfigsResponse.Config(config.error(), filteredEntries));
    }
    return new DescribeConfigsResponse(response.throttleTimeMs(), filteredConfigs);
  }

  private DescribeAclsResponse filteredDescribeAclsResponse(DescribeAclsResponse response) {
    String tenantPrefix = tenantContext.prefix();
    return new DescribeAclsResponse(
            new DescribeAclsResponseData()
                    .setThrottleTimeMs(response.throttleTimeMs())
                    .setErrorCode(response.error().error().code())
                    .setErrorMessage(response.error().message())
                    .setResources(
                            response.acls().stream()
                                    .filter(binding -> {
                                      ResourcePattern pattern = new ResourcePattern(
                                              ResourceType.fromCode(binding.resourceType()),
                                              binding.resourceName(),
                                              PatternType.fromCode(binding.patternType()));
                                      if (describeAclsPatternType == PatternType.LITERAL
                                              && pattern.patternType() != PatternType.LITERAL
                                              && !pattern.name().equals(tenantPrefix)) {
                                        return false;
                                      }
                                      if (describeAclsPatternType == PatternType.PREFIXED
                                              && pattern.patternType() == PatternType.PREFIXED
                                              && pattern.name().equals(tenantPrefix)) {
                                        return false;
                                      }
                                      return pattern.name().startsWith(tenantPrefix);
                                    })
                                    .map(binding -> {
                                              if (binding.resourceName().equals(tenantPrefix)) {
                                                return new DescribeAclsResponseData.DescribeAclsResource()
                                                        .setResourceType(binding.resourceType())
                                                        .setResourceName(tenantPrefix + "*")
                                                        .setPatternType(PatternType.LITERAL.code())
                                                        .setAcls(binding.acls());
                                              }
                                              return new DescribeAclsResponseData.DescribeAclsResource()
                                                      .setResourceType(binding.resourceType())
                                                      .setResourceName(binding.resourceName())
                                                      .setPatternType(binding.patternType())
                                                      .setAcls(binding.acls());
                                            }
                                    ).collect(Collectors.toList())));
  }

  private DeleteAclsResponse transformDeleteAclsResponse(DeleteAclsResponse response) {
    String tenantPrefix = tenantContext.prefix();
    response.filterResults().forEach(fr ->
      fr.matchingAcls().forEach(acl -> {
        if (acl.resourceName().equals(tenantPrefix)) {
          acl.setResourceName(tenantPrefix + "*");
          acl.setPatternType(PatternType.LITERAL.code());
        }
      })
    );
    return response;
  }

  private CreateTopicsResponse filteredCreateTopicsResponse(CreateTopicsResponse response) {
    CreateTopicsResponseData responseData = response.data();
    CreatableTopicResultCollection topics = new CreatableTopicResultCollection(responseData.topics().size());
    for (CreatableTopicResult result : responseData.topics()) {
      List<CreatableTopicConfigs> filteredConfigs = result.configs().stream()
          .filter(c -> MultiTenantConfigRestrictions.visibleTopicConfig(c.configName()))
          .collect(Collectors.toList());
      CreatableTopicResult topic = new CreatableTopicResult()
          .setName(result.name())
          .setErrorCode(result.errorCode())
          .setErrorMessage(result.errorMessage())
          .setTopicConfigErrorCode(result.topicConfigErrorCode())
          .setNumPartitions(result.numPartitions())
          .setReplicationFactor(result.replicationFactor())
          .setConfigs(filteredConfigs);
      topics.add(topic);
    }
    CreateTopicsResponseData data = new CreateTopicsResponseData()
        .setThrottleTimeMs(responseData.throttleTimeMs())
        .setTopics(topics);
    return new CreateTopicsResponse(data);
  }

  private boolean isUnsupportedApiVersionsRequest() {
    return header.apiKey() == ApiKeys.API_VERSIONS
        && !ApiKeys.API_VERSIONS.isVersionSupported(header.apiVersion());
  }

  private short minAclsRequestVersion(AbstractRequest request) {
    return request.version() >= 1 ? request.version() : 1;
  }

  /**
   * CreateAclsRequest transformations:
   *   Principal (done as schema transformation):
   *     User:userId -> TenantUser:clusterId_userId
   *     User:* -> TenantUser*:clusterId_ (MultiTenantAuthorizer handles this)
   *   Resource (prefixing done as schema transformation, others done here):
   *     LITERAL name -> LITERAL clusterId_name
   *     LITERAL * -> PREFIXED clusterId_
   *     PREFIXED prefix -> PREFIXED clusterId_prefix
   */
  private AbstractRequest transformCreateAclsRequest(CreateAclsRequest request) {
    String prefixedWildcard = tenantContext.prefixedWildcard();
    request.aclCreations().forEach(creation -> {
      ensureResourceNameNonEmpty(creation.resourceName());
      ensureSupportedResourceType(ResourceType.fromCode(creation.resourceType()));
      PatternType patternType = PatternType.fromCode(creation.resourcePatternType());
      ensureValidRequestPatternType(patternType);
      ensureValidPrincipal(creation.principal());
      if (patternType == PatternType.LITERAL && prefixedWildcard.equals(creation.resourceName())) {
        creation.setResourceName(tenantContext.prefix());
        creation.setResourcePatternType(PatternType.PREFIXED.code());
      }
    });
    return request;
  }

  private AbstractRequest transformDescribeAclsRequest(DescribeAclsRequest request) {
    this.describeAclsPatternType = request.filter().patternFilter().patternType();
    AclBindingFilter transformedFilter = transformAclFilter(request.filter());
    ensureValidPrincipal(request.filter().entryFilter().principal());
    return new DescribeAclsRequest.Builder(transformedFilter).build(minAclsRequestVersion(request));
  }

  private AbstractRequest transformDeleteAclsRequest(DeleteAclsRequest request) {
    List<AclBindingFilter> transformedFilters = request.filters().stream()
        .map(this::transformAclFilter)
        .collect(Collectors.toList());
    request.filters().forEach(filter -> ensureValidPrincipal(filter.entryFilter().principal()));
    return new DeleteAclsRequest.Builder(new DeleteAclsRequestData().setFilters(transformedFilters.stream()
            .map(DeleteAclsRequest::deleteAclsFilter).collect(Collectors.toList()))
    ).build(minAclsRequestVersion(request));
  }

  private WriteTxnMarkersRequest transformWriteTxnMarkersRequest(WriteTxnMarkersRequest request) {
    request.data.markers().forEach(marker ->
        marker.topics().forEach(topic -> topic.setName(tenantContext.addTenantPrefix(topic.name())))
    );
    return request;
  }

  private WriteTxnMarkersResponse transformWriteTxnMarkersResponse(WriteTxnMarkersResponse response) {
    response.data.markers().forEach(marker ->
      marker.topics().forEach(topic -> topic.setName(tenantContext.removeTenantPrefix(topic.name())))
    );
    return response;
  }

  private void ensureResourceNameNonEmpty(String name) {
    if (tenantContext.prefix().equals(name)) {
      throw new InvalidRequestException("Invalid empty resource name specified");
    }
  }

  private void ensureSupportedResourceType(ResourceType resourceType) {
    if (resourceType != ResourceType.TOPIC && resourceType != ResourceType.GROUP
        && resourceType != ResourceType.CLUSTER && resourceType != ResourceType.TRANSACTIONAL_ID
        && resourceType != ResourceType.ANY) {
      throw new InvalidRequestException("Unsupported resource type specified: " + resourceType);
    }
  }

  private void ensureValidRequestPatternType(PatternType patternType) {
    if (patternType.isTenantPrefixed()) {
      throw new InvalidRequestException("Unsupported pattern type specified: " + patternType);
    }
  }

  private void ensureValidPrincipal(String principal) {
    try {
      if (principal != null) { // null principals are supported in filters
        SecurityUtils.parseKafkaPrincipal(principal);
      }
    } catch (IllegalArgumentException e) {
      throw new InvalidRequestException(e.getMessage());
    }
  }

  /**
   * ACL filter transformations for DescribeAclsRequest and DeleteAclRequest:
   *   Principal (done as schema transformation):
   *     User:userId -> TenantUser:clusterId_userId
   *     * -> TenantUser*:clusterId_
   *     null -> not transformed (this is ok since resource names are prefixed)
   *   Resource filter (prefixing done as schema transformation, others done here):
   *     LITERAL name -> LITERAL clusterId_name
   *     LITERAL * -> PREFIXED clusterId_
   *     LITERAL null -> CONFLUENT_ALL_TENANT_LITERAL clusterId_
   *     PREFIXED prefix -> PREFIXED clusterId_prefix
   *     PREFIXED null -> CONFLUENT_ALL_TENANT_PREFIXED clusterId_
   *     ANY name -> ANY clusterId_name
   *     ANY * -> PREFIXED clusterId_
   *     ANY null -> CONFLUENT_ALL_TENANT_ANY clusterId_
   *     MATCH name -> CONFLUENT_ONLY_TENANT_MATCH clusterId_name
   *     MATCH null -> CONFLUENT_ALL_TENANT_ANY clusterId_
   */
  private AclBindingFilter transformAclFilter(AclBindingFilter aclFilter) {
    ResourcePatternFilter pattern = aclFilter.patternFilter();
    String resourceName = pattern.name();
    PatternType patternType = pattern.patternType();
    ensureValidRequestPatternType(patternType);
    ensureResourceNameNonEmpty(resourceName);
    ensureSupportedResourceType(pattern.resourceType());

    String prefixedWildcard = tenantContext.addTenantPrefix("*");
    if (prefixedWildcard.equals(resourceName) && patternType != PatternType.PREFIXED) {
      resourceName = tenantContext.prefix();
      patternType = PatternType.PREFIXED;
    }
    if (resourceName == null) {
      switch (patternType) {
        case LITERAL:
          patternType = PatternType.CONFLUENT_ALL_TENANT_LITERAL;
          break;
        case PREFIXED:
          patternType = PatternType.CONFLUENT_ALL_TENANT_PREFIXED;
          break;
        case ANY:
        case MATCH:
          patternType = PatternType.CONFLUENT_ALL_TENANT_ANY;
          break;
        default:
          break;
      }
      resourceName = tenantContext.prefix();
    } else if (patternType == PatternType.MATCH) {
      patternType = PatternType.CONFLUENT_ONLY_TENANT_MATCH;
    }
    ResourcePatternFilter transformedPattern = new ResourcePatternFilter(
          pattern.resourceType(),
          resourceName,
          patternType);
    return new AclBindingFilter(transformedPattern, aclFilter.entryFilter());
  }

  int calculateRequestSize(ByteBuffer buffer) {
    return 4  // size field before header
        + BASE_HEADER_SIZE    // header size excluding client-id string
        + Utils.utf8Length(header.clientId())
        + buffer.remaining(); // request body
  }

  private void updateRequestMetrics(ByteBuffer buffer, long currentTimeMs) {
    tenantMetrics.recordRequest(
        metrics,
        metricsRequestContext,
        calculateRequestSize(buffer),
        currentTimeMs
    );
  }

  private void updateResponseMetrics(AbstractResponse body, Send response, long currentTimeMs) {
    tenantMetrics.recordResponse(
        metrics,
        metricsRequestContext,
        response.size(), time.nanoseconds() - startNanos, body.errorCounts(), currentTimeMs
    );
  }

  private void updatePartitionBytesInMetrics(ProduceRequest request, long currentTimeMs) {
    request.partitionRecordsOrFail().forEach((tp, value) -> {
      int size = value.sizeInBytes();
      tenantMetrics.recordPartitionStatsIn(
          metrics,
          metricsRequestContext,
          tp, size, numRecords(value.batches()), currentTimeMs);
    });

  }

  private void updatePartitionBytesOutMetrics(FetchResponse<?> response, long currentTimeMs) {
    response.responseData().forEach((tp, value) -> {
      int size = value.records.sizeInBytes();
      tenantMetrics.recordPartitionStatsOut(
          metrics,
          metricsRequestContext,
          tp, size, numRecords(value.records), currentTimeMs);
    });
  }

  private static int numRecords(BaseRecords records) {
    if (records instanceof Records) {
      return numRecords(((Records) records).batches());
    } else {
      return 0;
    }
  }

  private static int numRecords(Iterable<? extends RecordBatch> batches) {
    return StreamSupport
        .stream(batches.spliterator(), false)
        .mapToInt(b -> {
          // only count records for formats that support it efficiently
          Integer count = b.countOrNull();
          return count != null ? count : 0;
        })
        .sum();
  }
}
