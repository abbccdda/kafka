// (Copyright) [2017 - 2017] Confluent, Inc.
package io.confluent.kafka.multitenant;

import io.confluent.kafka.multitenant.metrics.ApiSensorBuilder;
import io.confluent.kafka.multitenant.metrics.PartitionSensors;
import io.confluent.kafka.multitenant.metrics.TenantMetrics;
import io.confluent.kafka.multitenant.quota.TenantPartitionAssignor;
import io.confluent.kafka.multitenant.quota.TestCluster;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfluentTopicConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.message.ControlledShutdownRequestData;
import org.apache.kafka.common.message.ControlledShutdownResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignmentCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfigCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicConfigs;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResultCollection;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResultCollection;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestPartition;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopic;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterConfigsResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ByteBufferChannel;
import org.apache.kafka.common.requests.ControlledShutdownRequest;
import org.apache.kafka.common.requests.ControlledShutdownResponse;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreateAclsRequest.AclCreation;
import org.apache.kafka.common.requests.CreateAclsResponse;
import org.apache.kafka.common.requests.CreateAclsResponse.AclCreationResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DeleteAclsResponse.AclDeletionResult;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EpochEndOffset;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaderAndIsrRequest;
import org.apache.kafka.common.requests.LeaderAndIsrResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetDeleteRequest;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ProduceResponse.RecordError;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestInternals;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.StopReplicaRequest;
import org.apache.kafka.common.requests.StopReplicaResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.requests.UpdateMetadataResponse;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.clients.consumer.internals.ConsumerProtocol.PROTOCOL_TYPE;
import static org.apache.kafka.common.requests.DescribeGroupsResponse.AUTHORIZED_OPERATIONS_OMITTED;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MultiTenantRequestContextTest {

  private final static Locale LOCALE = Locale.ENGLISH;
  private MultiTenantPrincipal principal = new MultiTenantPrincipal("user",
      new TenantMetadata("tenant", "tenant_cluster_id"));
  private ListenerName listenerName = new ListenerName("listener");
  private SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
  private Time time = new MockTime();
  private Metrics metrics = new Metrics(new MetricConfig(), Collections.emptyList(), time, true);
  private TenantMetrics tenantMetrics = new TenantMetrics();
  private TenantPartitionAssignor partitionAssignor;
  private TestCluster testCluster;

  @Before
  public void setUp() {
    testCluster = new TestCluster();
    for (int i = 0; i < 3; i++) {
      testCluster.addNode(i, null);
    }
    partitionAssignor = new TenantPartitionAssignor();
    partitionAssignor.updateClusterMetadata(testCluster.cluster());
  }

  @After
  public void tearDown() {
    metrics.close();
  }

  @Test
  public void testProduceRequest() {
    MultiTenantRequestContext context = newRequestContext(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion());

    String transactionalId = "tr";
    Map<TopicPartition, MemoryRecords> records = new HashMap<>();
    records.put(new TopicPartition("foo", 0), MemoryRecords.withRecords(2, CompressionType.NONE,
        new SimpleRecord("foo".getBytes())));
    records.put(new TopicPartition("bar", 0), MemoryRecords.withRecords(1, CompressionType.NONE,
        new SimpleRecord("bar".getBytes())));

    ProduceRequest inbound = ProduceRequest.Builder.forMagic((byte) 2, (short) -1, 0, records, transactionalId).build();
    ProduceRequest intercepted = (ProduceRequest) parseRequest(context, inbound);

    Map<TopicPartition, MemoryRecords> requestRecords = intercepted.partitionRecordsOrFail();
    assertEquals(2, requestRecords.size());
    assertEquals(mkSet(new TopicPartition("tenant_foo", 0), new TopicPartition("tenant_bar", 0)),
        requestRecords.keySet());
    assertEquals("tenant_tr", intercepted.transactionalId());
    verifyRequestMetrics(ApiKeys.PRODUCE);
  }

  /**
   * Create three produce requests of different sizes and assert that request size metrics work as expected
   */
  @Test
  public void testRequestSizeMetrics() {
    MultiTenantRequestContext context = newRequestContext(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion());
    List<Integer> requestSizes = new ArrayList<>();
    Map<TopicPartition, Integer> partitionCounts = new HashMap<>();
    for (int recordCount : asList(1, 5, 10)) {
      Map<TopicPartition, MemoryRecords> partitionRecords = new HashMap<>();
      TopicPartition tp = new TopicPartition("foo", 0);
      partitionRecords.put(tp,
              MemoryRecords.withRecords(2, CompressionType.NONE, simpleRecords(recordCount).toArray(new SimpleRecord[recordCount])));
      ProduceRequest inbound = ProduceRequest.Builder.forMagic((byte) 2, (short) -1, 0, partitionRecords, null).build();
      parseRequest(context, inbound);
      requestSizes.add(context.calculateRequestSize(toByteBuffer(inbound)));
      int count = partitionCounts.getOrDefault(tp, 0) + recordCount;
      partitionCounts.put(tp, count);
    }
    double expectedAverage = requestSizes.stream().mapToInt(v -> v).average().orElseThrow(NoSuchElementException::new);
    double expectedMin = requestSizes.stream().mapToInt(v -> v).min().orElseThrow(NoSuchElementException::new);
    double expectedMax = requestSizes.stream().mapToInt(v -> v).max().orElseThrow(NoSuchElementException::new);
    int expectedTotal = requestSizes.stream().mapToInt(v -> v).sum();

    Map<String, KafkaMetric> metrics = verifyRequestMetrics(ApiKeys.PRODUCE);
    assertEquals(expectedMin, (double) metrics.get("request-byte-min").metricValue(), 0.1);
    assertEquals(expectedMax, (double) metrics.get("request-byte-max").metricValue(), 0.1);
    assertEquals(expectedAverage, (double) metrics.get("request-byte-avg").metricValue(), 0.1);
    assertEquals(expectedTotal, (int) ((double) metrics.get("request-byte-total").metricValue()));

    this.metrics.metrics().forEach((name, metric) -> {
      if (name.name().equals("partition-records-in-total")) {
        String topic = name.tags().get(PartitionSensors.TOPIC_TAG);
        int partition = Integer.parseInt(name.tags().get(PartitionSensors.PARTITION_TAG));
        TopicPartition tp = new TopicPartition(topic, partition);

        assertEquals((int) partitionCounts.get(context.tenantContext.removeTenantPrefix(tp)),
                     (int) ((double) metric.metricValue()));
      }
    });
  }

  private List<SimpleRecord> simpleRecords(int recordCount) {
    return Stream.generate(() -> new SimpleRecord("foo".getBytes())).limit(recordCount)
            .collect(toList());
  }

  @Test
  public void testProduceResponse() throws IOException {
    MultiTenantRequestContext context = newRequestContext(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion());
    Map<TopicPartition, ProduceResponse.PartitionResponse> partitionResponses = new HashMap<>();
    partitionResponses.put(new TopicPartition("tenant_foo", 0),
        new ProduceResponse.PartitionResponse(Errors.INVALID_RECORD));
    partitionResponses.put(new TopicPartition("tenant_bar", 0),
        new ProduceResponse.PartitionResponse(Errors.INVALID_RECORD, 5, 10, 1,
        Collections.singletonList(new RecordError(6, "Compacted topic cannot accept message without key in topic partition tenant_bar-0.")),
        "Errors found in topic tenant_bar: invalid record"));

    ProduceResponse outbound = new ProduceResponse(partitionResponses, 0);
    Struct struct = parseResponse(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion(), context.buildResponse(outbound));
    ProduceResponse intercepted = new ProduceResponse(struct);

    assertEquals(mkSet(new TopicPartition("foo", 0), new TopicPartition("bar", 0)),
        intercepted.responses().keySet());
    ProduceResponse.PartitionResponse partitionResponse = intercepted.responses().get(new TopicPartition("bar", 0));
    assertEquals("Errors found in topic bar: invalid record", partitionResponse.errorMessage);
    assertEquals("Compacted topic cannot accept message without key in topic partition bar-0.",
        partitionResponse.recordErrors.get(0).message);
    verifyResponseMetrics(ApiKeys.PRODUCE, Errors.INVALID_RECORD);
  }

  @Test
  public void testFetchRequest() {
    for (short ver = ApiKeys.FETCH.oldestVersion(); ver <= ApiKeys.FETCH.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.FETCH, ver);
      LinkedHashMap<TopicPartition, FetchRequest.PartitionData> partitions = new LinkedHashMap<>();
      partitions.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0L, -1, 1, Optional.empty()));
      partitions.put(new TopicPartition("bar", 0), new FetchRequest.PartitionData(0L, -1, 1, Optional.empty()));

      FetchRequest inbound = FetchRequest.Builder.forConsumer(0, 0, partitions).build(ver);
      FetchRequest intercepted = (FetchRequest) parseRequest(context, inbound);

      assertEquals(asList(new TopicPartition("tenant_foo", 0), new TopicPartition("tenant_bar", 0)),
          new ArrayList<>(intercepted.fetchData().keySet()));
      verifyRequestMetrics(ApiKeys.FETCH);
    }
  }

  @Test
  public void testFetchResponse() throws IOException {
    for (short ver = ApiKeys.FETCH.oldestVersion(); ver <= ApiKeys.FETCH.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.FETCH, ver);

      LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> responsePartitions =
          new LinkedHashMap<>();
      responsePartitions.put(new TopicPartition("tenant_foo", 0), new FetchResponse.PartitionData<>(
          Errors.NONE, 1330L, 1324L, 0L, Collections.<FetchResponse.AbortedTransaction>emptyList(), MemoryRecords.EMPTY));
      responsePartitions.put(new TopicPartition("tenant_bar", 0), new FetchResponse.PartitionData<>(
          Errors.NONE, 1330L, 1324L, 0L, Collections.<FetchResponse.AbortedTransaction>emptyList(), MemoryRecords.EMPTY));

      FetchResponse<MemoryRecords> outbound = new FetchResponse<>(Errors.INVALID_FETCH_SESSION_EPOCH,
          responsePartitions, 0, 1234);
      Struct struct = parseResponse(ApiKeys.FETCH, ver, context.buildResponse(outbound));
      FetchResponse<MemoryRecords> intercepted = FetchResponse.parse(struct);

      assertEquals(asList(new TopicPartition("foo", 0), new TopicPartition("bar", 0)),
          new ArrayList<>(intercepted.responseData().keySet()));
      if (ver >= 7) {
        assertEquals(1234, intercepted.sessionId());
        assertEquals(Errors.INVALID_FETCH_SESSION_EPOCH, intercepted.error());
      } else {
        assertEquals(0, intercepted.sessionId());
        assertEquals(Errors.NONE, intercepted.error());
      }
      verifyResponseMetrics(ApiKeys.FETCH, Errors.NONE);
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testListOffsetsRequest() {
    for (short ver = ApiKeys.LIST_OFFSETS.oldestVersion(); ver <= ApiKeys.LIST_OFFSETS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.LIST_OFFSETS, ver);
      ListOffsetRequest.Builder bldr = ListOffsetRequest.Builder.forConsumer(false, IsolationLevel.READ_UNCOMMITTED);
      Map<TopicPartition, ListOffsetRequest.PartitionData> offsetData = new HashMap<>();
      offsetData.put(new TopicPartition("foo", 0), new ListOffsetRequest.PartitionData(0L, 1));
      offsetData.put(new TopicPartition("bar", 0), new ListOffsetRequest.PartitionData(0L, 1));
      bldr.setTargetTimes(offsetData);

      ListOffsetRequest inbound = bldr.build(ver);
      ListOffsetRequest intercepted = (ListOffsetRequest) parseRequest(context, inbound);

      assertEquals(mkSet(new TopicPartition("tenant_foo", 0), new TopicPartition("tenant_bar", 0)),
          intercepted.partitionTimestamps().keySet());
      verifyRequestMetrics(ApiKeys.LIST_OFFSETS);
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testListOffsetsResponse() throws IOException {
    for (short ver = ApiKeys.LIST_OFFSETS.oldestVersion(); ver <= ApiKeys.LIST_OFFSETS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.LIST_OFFSETS, ver);

      Map<TopicPartition, ListOffsetResponse.PartitionData> responsePartitions = new HashMap<>();
      if (ver == 0) {
        responsePartitions.put(new TopicPartition("tenant_foo", 0), new ListOffsetResponse.PartitionData(Errors.NONE, asList(0L, 10L)));
        responsePartitions.put(new TopicPartition("tenant_bar", 0), new ListOffsetResponse.PartitionData(Errors.NONE, asList(0L, 10L)));
      } else {
        responsePartitions.put(new TopicPartition("tenant_foo", 0), new ListOffsetResponse.PartitionData(Errors.NONE, 0L, 0L, Optional.empty()));
        responsePartitions.put(new TopicPartition("tenant_bar", 0), new ListOffsetResponse.PartitionData(Errors.NONE, 0L, 0L, Optional.empty()));
      }

      ListOffsetResponse outbound = new ListOffsetResponse(0, responsePartitions);
      Struct struct = parseResponse(ApiKeys.LIST_OFFSETS, ver, context.buildResponse(outbound));
      ListOffsetResponse intercepted = new ListOffsetResponse(struct);

      assertEquals(mkSet(new TopicPartition("foo", 0), new TopicPartition("bar", 0)),
          intercepted.responseData().keySet());
      verifyResponseMetrics(ApiKeys.LIST_OFFSETS, Errors.NONE);
    }
  }

  @Test
  public void testMetadataRequest() {
    for (short ver = 0; ver <= ApiKeys.METADATA.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.METADATA, ver);
      MetadataRequestData data = new MetadataRequestData();
      data.setAllowAutoTopicCreation(true);
      data.setTopics(Stream.of("foo", "bar").map(t -> new MetadataRequestTopic().setName(t))
          .collect(toList()));
      MetadataRequest inbound = new MetadataRequest(data, ver);
      MetadataRequest intercepted = (MetadataRequest) parseRequest(context, inbound);
      assertEquals(asList("tenant_foo", "tenant_bar"), intercepted.topics());
      verifyRequestMetrics(ApiKeys.METADATA);
    }
  }

  @Test
  public void testMetadataResponseNoController() throws IOException {
    for (short ver = ApiKeys.METADATA.oldestVersion(); ver <= ApiKeys.METADATA.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.METADATA, ver);
      Node node = new Node(1, "localhost", 9092);

      MetadataRequestData data = new MetadataRequestData().
              setAllowAutoTopicCreation(true)
              .setTopics(ver == 0 ? Collections.emptyList() : null);
      MetadataRequest inbound = new MetadataRequest(data, ver);
      MetadataRequest interceptedInbound = (MetadataRequest) parseRequest(context, inbound);
      assertTrue(interceptedInbound.isAllTopics());

      MetadataResponse outbound = MetadataResponse.prepareResponse(0, singletonList(node),
              "231412341", MetadataResponse.NO_CONTROLLER_ID,
              new ArrayList<>());
      assertNull(outbound.controller());

      Struct struct = parseResponse(ApiKeys.METADATA, ver, context.buildResponse(outbound));
      MetadataResponse intercepted = new MetadataResponse(struct, ver);
      assertNull(intercepted.controller());
    }
  }

  @Test
  public void testMetadataResponse() throws IOException {
    for (short ver = ApiKeys.METADATA.oldestVersion(); ver <= ApiKeys.METADATA.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.METADATA, ver);

      Node node = new Node(1, "localhost", 9092);
      List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();
      topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "tenant_foo", false,
          singletonList(new MetadataResponse.PartitionMetadata(Errors.NONE, 0, node,
              Optional.empty(), singletonList(node), singletonList(node),
              Collections.<Node>emptyList()))));
      topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "tenant_bar", false,
          singletonList(new MetadataResponse.PartitionMetadata(Errors.NONE, 0, node,
              Optional.empty(), singletonList(node), singletonList(node),
              Collections.<Node>emptyList()))));

      MetadataResponse outbound = MetadataResponse.prepareResponse(0, singletonList(node),
              "231412341", 1, topicMetadata);
      Struct struct = parseResponse(ApiKeys.METADATA, ver, context.buildResponse(outbound));
      MetadataResponse intercepted = new MetadataResponse(struct, ver);
      if (ver < 2)
        assertNull(intercepted.clusterId());
      else
        assertEquals("tenant_cluster_id", intercepted.clusterId());

      Iterator<MetadataResponse.TopicMetadata> iterator = intercepted.topicMetadata().iterator();
      assertTrue(iterator.hasNext());
      assertEquals("foo", iterator.next().topic());
      assertTrue(iterator.hasNext());
      assertEquals("bar", iterator.next().topic());
      assertFalse(iterator.hasNext());
      verifyResponseMetrics(ApiKeys.METADATA, Errors.NONE);
    }
  }

  @Test
  public void testMetadataFetchAllTopics() throws IOException {
    for (short ver = 0; ver <= ApiKeys.METADATA.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.METADATA, ver);

      MetadataRequestData data = new MetadataRequestData().setAllowAutoTopicCreation(true)
          .setTopics(ver == 0 ? Collections.emptyList() : null);
      MetadataRequest inbound = new MetadataRequest(data, ver);
      MetadataRequest interceptedInbound = (MetadataRequest) parseRequest(context, inbound);
      assertTrue(interceptedInbound.isAllTopics());

      Node node = new Node(1, "localhost", 9092);
      List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();
      topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "tenant_foo", false,
          singletonList(new MetadataResponse.PartitionMetadata(Errors.NONE, 0, node,
              Optional.empty(), singletonList(node), singletonList(node),
              Collections.<Node>emptyList()))));
      topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "othertenant_foo", false,
          singletonList(new MetadataResponse.PartitionMetadata(Errors.NONE, 0, node,
              Optional.empty(), singletonList(node), singletonList(node),
              Collections.<Node>emptyList()))));
      topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "tenant_bar", false,
          singletonList(new MetadataResponse.PartitionMetadata(Errors.NONE, 0, node,
              Optional.empty(), singletonList(node), singletonList(node),
              Collections.<Node>emptyList()))));
      topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "othertenant_bar", false,
          singletonList(new MetadataResponse.PartitionMetadata(Errors.NONE, 0, node,
              Optional.empty(), singletonList(node), singletonList(node),
              Collections.<Node>emptyList()))));

      MetadataResponse outbound = MetadataResponse.prepareResponse(0, singletonList(node), "clusterId", 1, topicMetadata);
      Struct struct = parseResponse(ApiKeys.METADATA, ver, context.buildResponse(outbound));
      MetadataResponse interceptedOutbound = new MetadataResponse(struct, ver);

      Iterator<MetadataResponse.TopicMetadata> iterator = interceptedOutbound.topicMetadata().iterator();
      assertTrue(iterator.hasNext());
      assertEquals("foo", iterator.next().topic());
      assertTrue(iterator.hasNext());
      assertEquals("bar", iterator.next().topic());
      assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testOffsetCommitRequest() {
    for (short ver = ApiKeys.OFFSET_COMMIT.oldestVersion(); ver <= ApiKeys.OFFSET_COMMIT.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.OFFSET_COMMIT, ver);
      String groupId = "group";
      OffsetCommitRequest inbound = new OffsetCommitRequest.Builder(
              new OffsetCommitRequestData()
                      .setGroupId(groupId)
                      .setTopics(asList(
                              new OffsetCommitRequestData.OffsetCommitRequestTopic()
                                      .setName("foo")
                                      .setPartitions(singletonList(
                                              new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                                      .setPartitionIndex(0)
                                                      .setCommittedOffset(0L)
                                                      .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                                      .setCommittedMetadata(""))),
                      new OffsetCommitRequestData.OffsetCommitRequestTopic()
                                      .setName("bar")
                                      .setPartitions(singletonList(
                                              new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                                      .setPartitionIndex(0)
                                                      .setCommittedOffset(0L)
                                                      .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                                      .setCommittedMetadata(""))))))
              .build(ver);

      OffsetCommitRequest intercepted = (OffsetCommitRequest) parseRequest(context, inbound);
      assertEquals("tenant_group", intercepted.data().groupId());
      assertEquals(asList(new TopicPartition("tenant_foo", 0), new TopicPartition("tenant_bar", 0)),
          intercepted.data().topics().stream()
                  .flatMap(t -> t.partitions().stream().map(p -> new TopicPartition(t.name(), p.partitionIndex())))
                  .collect(Collectors.toList()));
      verifyRequestMetrics(ApiKeys.OFFSET_COMMIT);
    }
  }

  @Test
  public void testOffsetCommitResponse() throws IOException {
    for (short ver = ApiKeys.OFFSET_COMMIT.oldestVersion(); ver <= ApiKeys.OFFSET_COMMIT.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.OFFSET_COMMIT, ver);
      Map<TopicPartition, Errors> partitionErrors = new HashMap<>();
      partitionErrors.put(new TopicPartition("tenant_foo", 0), Errors.NONE);
      partitionErrors.put(new TopicPartition("tenant_bar", 0), Errors.NONE);
      OffsetCommitResponse outbound = new OffsetCommitResponse(0, partitionErrors);
      Struct struct = parseResponse(ApiKeys.OFFSET_COMMIT, ver, context.buildResponse(outbound));
      OffsetCommitResponse intercepted = new OffsetCommitResponse(struct, ver);
      assertEquals(new HashSet<>(asList(new TopicPartition("foo", 0),
              new TopicPartition("bar", 0))),
              intercepted.data().topics().stream()
                      .flatMap(t -> t.partitions().stream().map(p -> new TopicPartition(t.name(),
                              p.partitionIndex())))
                      .collect(Collectors.toSet()));
      verifyResponseMetrics(ApiKeys.OFFSET_COMMIT, Errors.NONE);
    }
  }

  @Test
  public void testOffsetFetchRequest() {
    for (short ver = ApiKeys.OFFSET_FETCH.oldestVersion(); ver <= ApiKeys.OFFSET_FETCH.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.OFFSET_FETCH, ver);
      String groupId = "group";
      OffsetFetchRequest inbound = new OffsetFetchRequest.Builder(groupId, asList(new TopicPartition("foo", 0),
          new TopicPartition("bar", 0))).build(ver);
      OffsetFetchRequest intercepted = (OffsetFetchRequest) parseRequest(context, inbound);
      assertEquals("tenant_group", intercepted.groupId());
      assertEquals(mkSet(new TopicPartition("tenant_foo", 0), new TopicPartition("tenant_bar", 0)),
          new HashSet<>(intercepted.partitions()));
      verifyRequestMetrics(ApiKeys.OFFSET_FETCH);
    }
  }

  @Test
  public void testOffsetFetchResponse() throws IOException {
    for (short ver = ApiKeys.OFFSET_FETCH.oldestVersion(); ver <= ApiKeys.OFFSET_FETCH.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.OFFSET_FETCH, ver);
      Map<TopicPartition, OffsetFetchResponse.PartitionData> responsePartitions = new HashMap<>();
      responsePartitions.put(new TopicPartition("tenant_foo", 0), new OffsetFetchResponse.PartitionData(0L, Optional.empty(), "", Errors.NONE));
      responsePartitions.put(new TopicPartition("tenant_bar", 0), new OffsetFetchResponse.PartitionData(0L, Optional.empty(), "", Errors.NONE));
      OffsetFetchResponse outbound = new OffsetFetchResponse(0, Errors.NONE, responsePartitions);
      Struct struct = parseResponse(ApiKeys.OFFSET_FETCH, ver, context.buildResponse(outbound));
      OffsetFetchResponse intercepted = new OffsetFetchResponse(struct, ver);
      assertEquals(mkSet(new TopicPartition("foo", 0), new TopicPartition("bar", 0)),
          intercepted.responseData().keySet());
      verifyResponseMetrics(ApiKeys.OFFSET_FETCH, Errors.NONE);
    }
  }

  @Test
  public void testFindGroupCoordinatorRequest() {
    for (short ver = ApiKeys.FIND_COORDINATOR.oldestVersion(); ver <= ApiKeys.FIND_COORDINATOR.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.FIND_COORDINATOR, ver);
      FindCoordinatorRequest inbound =
              new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
                      .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                      .setKey("group"))
                      .build(ver);
      FindCoordinatorRequest intercepted = (FindCoordinatorRequest) parseRequest(context, inbound);
      assertEquals("tenant_group", intercepted.data().key());
      verifyRequestMetrics(ApiKeys.FIND_COORDINATOR);
    }
  }

  @Test
  public void testFindTxnCoordinatorRequest() {
    for (short ver = 1; ver <= ApiKeys.FIND_COORDINATOR.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.FIND_COORDINATOR, ver);
      FindCoordinatorRequest inbound =
              new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
                      .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                      .setKey("tr"))
                      .build(ver);
      FindCoordinatorRequest intercepted = (FindCoordinatorRequest) parseRequest(context, inbound);
      assertEquals("tenant_tr", intercepted.data().key());
      verifyRequestMetrics(ApiKeys.FIND_COORDINATOR);
    }
  }

  @Test
  public void testJoinGroupRequest() {
    String group = "group";
    String protocolName = "protocol";
    Subscription subscription = new Subscription(
        Collections.singletonList("topic"),
        ByteBuffer.allocate(10),
        Collections.singletonList(new TopicPartition("topic", 0)));

    // Not a consumer group
    byte[] protocolMetadata = new byte[20];
    new Random().nextBytes(protocolMetadata);
    testJoinGroupRequest(group, "non-consumer", protocolName, protocolMetadata,
        metadata -> assertArrayEquals(protocolMetadata, metadata));

    // A consumer group (V0)
    byte[] protocolMetadataV0 = ConsumerProtocol.serializeSubscriptionV0(subscription).array();
    testJoinGroupRequest(group, PROTOCOL_TYPE, protocolName, protocolMetadataV0, metadata -> {
      Subscription interceptedSubscription = ConsumerProtocol.deserializeSubscription(
          ByteBuffer.wrap(metadata));
      assertArrayEquals(Collections.singletonList("tenant_topic").toArray(),
          interceptedSubscription.topics().toArray());
      assertEquals(subscription.userData(), interceptedSubscription.userData());
    });

    // A consumer group (V1)
    byte[] protocolMetadataV1 = ConsumerProtocol.serializeSubscriptionV1(subscription).array();
    testJoinGroupRequest(group, PROTOCOL_TYPE, protocolName, protocolMetadataV1, metadata -> {
      Subscription interceptedSubscription = ConsumerProtocol.deserializeSubscription(
          ByteBuffer.wrap(metadata));
      assertArrayEquals(Collections.singletonList("tenant_topic").toArray(),
          interceptedSubscription.topics().toArray());
      assertEquals(subscription.userData(), interceptedSubscription.userData());
      assertArrayEquals(subscription.ownedPartitions().toArray(),
          interceptedSubscription.ownedPartitions().toArray());
    });
  }

  private void testJoinGroupRequest(String group, String protocolType, String protocolName,
      byte[] protocolMetadata, Consumer<byte[]> verifySubscription) {
    for (short ver = ApiKeys.JOIN_GROUP.oldestVersion(); ver <= ApiKeys.JOIN_GROUP.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.JOIN_GROUP, ver);

      JoinGroupRequest inbound = buildJoinGroupRequest(group, protocolType, protocolName,
          protocolMetadata, ver);
      JoinGroupRequest intercepted = (JoinGroupRequest) parseRequest(context, inbound);

      assertEquals("tenant_group", intercepted.data().groupId());
      assertEquals(1, intercepted.data().protocols().size());

      verifySubscription.accept(intercepted.data().protocols().find("protocol").metadata());
      verifyRequestMetrics(ApiKeys.JOIN_GROUP);
    }
  }

  @Test
  public void testJoinGroupResponse() throws IOException {
    String group = "group";
    String protocolName = "protocol";
    Subscription subscription = new Subscription(
        Collections.singletonList("topic"),
        ByteBuffer.allocate(10),
        Collections.singletonList(new TopicPartition("topic", 0)));

    // Not a consumer group
    byte[] protocolMetadata = new byte[20];
    new Random().nextBytes(protocolMetadata);
    testJoinGroupResponse(group, "non-consumer", protocolName, protocolMetadata, true);
    testJoinGroupResponse(group, "non-consumer", protocolName, protocolMetadata, false);

    // A consumer group (V0)
    byte[] protocolMetadataV0 = ConsumerProtocol.serializeSubscriptionV0(subscription).array();
    testJoinGroupResponse(group, PROTOCOL_TYPE, protocolName, protocolMetadataV0, true);
    testJoinGroupResponse(group, PROTOCOL_TYPE, protocolName, protocolMetadataV0, false);

    // A consumer group (V1)
    byte[] protocolMetadataV1 = ConsumerProtocol.serializeSubscriptionV1(subscription).array();
    testJoinGroupResponse(group, PROTOCOL_TYPE, protocolName, protocolMetadataV1, true);
    testJoinGroupResponse(group, PROTOCOL_TYPE, protocolName, protocolMetadataV1, false);
  }

  private void testJoinGroupResponse(String group, String protocolType, String protocolName,
      byte[] protocolMetadata, boolean leader) throws IOException {

    for (short ver = ApiKeys.JOIN_GROUP.oldestVersion(); ver <= ApiKeys.JOIN_GROUP.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.JOIN_GROUP, ver);

      JoinGroupRequest inbound = buildJoinGroupRequest(group, protocolType,
          protocolName, protocolMetadata, ver);
      JoinGroupRequest inboundIntercepted = (JoinGroupRequest) parseRequest(context, inbound);

      // The test assumes that the request is processed correctly thus it reuses the intercepted
      // metadata.
      JoinGroupResponse outbound = buildJoinGroupResponse(leader, protocolName,
          inboundIntercepted.data().protocols().find(protocolName).metadata());
      Struct struct = parseResponse(ApiKeys.JOIN_GROUP, ver, context.buildResponse(outbound));
      JoinGroupResponse intercepted = new JoinGroupResponse(struct, ver);

      assertEquals(outbound.isLeader(), intercepted.isLeader());
      assertEquals(outbound.data().generationId(), intercepted.data().generationId());
      assertEquals(outbound.data().protocolName(), intercepted.data().protocolName());
      assertEquals(outbound.data().memberId(), intercepted.data().memberId());
      assertEquals(outbound.data().members().size(), intercepted.data().members().size());

      for (int i = 0; i < outbound.data().members().size(); i++) {
        JoinGroupResponseMember member = intercepted.data().members().get(i);
        assertEquals(i == 0 ? "leader" : "follower", member.memberId());
        assertArrayEquals(protocolMetadata, member.metadata());
      }
    }
  }

  private JoinGroupRequest buildJoinGroupRequest(String group, String protocolType,
      String protocolName, byte[] protocolMetadata, short version) {

    JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
        new JoinGroupRequestData.JoinGroupRequestProtocolCollection(Collections.singleton(
            new JoinGroupRequestData.JoinGroupRequestProtocol()
                .setName(protocolName)
                .setMetadata(protocolMetadata)
        ).iterator()
    );

    JoinGroupRequestData data = new JoinGroupRequestData()
        .setGroupId(group)
        .setSessionTimeoutMs(30000)
        .setMemberId("")
        .setProtocolType(protocolType)
        .setProtocols(protocols);

    return new JoinGroupRequest.Builder(data).build(version);
  }

  private JoinGroupResponse buildJoinGroupResponse(boolean leader, String protocolName, byte[] protocolMetadata) {
    JoinGroupResponseData data = new JoinGroupResponseData()
        .setLeader("leader")
        .setMemberId(leader ? "leader" : "follower")
        .setGenerationId(10)
        .setProtocolName(protocolName);

    if (leader) {
      List<JoinGroupResponseMember> members = Arrays.asList(
          new JoinGroupResponseMember()
              .setMemberId("leader")
              .setMetadata(protocolMetadata),
          new JoinGroupResponseMember()
              .setMemberId("follower")
              .setMetadata(protocolMetadata)
      );

      data.setMembers(members);
    }

    return new JoinGroupResponse(data);
  }

  @Test
  public void testSyncGroupRequest() {
    for (short ver = ApiKeys.SYNC_GROUP.oldestVersion(); ver <= ApiKeys.SYNC_GROUP.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.SYNC_GROUP, ver);
      SyncGroupRequest inbound = new SyncGroupRequest.Builder(new SyncGroupRequestData()
          .setGroupId("group").setGenerationId(1).setMemberId("memberId")).build(ver);
      SyncGroupRequest intercepted = (SyncGroupRequest) parseRequest(context, inbound);
      assertEquals("tenant_group", intercepted.data.groupId());
      verifyRequestMetrics(ApiKeys.SYNC_GROUP);
    }
  }

  @Test
  public void testHeartbeatRequest() {
    for (short ver = ApiKeys.HEARTBEAT.oldestVersion(); ver <= ApiKeys.HEARTBEAT.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.HEARTBEAT, ver);
      HeartbeatRequest inbound = new HeartbeatRequest.Builder(new HeartbeatRequestData()
          .setGroupId("group")
          .setGenerationId(1)
          .setMemberId("memberId")).build(ver);
      HeartbeatRequest intercepted = (HeartbeatRequest) parseRequest(context, inbound);
      assertEquals("tenant_group", intercepted.data.groupId());
      verifyRequestMetrics(ApiKeys.HEARTBEAT);
    }
  }

  @Test
  public void testLeaveGroupRequest() {
    for (short ver = ApiKeys.LEAVE_GROUP.oldestVersion(); ver <= ApiKeys.LEAVE_GROUP.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.LEAVE_GROUP, ver);
      LeaveGroupRequest inbound = new LeaveGroupRequest.Builder("group",
              singletonList(new LeaveGroupRequestData.MemberIdentity().setMemberId("memberId")))
              .build(ver);
      LeaveGroupRequest intercepted = (LeaveGroupRequest) parseRequest(context, inbound);
      assertEquals("tenant_group", intercepted.data().groupId());
      verifyRequestMetrics(ApiKeys.LEAVE_GROUP);
    }
  }

  @Test
  public void testDescribeGroupsRequest() {
    for (short ver = ApiKeys.DESCRIBE_GROUPS.oldestVersion(); ver <= ApiKeys.DESCRIBE_GROUPS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.DESCRIBE_GROUPS, ver);
      DescribeGroupsRequestData describeGroupsRequestData = new DescribeGroupsRequestData();
      describeGroupsRequestData.setGroups(asList("foo", "bar"));
      DescribeGroupsRequest inbound = new DescribeGroupsRequest.Builder(describeGroupsRequestData).build(ver);
      DescribeGroupsRequest intercepted = (DescribeGroupsRequest) parseRequest(context, inbound);
      assertEquals(asList("tenant_foo", "tenant_bar"), intercepted.data().groups());
      verifyRequestMetrics(ApiKeys.DESCRIBE_GROUPS);
    }
  }

  @Test
  public void testDescribeGroupsResponse() throws IOException {
    List<String> outboundGroups = Arrays.asList("tenant_foo", "tenant_bar");
    Subscription outboundSubscriptionWithPrefix = new Subscription(
        Collections.singletonList("tenant_topic"),
        ByteBuffer.allocate(10),
        Collections.singletonList(new TopicPartition("topic", 0)));
    Subscription outboundSubscriptionWithoutPrefix = new Subscription(
        Collections.singletonList("topic"),
        ByteBuffer.allocate(10),
        Collections.singletonList(new TopicPartition("topic", 0)));
    Subscription interceptedSubscription = new Subscription(
        Collections.singletonList("topic"),
        ByteBuffer.allocate(10),
        Collections.singletonList(new TopicPartition("topic", 0)));

    // Not a consumer group
    byte[] protocolMetadata = new byte[20];
    new Random().nextBytes(protocolMetadata);
    testDescribeGroupsResponse(outboundGroups, "non-consumer", protocolMetadata, protocolMetadata);

    // A consumer group (V0), prefixed subscriptions
    byte[] outboundProtocolMetadataV0 = ConsumerProtocol.serializeSubscriptionV0(outboundSubscriptionWithPrefix).array();
    byte[] interceptedProtocolMetadataV0 = ConsumerProtocol.serializeSubscriptionV0(interceptedSubscription).array();
    testDescribeGroupsResponse(outboundGroups, PROTOCOL_TYPE, outboundProtocolMetadataV0, interceptedProtocolMetadataV0);

    // A consumer group (V0), not prefixed subscriptions
    outboundProtocolMetadataV0 = ConsumerProtocol.serializeSubscriptionV0(outboundSubscriptionWithoutPrefix).array();
    interceptedProtocolMetadataV0 = ConsumerProtocol.serializeSubscriptionV0(interceptedSubscription).array();
    testDescribeGroupsResponse(outboundGroups, PROTOCOL_TYPE, outboundProtocolMetadataV0, interceptedProtocolMetadataV0);

    // A consumer group (V1), prefixed subscriptions
    byte[] outboundProtocolMetadataV1 = ConsumerProtocol.serializeSubscriptionV1(outboundSubscriptionWithPrefix).array();
    byte[] interceptedProtocolMetadataV1 = ConsumerProtocol.serializeSubscriptionV1(interceptedSubscription).array();
    testDescribeGroupsResponse(outboundGroups, PROTOCOL_TYPE, outboundProtocolMetadataV1, interceptedProtocolMetadataV1);

    // A consumer group (V1), not prefixed subscriptions
    outboundProtocolMetadataV1 = ConsumerProtocol.serializeSubscriptionV1(outboundSubscriptionWithoutPrefix).array();
    interceptedProtocolMetadataV1 = ConsumerProtocol.serializeSubscriptionV1(interceptedSubscription).array();
    testDescribeGroupsResponse(outboundGroups, PROTOCOL_TYPE, outboundProtocolMetadataV1, interceptedProtocolMetadataV1);

    // A consumer group, not stable thus no subscriptions
    byte[] emptyMetadata = new byte[0];
    testDescribeGroupsResponse(outboundGroups, PROTOCOL_TYPE, emptyMetadata, emptyMetadata);
  }

  private void testDescribeGroupsResponse(List<String> outboundGroups, String protocolType,
      byte[] outboundProtocolMetadata, byte[] interceptedProtocolMetadata) throws IOException {

    for (short ver = ApiKeys.DESCRIBE_GROUPS.oldestVersion(); ver <= ApiKeys.DESCRIBE_GROUPS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.DESCRIBE_GROUPS, ver);

      DescribeGroupsResponse outbound = buildDescribeGroupsResponse(outboundGroups, protocolType, "range",
        outboundProtocolMetadata);
      Struct struct = parseResponse(ApiKeys.DESCRIBE_GROUPS, ver, context.buildResponse(outbound));
      DescribeGroupsResponse intercepted = new DescribeGroupsResponse(struct, ver);

      for (int i = 0; i < intercepted.data().groups().size(); i++) {
        DescribedGroup interceptedGroup = intercepted.data().groups().get(i);
        DescribedGroup outboundGroup = outbound.data().groups().get(i);

        assertEquals(context.tenantContext.removeTenantPrefix(outboundGroup.groupId()),
            interceptedGroup.groupId());
        assertEquals(outboundGroup.groupState(), interceptedGroup.groupState());
        assertEquals(outboundGroup.protocolType(), interceptedGroup.protocolType());
        assertEquals(outboundGroup.protocolData(), interceptedGroup.protocolData());

        for (int j = 0; j < interceptedGroup.members().size(); j++) {
          DescribedGroupMember interceptedMember = interceptedGroup.members().get(j);
          DescribedGroupMember outboundMember = outboundGroup.members().get(j);

          assertEquals(outboundMember.memberId(), interceptedMember.memberId());
          assertArrayEquals(outboundMember.memberAssignment(), interceptedMember.memberAssignment());
          assertArrayEquals(interceptedProtocolMetadata, interceptedMember.memberMetadata());
        }
      }

      verifyResponseMetrics(ApiKeys.DESCRIBE_GROUPS, Errors.NONE);
    }
  }

  private DescribeGroupsResponse buildDescribeGroupsResponse(List<String> groups, String protocolType,
      String protocolName, byte[] protocolMetadata) {
    DescribeGroupsResponseData describeGroupsResponseData = new DescribeGroupsResponseData();

    for (String group : groups) {
      DescribedGroupMember member1 = DescribeGroupsResponse.groupMember("member1", null,
          "clientid", "clienthost", new byte[0], protocolMetadata);
      DescribedGroupMember member2 = DescribeGroupsResponse.groupMember("member2", null,
          "clientid", "clienthost", new byte[0], protocolMetadata);
      describeGroupsResponseData.groups().add(DescribeGroupsResponse.groupMetadata(group, Errors.NONE,
          "STABLE", protocolType, protocolName, Arrays.asList(member1, member2), AUTHORIZED_OPERATIONS_OMITTED));
    }

    return new DescribeGroupsResponse(describeGroupsResponseData);
  }

  @Test
  public void testListGroupsResponse() throws IOException {
    for (short ver = ApiKeys.LIST_GROUPS.oldestVersion(); ver <= ApiKeys.LIST_GROUPS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.LIST_GROUPS, ver);

      ListGroupsResponseData.ListedGroup fooTenant = new ListGroupsResponseData.ListedGroup();
      fooTenant.setGroupId("tenant_foo");
      fooTenant.setProtocolType("consumer");
      ListGroupsResponseData.ListedGroup otherFooTenant = new ListGroupsResponseData.ListedGroup();
      otherFooTenant.setGroupId("othertenant_foo");
      otherFooTenant.setProtocolType("consumer");
      ListGroupsResponseData.ListedGroup barTenant = new ListGroupsResponseData.ListedGroup();
      barTenant.setGroupId("tenant_bar");
      barTenant.setProtocolType("consumer");
      ListGroupsResponseData.ListedGroup bazTenant = new ListGroupsResponseData.ListedGroup();
      bazTenant.setGroupId("othertenant_baz");
      bazTenant.setProtocolType("consumer");

      ListGroupsResponseData data = new ListGroupsResponseData();
      data.setThrottleTimeMs(0);
      data.setErrorCode(Errors.NONE.code());
      data.setGroups(asList(fooTenant, otherFooTenant, barTenant, bazTenant));

      ListGroupsResponse outbound = new ListGroupsResponse(data);
      Struct struct = parseResponse(ApiKeys.LIST_GROUPS, ver, context.buildResponse(outbound));
      ListGroupsResponse intercepted = new ListGroupsResponse(new ListGroupsResponseData(struct, ver));
      assertEquals(2, intercepted.data().groups().size());
      assertEquals("foo", intercepted.data().groups().get(0).groupId());
      assertEquals("bar", intercepted.data().groups().get(1).groupId());
      verifyResponseMetrics(ApiKeys.LIST_GROUPS, Errors.NONE);
    }
  }

  @Test
  public void testDeleteGroupsRequest() {
    for (short ver = ApiKeys.DELETE_GROUPS.oldestVersion(); ver <= ApiKeys.DELETE_GROUPS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.DELETE_GROUPS, ver);
      DeleteGroupsRequestData requestData = new DeleteGroupsRequestData().setGroupsNames(asList("foo", "bar"));
      DeleteGroupsRequest inbound = new DeleteGroupsRequest.Builder(requestData).build(ver);
      DeleteGroupsRequest intercepted = (DeleteGroupsRequest) parseRequest(context, inbound);
      assertEquals(asList("tenant_foo", "tenant_bar"), intercepted.data.groupsNames());
      verifyRequestMetrics(ApiKeys.DELETE_GROUPS);
    }
  }

  @Test
  public void testDeleteGroupsResponse() throws IOException {
    for (short ver = ApiKeys.DELETE_GROUPS.oldestVersion(); ver <= ApiKeys.DELETE_GROUPS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.DELETE_GROUPS, ver);
      DeleteGroupsResponseData responseData = new DeleteGroupsResponseData()
              .setResults(new DeleteGroupsResponseData.DeletableGroupResultCollection(Arrays.asList(
                      new DeleteGroupsResponseData.DeletableGroupResult()
                              .setErrorCode(Errors.NONE.code())
                              .setGroupId("tenant_foo"),
                      new DeleteGroupsResponseData.DeletableGroupResult()
                              .setErrorCode(Errors.NONE.code())
                              .setGroupId("tenant_bar")
              ).iterator()));
      DeleteGroupsResponse outbound = new DeleteGroupsResponse(responseData);
      Struct struct = parseResponse(ApiKeys.DELETE_GROUPS, ver, context.buildResponse(outbound));
      DeleteGroupsResponse intercepted = new DeleteGroupsResponse(struct, ver);
      assertEquals(mkSet("foo", "bar"), intercepted.errors().keySet());
      verifyResponseMetrics(ApiKeys.DELETE_GROUPS, Errors.NONE);
    }
  }

  @Test
  public void testOffsetDeleteRequest() {
    String tenantPrefix = principal.tenantMetadata().tenantPrefix();

    for (short ver = ApiKeys.OFFSET_DELETE.oldestVersion(); ver <= ApiKeys.OFFSET_DELETE.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.OFFSET_DELETE, ver);

      OffsetDeleteRequestTopicCollection topics = new OffsetDeleteRequestTopicCollection();
      topics.add(new OffsetDeleteRequestTopic().setName("foo").setPartitions(Arrays.asList(
          new OffsetDeleteRequestPartition().setPartitionIndex(0),
          new OffsetDeleteRequestPartition().setPartitionIndex(1))));
      topics.add(new OffsetDeleteRequestTopic().setName("bar").setPartitions(Arrays.asList(
          new OffsetDeleteRequestPartition().setPartitionIndex(2),
          new OffsetDeleteRequestPartition().setPartitionIndex(3))));

      OffsetDeleteRequestData data = new OffsetDeleteRequestData()
          .setGroupId("group")
          .setTopics(topics);

      OffsetDeleteRequest inbound = new OffsetDeleteRequest.Builder(data).build(ver);
      OffsetDeleteRequest intercepted = (OffsetDeleteRequest) parseRequest(context, inbound);

      assertTrue(intercepted.data.groupId().startsWith(tenantPrefix));
      assertEquals(inbound.data.topics().size(), intercepted.data.topics().size());

      for (OffsetDeleteRequestTopic topic : intercepted.data.topics()) {
        assertTrue(topic.name().startsWith(tenantPrefix));
        assertArrayEquals(
            inbound.data.topics().find(topic.name().substring(tenantPrefix.length())).partitions().toArray(),
            topic.partitions().toArray());
      }

      verifyRequestMetrics(ApiKeys.OFFSET_DELETE);
    }
  }

  @Test
  public void testOffsetDeleteResponse() throws IOException {
    String tenantPrefix = principal.tenantMetadata().tenantPrefix();

    for (short ver = ApiKeys.OFFSET_DELETE.oldestVersion(); ver <= ApiKeys.OFFSET_DELETE.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.OFFSET_DELETE, ver);

      OffsetDeleteResponseTopicCollection topics = new OffsetDeleteResponseTopicCollection();
      topics.add(new OffsetDeleteResponseTopic()
          .setName("tenant_foo")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(
              Arrays.asList(
                  new OffsetDeleteResponsePartition().setPartitionIndex(0),
                  new OffsetDeleteResponsePartition().setPartitionIndex(1)
              ).iterator())));
      topics.add(new OffsetDeleteResponseTopic()
          .setName("tenant_bar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(
              Arrays.asList(
                  new OffsetDeleteResponsePartition().setPartitionIndex(2),
                  new OffsetDeleteResponsePartition().setPartitionIndex(3)
              ).iterator())));

      OffsetDeleteResponseData data = new OffsetDeleteResponseData()
          .setTopics(topics);

      OffsetDeleteResponse outbound = new OffsetDeleteResponse(data);
      Struct struct = parseResponse(ApiKeys.OFFSET_DELETE, ver, context.buildResponse(outbound));
      OffsetDeleteResponse intercepted = new OffsetDeleteResponse(struct, ver);

      assertEquals(outbound.data.topics().size(), intercepted.data.topics().size());

      for (OffsetDeleteResponseTopic interceptedTopic : intercepted.data.topics()) {
        assertFalse(interceptedTopic.name().startsWith(tenantPrefix));
        OffsetDeleteResponseTopic outboundOriginal = outbound.data.topics()
            .find(context.tenantContext.addTenantPrefix(interceptedTopic.name()));
        assertEquals(context.tenantContext.removeTenantPrefix(outboundOriginal.name()), interceptedTopic.name());
        assertEquals(outboundOriginal.partitions().size(), interceptedTopic.partitions().size());
      }

      verifyResponseMetrics(ApiKeys.OFFSET_DELETE, Errors.NONE);
    }
  }

  CreateTopicsRequestData.CreatableTopic creatableTopic(String topicName, int numPartitions,
                                                        short replicationFactor,
                                                        CreateableTopicConfigCollection configs) {
      return new CreateTopicsRequestData.CreatableTopic()
              .setName(topicName)
              .setNumPartitions(numPartitions)
              .setReplicationFactor(replicationFactor)
              .setConfigs(configs);
  }

  CreateTopicsRequestData.CreatableTopic creatableTopic(String topicName, int numPartitions, short replicationFactor) {
      return new CreateTopicsRequestData.CreatableTopic()
              .setName(topicName)
              .setNumPartitions(numPartitions)
              .setReplicationFactor(replicationFactor);
  }

  @Test
  public void testCreateTopicsRequest() {
    for (short ver = ApiKeys.CREATE_TOPICS.oldestVersion(); ver <= ApiKeys.CREATE_TOPICS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.CREATE_TOPICS, ver);
      CreatableTopicCollection requestTopics =
              new CreateTopicsRequestData.CreatableTopicCollection();

      requestTopics.add(creatableTopic("foo", 4, (short) 1, testConfigs()));

      CreatableReplicaAssignmentCollection unbalancedAssignments =
              new CreateTopicsRequestData.CreatableReplicaAssignmentCollection();
      unbalancedAssignments.add(new CreatableReplicaAssignment()
              .setPartitionIndex(0)
              .setBrokerIds(asList(0, 1)));
      unbalancedAssignments.add(new CreatableReplicaAssignment()
              .setPartitionIndex(1)
              .setBrokerIds(asList(0, 1)));

      requestTopics.add(new CreateTopicsRequestData.CreatableTopic()
              .setName("bar")
              .setNumPartitions(3)
              .setAssignments(unbalancedAssignments)
              .setReplicationFactor((short) 5));

      requestTopics.add(creatableTopic("invalid", 3, (short) 5));

      CreateTopicsRequest inbound = new CreateTopicsRequest.Builder(
              new CreateTopicsRequestData()
                      .setTopics(requestTopics)
                      .setTimeoutMs(30000)
                      .setValidateOnly(false))
              .build(ver);

      CreateTopicsRequest intercepted = (CreateTopicsRequest) parseRequest(context, inbound);

      assertEquals(mkSet("tenant_foo", "tenant_bar", "tenant_invalid"),
              intercepted.data().topics().stream().map(t -> t.name()).collect(Collectors.toSet()));

      assertEquals(4, intercepted.data().topics().find("tenant_foo").assignments().size());
      // Configs should be transformed by removing non-updateable configs, except for min.insync.replicas
      assertEquals(transformedTestConfigs(),
              intercepted.data().topics().find("tenant_foo").configs());

      // if assignment is set, verify number of partitions and replication factor is not set
      assertEquals(CreateTopicsRequest.NO_NUM_PARTITIONS,
                   intercepted.data().topics().find("tenant_foo").numPartitions());
      assertEquals(CreateTopicsRequest.NO_REPLICATION_FACTOR,
                   intercepted.data().topics().find("tenant_foo").replicationFactor());

      assertEquals(2, intercepted.data().topics().find("tenant_bar").assignments().size());
      assertNotEquals(unbalancedAssignments,
              intercepted.data().topics().find("tenant_invalid").assignments());

      assertTrue(intercepted.data().topics().find("tenant_invalid").assignments().isEmpty());
      assertEquals(3, intercepted.data().topics().find("tenant_invalid").numPartitions());
      assertEquals(5, intercepted.data().topics().find("tenant_invalid").replicationFactor());

      verifyRequestMetrics(ApiKeys.CREATE_TOPICS);
    }
  }

  @Test
  public void testCreateTopicsRequestWithoutPartitionAssignor() {
    partitionAssignor = null;
    for (short ver = ApiKeys.CREATE_TOPICS.oldestVersion(); ver <= ApiKeys.CREATE_TOPICS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.CREATE_TOPICS, ver);

       CreateTopicsRequestData.CreatableTopicCollection requestTopics =
              new CreateTopicsRequestData.CreatableTopicCollection();
      requestTopics.add(creatableTopic("foo", 4, (short) 1, testConfigs()));

       CreateTopicsRequestData.CreatableReplicaAssignmentCollection unbalancedAssignments =
              new CreateTopicsRequestData.CreatableReplicaAssignmentCollection();
      unbalancedAssignments.add(new CreateTopicsRequestData.CreatableReplicaAssignment()
              .setPartitionIndex(0)
              .setBrokerIds(asList(0, 1)));
      unbalancedAssignments.add(new CreateTopicsRequestData.CreatableReplicaAssignment()
              .setPartitionIndex(1)
              .setBrokerIds(asList(0, 1)));

      requestTopics.add(new CreateTopicsRequestData.CreatableTopic()
              .setName("bar")
              .setNumPartitions(3)
              .setAssignments(unbalancedAssignments)
              .setReplicationFactor((short) 5));

      CreateTopicsRequest inbound = new CreateTopicsRequest.Builder(
              new CreateTopicsRequestData()
                      .setTopics(requestTopics)
                      .setTimeoutMs(30000)
                      .setValidateOnly(false))
              .build(ver);

      requestTopics.add(creatableTopic("foo", 3, (short) 5, testConfigs()));

      requestTopics.add(creatableTopic("invalid", 3, (short) 5));

      CreateTopicsRequest intercepted = (CreateTopicsRequest) parseRequest(context, inbound);

      assertEquals(mkSet("tenant_foo", "tenant_bar", "tenant_invalid"),
              intercepted.data().topics().stream().map(t -> t.name()).collect(Collectors.toSet()));

      assertEquals(4, intercepted.data().topics().find("tenant_foo").numPartitions());
      assertEquals(1, intercepted.data().topics().find("tenant_foo").replicationFactor());
      assertTrue(intercepted.data().topics().find("tenant_foo").assignments().isEmpty());

      // Configs should be transformed by removing non-updateable configs, except for min.insync.replicas
      assertEquals(transformedTestConfigs(),
              intercepted.data().topics().find("tenant_foo").configs());

      assertEquals(2, intercepted.data().topics().find("tenant_bar").assignments().size());
      assertNotEquals(unbalancedAssignments,
              intercepted.data().topics().find("tenant_invalid").assignments());

      assertTrue(intercepted.data().topics().find("tenant_invalid").assignments().isEmpty());
      assertEquals(3, intercepted.data().topics().find("tenant_invalid").numPartitions());
      assertEquals(5, intercepted.data().topics().find("tenant_invalid").replicationFactor());

      verifyRequestMetrics(ApiKeys.CREATE_TOPICS);
    }
  }

  @Test
  public void testCreateTopicsResponse() throws IOException {
    for (short ver = ApiKeys.CREATE_TOPICS.oldestVersion(); ver <= ApiKeys.CREATE_TOPICS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.CREATE_TOPICS, ver);
      List<CreatableTopicConfigs> configs = Arrays.asList(
          new CreatableTopicConfigs().setConfigName("confluent.tier.enable").setValue("true"),
          new CreatableTopicConfigs().setConfigName(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG).setValue("{}"),
          new CreatableTopicConfigs().setConfigName("max.message.bytes").setValue("100000"),
          new CreatableTopicConfigs().setConfigName("tenant_config").setValue("somevalue")
      );
      CreatableTopicResult firstResult = new CreatableTopicResult()
              .setErrorCode(Errors.NONE.code())
              .setErrorMessage("")
              .setName("tenant_foo")
              .setTopicConfigErrorCode(Errors.NONE.code());
      if (ver >= 5) {
          // Will throw for versions < 5
          firstResult = firstResult.setConfigs(configs).setNumPartitions(2).setReplicationFactor((short) 3);
      }
      Collection<CreatableTopicResult> results = asList(
              firstResult,
              new CreatableTopicResult()
                      .setErrorCode(Errors.NONE.code())
                      .setErrorMessage("")
                      .setName("tenant_bar"));
      CreateTopicsResponse outbound = new CreateTopicsResponse(new CreateTopicsResponseData()
              .setTopics(new CreatableTopicResultCollection(results.iterator())));
      Struct struct = parseResponse(ApiKeys.CREATE_TOPICS, ver, context.buildResponse(outbound));
      CreateTopicsResponse intercepted = new CreateTopicsResponse(struct, ver);
      assertEquals(new HashSet<>(asList("foo", "bar")), intercepted.data().topics()
              .stream().map(CreatableTopicResult::name).collect(Collectors.toSet()));
      if (ver >= 5) {
        assertEquals(Utils.mkSet("max.message.bytes", "tenant_config"),
            intercepted.data().topics().find("foo").configs().stream().map(CreatableTopicConfigs::configName).collect(Collectors.toSet()));
      } else {
        assertTrue(intercepted.data().topics().find("foo").configs().isEmpty());
      }
      verifyResponseMetrics(ApiKeys.CREATE_TOPICS, Errors.NONE);
    }
  }

  @Test
  public void testCreateTopicsResponsePolicyFailure() throws IOException {
    for (short ver = ApiKeys.CREATE_TOPICS.oldestVersion(); ver <= ApiKeys.CREATE_TOPICS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.CREATE_TOPICS, ver);
      Collection<CreatableTopicResult> results = asList(
              new CreatableTopicResult()
                      .setErrorCode(Errors.POLICY_VIOLATION.code())
                      .setErrorMessage("Topic tenant_foo is not permitted")
                      .setName("tenant_foo"),
              new CreatableTopicResult()
                      .setErrorCode(Errors.NONE.code())
                      .setErrorMessage("")
                      .setName("tenant_bar"));
      CreateTopicsResponse outbound = new CreateTopicsResponse(new CreateTopicsResponseData()
              .setTopics(new CreatableTopicResultCollection(results.iterator())));
      Struct struct = parseResponse(ApiKeys.CREATE_TOPICS, ver, context.buildResponse(outbound));
      CreateTopicsResponse intercepted = new CreateTopicsResponse(struct, ver);
      assertEquals(new HashSet<>(asList("foo", "bar")), intercepted.data().topics()
              .stream().map(CreatableTopicResult::name).collect(Collectors.toSet()));
      assertEquals(Errors.NONE.code(), intercepted.data().topics().find("bar").errorCode());
      assertEquals(Errors.POLICY_VIOLATION.code(), intercepted.data().topics().find("foo").errorCode());
      if (ver >= 1) {
        assertEquals("Topic foo is not permitted",
                intercepted.data().topics().find("foo").errorMessage());
      }
    }
  }

  @Test
  public void testDeleteTopicsRequest() {
    for (short ver = ApiKeys.DELETE_TOPICS.oldestVersion(); ver <= ApiKeys.DELETE_TOPICS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.DELETE_TOPICS, ver);
      DeleteTopicsRequest inbound = new DeleteTopicsRequest.Builder(
              new DeleteTopicsRequestData()
                      .setTopicNames(asList("foo", "bar")))
              .build(ver);
      DeleteTopicsRequest intercepted = (DeleteTopicsRequest) parseRequest(context, inbound);
      assertEquals(mkSet("tenant_foo", "tenant_bar"), new HashSet<>(intercepted.data().topicNames()));
      verifyRequestMetrics(ApiKeys.DELETE_TOPICS);
    }
  }

  @Test
  public void testDeleteTopicsResponse() throws IOException {
    for (short ver = ApiKeys.DELETE_TOPICS.oldestVersion(); ver <= ApiKeys.DELETE_TOPICS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.DELETE_TOPICS, ver);
      DeletableTopicResultCollection deleted = new DeletableTopicResultCollection();
      deleted.add(new DeletableTopicResult().setName("tenant_foo").setErrorCode(Errors.NONE.code()));
      deleted.add(new DeletableTopicResult().setName("tenant_bar").setErrorCode(Errors.NONE.code()));
      DeleteTopicsResponse outbound = new DeleteTopicsResponse(new DeleteTopicsResponseData().setResponses(deleted));
      Struct struct = parseResponse(ApiKeys.DELETE_TOPICS, ver, context.buildResponse(outbound));
      DeleteTopicsResponse intercepted = new DeleteTopicsResponse(struct, ver);
      assertEquals(mkSet("foo", "bar"),
              intercepted.data().responses().stream()
                      .map(DeletableTopicResult::name)
                      .collect(Collectors.toSet()));
      verifyResponseMetrics(ApiKeys.DELETE_TOPICS, Errors.NONE);
    }
  }

  @Test
  public void testInitProducerIdRequest() {
    for (short ver = ApiKeys.INIT_PRODUCER_ID.oldestVersion(); ver <= ApiKeys.INIT_PRODUCER_ID.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.INIT_PRODUCER_ID, ver);
      InitProducerIdRequest inbound = new InitProducerIdRequest.Builder(new InitProducerIdRequestData().
          setTransactionalId("tr").setTransactionTimeoutMs(30000)).build(ver);
      InitProducerIdRequest intercepted = (InitProducerIdRequest) parseRequest(context, inbound);
      assertEquals("tenant_tr", intercepted.data.transactionalId());
      verifyRequestMetrics(ApiKeys.INIT_PRODUCER_ID);
    }
  }

  @Test
  public void testInitProducerIdRequestNullTransactionalId() {
    for (short ver = ApiKeys.INIT_PRODUCER_ID.oldestVersion(); ver <= ApiKeys.INIT_PRODUCER_ID.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.INIT_PRODUCER_ID, ver);
      InitProducerIdRequest inbound = new InitProducerIdRequest.Builder(new InitProducerIdRequestData().
          setTransactionalId(null).setTransactionTimeoutMs(1000)).build(ver);
      InitProducerIdRequest intercepted = (InitProducerIdRequest) parseRequest(context, inbound);
      assertNull(intercepted.data.transactionalId());
      verifyRequestMetrics(ApiKeys.INIT_PRODUCER_ID);
    }
  }

  @Test
  public void testControlledShutdownNotAllowed() throws Exception {
    for (short ver = ApiKeys.CONTROLLED_SHUTDOWN.oldestVersion(); ver <= ApiKeys.CONTROLLED_SHUTDOWN.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.CONTROLLED_SHUTDOWN, ver);
      ControlledShutdownRequest inbound = new ControlledShutdownRequest.Builder(
          new ControlledShutdownRequestData()
              .setBrokerId(1)
              .setBrokerEpoch(0), ver)
          .build(ver);
      ControlledShutdownRequest request = (ControlledShutdownRequest) parseRequest(context, inbound);
      assertTrue(context.shouldIntercept());
      ControlledShutdownResponse response = (ControlledShutdownResponse) context.intercept(request, 0);
      Struct struct = parseResponse(ApiKeys.CONTROLLED_SHUTDOWN, ver, context.buildResponse(response));
      ControlledShutdownResponse outbound = new ControlledShutdownResponse(new ControlledShutdownResponseData(struct, ver));
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, outbound.error());
      verifyRequestAndResponseMetrics(ApiKeys.CONTROLLED_SHUTDOWN, Errors.CLUSTER_AUTHORIZATION_FAILED);
    }
  }

  @Test
  public void testStopReplicaNotAllowed() throws Exception {
    for (short ver = ApiKeys.STOP_REPLICA.oldestVersion(); ver <= ApiKeys.STOP_REPLICA.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.STOP_REPLICA, ver);
      TopicPartition partition = new TopicPartition("foo", 0);
      StopReplicaRequest inbound = new StopReplicaRequest.Builder((short) 1, 0, 0, 0, false,
          Collections.singleton(partition)).build(ver);
      StopReplicaRequest request = (StopReplicaRequest) parseRequest(context, inbound);
      assertEquals(singletonList(partition),
          StreamSupport.stream(request.partitions().spliterator(), false).map(p ->
              new TopicPartition(p.topic(), p.partition())).collect(toList()));
      assertTrue(context.shouldIntercept());
      StopReplicaResponse response = (StopReplicaResponse) context.intercept(request, 0);
      Struct struct = parseResponse(ApiKeys.STOP_REPLICA, ver, context.buildResponse(response));
      StopReplicaResponse outbound = new StopReplicaResponse(struct, ver);
      assertEquals(Optional.of(Errors.CLUSTER_AUTHORIZATION_FAILED.code()), outbound.partitionErrors()
          .stream()
          .filter(pe -> pe.topicName().equals(partition.topic()) && pe.partitionIndex() == partition.partition())
          .findFirst()
          .map(pe -> pe.errorCode()));
      verifyRequestAndResponseMetrics(ApiKeys.STOP_REPLICA, Errors.CLUSTER_AUTHORIZATION_FAILED);
    }
  }

  @Test
  public void testLeaderAndIsrNotAllowed() throws Exception {
    for (short ver = ApiKeys.LEADER_AND_ISR.oldestVersion(); ver <= ApiKeys.LEADER_AND_ISR.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.LEADER_AND_ISR, ver);
      String topic = "foo";
      int partition = 0;
      LeaderAndIsrRequest inbound = new LeaderAndIsrRequest.Builder(ver, 1, 1, 0,
          Collections.singletonList(new LeaderAndIsrPartitionState()
              .setTopicName(topic)
              .setPartitionIndex(partition)
              .setControllerEpoch(15)
              .setLeader(1)
              .setLeaderEpoch(20)
              .setIsr(Collections.emptyList())
              .setZkVersion(15)
              .setReplicas(Collections.emptyList())
              .setIsNew(false)),
              Collections.emptySet(),
              false).build(ver);
      LeaderAndIsrRequest request = (LeaderAndIsrRequest) parseRequest(context, inbound);
      assertTrue(context.shouldIntercept());
      LeaderAndIsrResponse response = (LeaderAndIsrResponse) context.intercept(request, 0);
      Struct struct = parseResponse(ApiKeys.LEADER_AND_ISR, ver, context.buildResponse(response));
      LeaderAndIsrResponse outbound = new LeaderAndIsrResponse(struct, ver, false);
      assertEquals(Optional.of(Errors.CLUSTER_AUTHORIZATION_FAILED.code()),
          outbound.partitions().stream()
              .filter(ps -> ps.topicName().equals(topic) && ps.partitionIndex() == partition)
              .findFirst()
              .map(pe -> pe.errorCode()));
      verifyRequestAndResponseMetrics(ApiKeys.LEADER_AND_ISR, Errors.CLUSTER_AUTHORIZATION_FAILED);
    }
  }

  @Test
  public void testUpdateMetadataNotAllowed() throws Exception {
    for (short ver = ApiKeys.UPDATE_METADATA.oldestVersion(); ver <= ApiKeys.UPDATE_METADATA.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.UPDATE_METADATA, ver);
      String topic = "foo";
      int partition = 0;
      UpdateMetadataRequest inbound = new UpdateMetadataRequest.Builder(ver, 1, 1, 0,
          Collections.singletonList(new UpdateMetadataPartitionState()
              .setTopicName(topic)
              .setPartitionIndex(partition)
              .setControllerEpoch(15)
              .setLeader(1)
              .setLeaderEpoch(20)
              .setIsr(Collections.emptyList())
              .setZkVersion(15)
              .setReplicas(Collections.emptyList())),
          Collections.emptyList()).build(ver);
      UpdateMetadataRequest request = (UpdateMetadataRequest) parseRequest(context, inbound);
      assertTrue(context.shouldIntercept());
      UpdateMetadataResponse response = (UpdateMetadataResponse) context.intercept(request, 0);
      Struct struct = parseResponse(ApiKeys.UPDATE_METADATA, ver, context.buildResponse(response));
      UpdateMetadataResponse outbound = new UpdateMetadataResponse(struct, ver);
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, outbound.error());
      verifyRequestAndResponseMetrics(ApiKeys.UPDATE_METADATA, Errors.CLUSTER_AUTHORIZATION_FAILED);
    }
  }

  @Test
  public void testOffsetForLeaderEpochRequest() {
    for (short ver = ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion(); ver <= ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.OFFSET_FOR_LEADER_EPOCH, ver);
      TopicPartition partition = new TopicPartition("foo", 0);
      OffsetsForLeaderEpochRequest inbound = OffsetsForLeaderEpochRequest.Builder.forFollower(
              ver, Collections.singletonMap(partition, new OffsetsForLeaderEpochRequest.PartitionData(Optional.empty(), 0)), 1).build(ver);
      OffsetsForLeaderEpochRequest request = (OffsetsForLeaderEpochRequest) parseRequest(context, inbound);
      assertEquals(mkSet(new TopicPartition("tenant_foo", 0)), request.epochsByTopicPartition().keySet());
      assertFalse(context.shouldIntercept());
      verifyRequestMetrics(ApiKeys.OFFSET_FOR_LEADER_EPOCH);
    }
  }

  @Test
  public void testOffsetForLeaderEpochResponse() throws Exception {
    for (short ver = ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion(); ver <= ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(); ver++) {
      TopicPartition partition = new TopicPartition("foo", 0);
      MultiTenantRequestContext context = newRequestContext(ApiKeys.OFFSET_FOR_LEADER_EPOCH, ver);
      OffsetsForLeaderEpochResponse outbound = new OffsetsForLeaderEpochResponse(Collections.singletonMap(
              new TopicPartition("tenant_foo", 0), new EpochEndOffset(5, 37L)));
      Struct struct = parseResponse(ApiKeys.OFFSET_FOR_LEADER_EPOCH, ver, context.buildResponse(outbound));
      OffsetsForLeaderEpochResponse intercepted = new OffsetsForLeaderEpochResponse(struct);
      assertEquals(1, intercepted.responses().size());
      assertEquals(Errors.NONE, intercepted.responses().get(partition).error());
      verifyResponseMetrics(ApiKeys.OFFSET_FOR_LEADER_EPOCH, Errors.NONE);
    }
  }

  @Test
  public void testWriteTxnMarkersNotAllowed() throws Exception {
    for (short ver = ApiKeys.WRITE_TXN_MARKERS.oldestVersion(); ver <= ApiKeys.WRITE_TXN_MARKERS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.WRITE_TXN_MARKERS, ver);
      TopicPartition partition = new TopicPartition("foo", 0);
      WriteTxnMarkersRequest inbound = new WriteTxnMarkersRequest.Builder(
          singletonList(new WriteTxnMarkersRequest.TxnMarkerEntry(233L, (short) 5, 37,
              TransactionResult.ABORT, singletonList(partition)))).build(ver);
      WriteTxnMarkersRequest request = (WriteTxnMarkersRequest) parseRequest(context, inbound);
      assertEquals(1, request.markers().size());
      assertEquals(singletonList(new TopicPartition("tenant_foo", 0)),
          request.markers().get(0).partitions());
      assertTrue(context.shouldIntercept());
      WriteTxnMarkersResponse response = (WriteTxnMarkersResponse) context.intercept(request, 0);
      Struct struct = parseResponse(ApiKeys.WRITE_TXN_MARKERS, ver, context.buildResponse(response));
      WriteTxnMarkersResponse outbound = new WriteTxnMarkersResponse(struct);
      assertEquals(1, outbound.errors(233L).size());
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, outbound.errors(233L).get(partition));
      verifyRequestAndResponseMetrics(ApiKeys.WRITE_TXN_MARKERS, Errors.CLUSTER_AUTHORIZATION_FAILED);
    }
  }

  private static class AclTestParams {
    final static List<ResourceType> RESOURCE_TYPES = asList(
        ResourceType.TOPIC,
        ResourceType.GROUP,
        ResourceType.TRANSACTIONAL_ID,
        ResourceType.CLUSTER
    );
    final PatternType patternType;
    final boolean wildcard;
    final boolean hasResourceName;

    AclTestParams(PatternType patternType, boolean wildcard, boolean hasResourceName) {
      this.patternType = patternType;
      this.wildcard = wildcard;
      this.hasResourceName = hasResourceName;
    }

    private String resourceName(ResourceType resourceType) {
      String suffix = resourceType.name().toLowerCase(LOCALE);
      if (!hasResourceName) {
        return null;
      } else if (wildcard) {
        return "*";
      } else if (resourceType == ResourceType.CLUSTER) {
        return "kafka-cluster";
      } else if (patternType == PatternType.PREFIXED) {
        return "prefix." + suffix;
      } else {
        return "test." + suffix;
      }
    }

    String tenantResourceName(ResourceType resourceType) {
      String suffix = resourceType.name().toLowerCase(LOCALE);
      if (!hasResourceName) {
        return "tenant_";
      } else if (wildcard) {
        return "tenant_";
      } else if (resourceType == ResourceType.CLUSTER) {
        return "tenant_kafka-cluster";
      } else if (patternType == PatternType.PREFIXED) {
        return "tenant_prefix." + suffix;
      } else {
        return "tenant_test." + suffix;
      }
    }

    String principal() {
      return wildcard ? "User:*" : "User:principal";
    }

    String tenantPrincipal() {
      return wildcard ? "TenantUser*:tenant_" : "TenantUser:tenant_principal";
    }

    PatternType tenantPatternType(ResourceType resourceType) {
      if (hasResourceName) {
        switch (patternType) {
          case LITERAL:
            return wildcard ? PatternType.PREFIXED : PatternType.LITERAL;
          case PREFIXED:
            return PatternType.PREFIXED;
          case ANY:
            return PatternType.ANY;
          case MATCH:
            return PatternType.CONFLUENT_ONLY_TENANT_MATCH;
          default:
            throw new IllegalArgumentException("Unsupported pattern type " + patternType);
        }
      } else {
        switch (patternType) {
          case LITERAL:
            return PatternType.CONFLUENT_ALL_TENANT_LITERAL;
          case PREFIXED:
            return PatternType.CONFLUENT_ALL_TENANT_PREFIXED;
          case ANY:
          case MATCH:
            return PatternType.CONFLUENT_ALL_TENANT_ANY;
          default:
            throw new IllegalArgumentException("Unsupported pattern type " + patternType);
        }
      }
    }

    @Override
    public String toString() {
      return String.format("AclTestParams(patternType=%s, wildcard=%s, hasResourceName=%s)",
          patternType, wildcard, hasResourceName);
    }

    static List<AclTestParams> aclTestParams(short ver) {
      List<AclTestParams> tests = new ArrayList<>();
      tests.add(new AclTestParams(PatternType.LITERAL, false, true));
      tests.add(new AclTestParams(PatternType.LITERAL, true, true));
      if (ver > 0) {
        tests.add(new AclTestParams(PatternType.PREFIXED, false, true));
      }
      return tests;
    }

    static List<AclTestParams> filterTestParams(short ver) {
      List<AclTestParams> tests = new ArrayList<>();
      tests.add(new AclTestParams(PatternType.LITERAL, false, true));
      tests.add(new AclTestParams(PatternType.LITERAL, true, true));
      tests.add(new AclTestParams(PatternType.LITERAL, false, false));
      if (ver > 0) {
        tests.add(new AclTestParams(PatternType.PREFIXED, false, true));
        tests.add(new AclTestParams(PatternType.PREFIXED, false, false));
        tests.add(new AclTestParams(PatternType.ANY, false, true));
        tests.add(new AclTestParams(PatternType.ANY, false, false));
        tests.add(new AclTestParams(PatternType.MATCH, false, true));
        tests.add(new AclTestParams(PatternType.MATCH, false, false));
      }
      return tests;
    }
  }

  @Test
  public void testCreateAclsRequest() throws Exception {
    for (short ver = ApiKeys.CREATE_ACLS.oldestVersion(); ver <= ApiKeys.CREATE_ACLS.latestVersion(); ver++) {
      final short version = ver;
      AclTestParams.aclTestParams(ver).forEach(params -> {
        try {
          verifyCreateAclsRequest(params, version);
        } catch (Throwable e) {
          throw new RuntimeException("CreateAclsRequest test failed with " + params, e);
        }
      });
      AclBinding acl = new AclBinding(
          new ResourcePattern(ResourceType.DELEGATION_TOKEN, "123", PatternType.LITERAL),
          new AccessControlEntry("User:1", "*", AclOperation.WRITE, AclPermissionType.ALLOW));
      verifyInvalidCreateAclsRequest(acl, version);

      List<String> invalidPrincipals = asList("", "userWithoutPrincipalType");
      invalidPrincipals.forEach(principal -> {
        AclBinding invalidAcl = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "topic1", PatternType.LITERAL),
            new AccessControlEntry(principal, "*", AclOperation.WRITE,
                AclPermissionType.ALLOW));
        verifyInvalidCreateAclsRequest(invalidAcl, version);
      });
    }
  }

  private void verifyCreateAclsRequest(AclTestParams params, short version) throws Exception {
    MultiTenantRequestContext context = newRequestContext(ApiKeys.CREATE_ACLS, version);
    AccessControlEntry ace =
        new AccessControlEntry(params.principal(), "*", AclOperation.CREATE, AclPermissionType.ALLOW);
    List<CreateAclsRequest.AclCreation> aclCreations = AclTestParams.RESOURCE_TYPES.stream().map(resourceType ->
        new CreateAclsRequest.AclCreation(new AclBinding(
            new ResourcePattern(resourceType, params.resourceName(resourceType), params.patternType), ace)
        )).collect(Collectors.toList());

    CreateAclsRequest inbound = new CreateAclsRequest.Builder(aclCreations).build(version);
    CreateAclsRequest request = (CreateAclsRequest) parseRequest(context, inbound);
    assertEquals(aclCreations.size(), request.aclCreations().size());

    request.aclCreations().forEach(creation -> {
      assertEquals(params.tenantPrincipal(), creation.acl().entry().principal());
      ResourcePattern pattern = creation.acl().pattern();
      assertEquals(params.tenantPatternType(pattern.resourceType()), pattern.patternType());
      assertEquals(params.tenantResourceName(pattern.resourceType()), pattern.name());
    });
    assertEquals(AclTestParams.RESOURCE_TYPES,
        request.aclCreations().stream().map(c -> c.acl().pattern().resourceType()).collect(Collectors.toList()));

    assertFalse(context.shouldIntercept());
    verifyRequestMetrics(ApiKeys.CREATE_ACLS);
  }

  private void verifyInvalidCreateAclsRequest(AclBinding acl, short version) {
    AclCreation aclCreation = new AclCreation(acl);
    CreateAclsRequest inbound = new CreateAclsRequest.Builder(
        singletonList(aclCreation)).build(version);
    MultiTenantRequestContext context = newRequestContext(ApiKeys.CREATE_ACLS, version);
    parseRequest(context, inbound);
    assertTrue(context.shouldIntercept());
    assertEquals(Collections.singleton(Errors.INVALID_REQUEST), context.intercept(inbound, 0).errorCounts().keySet());
  }

  @Test
  public void testCreateAclsResponse() throws Exception {
    for (short ver = ApiKeys.CREATE_ACLS.oldestVersion();
        ver <= ApiKeys.CREATE_ACLS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.CREATE_ACLS, ver);
      List<CreateAclsResponse.AclCreationResponse> aclCreationResponses =
          singletonList(new AclCreationResponse(ApiError.NONE));
      CreateAclsResponse outbound = new CreateAclsResponse(23, aclCreationResponses);
      Struct struct = parseResponse(ApiKeys.CREATE_ACLS, ver, context.buildResponse(outbound));
      CreateAclsResponse intercepted = new CreateAclsResponse(struct, ver);
      assertEquals(ApiError.NONE.error(),
          intercepted.aclCreationResponses().get(0).error().error());
      verifyResponseMetrics(ApiKeys.CREATE_ACLS, Errors.NONE);
    }
  }

  @Test
  public void testDeleteAclsRequest() throws Exception {
    for (short ver = ApiKeys.DELETE_ACLS.oldestVersion(); ver <= ApiKeys.DELETE_ACLS.latestVersion(); ver++) {
      final short version = ver;
      AclTestParams.filterTestParams(ver).forEach(params -> {
        try {
          verifyDeleteAclsRequest(params, version);
        } catch (Throwable e) {
          throw new RuntimeException("DeleteAclsRequest test failed with " + params, e);
        }
      });
    }
  }

  private void verifyDeleteAclsRequest(AclTestParams params, short version) {
    MultiTenantRequestContext context = newRequestContext(ApiKeys.DELETE_ACLS, version);
    AccessControlEntryFilter ace =
        new AccessControlEntryFilter(params.principal(), "*", AclOperation.CREATE, AclPermissionType.ALLOW);
    List<AclBindingFilter> aclBindingFilters = AclTestParams.RESOURCE_TYPES.stream().map(resourceType ->
        new AclBindingFilter(new ResourcePatternFilter(resourceType, params.resourceName(resourceType), params.patternType), ace))
        .collect(Collectors.toList());

    DeleteAclsRequest inbound = new DeleteAclsRequest.Builder(aclBindingFilters).build(version);
    DeleteAclsRequest request = (DeleteAclsRequest) parseRequest(context, inbound);
    assertEquals(aclBindingFilters.size(), request.filters().size());

    request.filters().forEach(acl -> {
      assertEquals(params.tenantPrincipal(), acl.entryFilter().principal());
      ResourcePatternFilter pattern = acl.patternFilter();
      assertEquals(params.tenantPatternType(pattern.resourceType()), pattern.patternType());
      assertEquals(params.tenantResourceName(pattern.resourceType()), pattern.name());
    });
    assertEquals(AclTestParams.RESOURCE_TYPES,
        request.filters().stream().map(acl -> acl.patternFilter().resourceType()).collect(Collectors.toList()));
  }

  @Test
  public void testDeleteAclsResponse() throws Exception {
    for (short ver = ApiKeys.DELETE_ACLS.oldestVersion(); ver <= ApiKeys.DELETE_ACLS.latestVersion(); ver++) {
      final short version = ver;
      AclTestParams.aclTestParams(ver).forEach(params -> {
        try {
          verifyDeleteAclsResponse(params, version);
        } catch (Throwable e) {
          throw new RuntimeException("DeleteAclsResponse test failed with " + params, e);
        }
      });
    }
  }

  private void verifyDeleteAclsResponse(AclTestParams params, short version) throws Exception {
    MultiTenantRequestContext context = newRequestContext(ApiKeys.DELETE_ACLS, version);
    AccessControlEntry ace =
        new AccessControlEntry(params.tenantPrincipal(), "*", AclOperation.ALTER, AclPermissionType.DENY);
    List<AclDeletionResult> deletionResults0 = asList(
        new AclDeletionResult(ApiError.NONE, new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, params.tenantResourceName(ResourceType.TOPIC),
                params.tenantPatternType(ResourceType.TOPIC)), ace)),
        new AclDeletionResult(ApiError.NONE, new AclBinding(
            new ResourcePattern(ResourceType.GROUP, params.tenantResourceName(ResourceType.GROUP),
                params.tenantPatternType(ResourceType.GROUP)), ace)));
    List<AclDeletionResult> deletionResults1 = asList(
        new AclDeletionResult(ApiError.NONE, new AclBinding(
            new ResourcePattern(ResourceType.TRANSACTIONAL_ID, params.tenantResourceName(ResourceType.TRANSACTIONAL_ID),
                params.tenantPatternType(ResourceType.TRANSACTIONAL_ID)), ace)),
        new AclDeletionResult(ApiError.NONE, new AclBinding(
            new ResourcePattern(ResourceType.CLUSTER, params.tenantResourceName(ResourceType.CLUSTER),
                params.tenantPatternType(ResourceType.CLUSTER)), ace)));
    List<DeleteAclsResponse.AclFilterResponse> aclDeletionResponses = asList(
        new DeleteAclsResponse.AclFilterResponse(ApiError.NONE, deletionResults0),
        new DeleteAclsResponse.AclFilterResponse(ApiError.NONE, deletionResults1));
    DeleteAclsResponse outbound = new DeleteAclsResponse(11, aclDeletionResponses);
    Struct struct = parseResponse(ApiKeys.DELETE_ACLS, version, context.buildResponse(outbound));
    DeleteAclsResponse intercepted = new DeleteAclsResponse(struct, version);
    List<DeleteAclsResponse.AclFilterResponse> interceptedResponses = intercepted.responses();
    assertEquals(aclDeletionResponses.size(), interceptedResponses.size());

    interceptedResponses.forEach(acl -> {
      assertEquals(ApiError.NONE.error(), acl.error().error());
      acl.deletions().forEach(deletion -> {
        assertEquals(params.principal(), deletion.acl().entry().principal());
        ResourcePattern pattern = deletion.acl().pattern();
        assertEquals(params.patternType, pattern.patternType());
        assertEquals(params.resourceName(pattern.resourceType()), pattern.name());
      });
    });

    Iterator<AclDeletionResult> it = interceptedResponses.get(0).deletions().iterator();
    assertEquals(ResourceType.TOPIC, it.next().acl().pattern().resourceType());
    assertEquals(ResourceType.GROUP, it.next().acl().pattern().resourceType());
    assertFalse(it.hasNext());
    it = interceptedResponses.get(1).deletions().iterator();
    assertEquals(ResourceType.TRANSACTIONAL_ID, it.next().acl().pattern().resourceType());
    assertEquals(ResourceType.CLUSTER, it.next().acl().pattern().resourceType());
    assertFalse(it.hasNext());
  }

  @Test
  public void testDescribeAclsRequest() throws Exception {
    for (short ver = ApiKeys.DESCRIBE_ACLS.oldestVersion(); ver <= ApiKeys.DESCRIBE_ACLS.latestVersion(); ver++) {
      final short version = ver;
      AclTestParams.filterTestParams(ver).forEach(params ->
        AclTestParams.RESOURCE_TYPES.forEach(resourceType -> {
          try {
            verifyDescribeAclsRequest(resourceType, params, version);
          } catch (Throwable e) {
            throw new RuntimeException("DescribeAclsRequest test failed with " + params, e);
          }
        })
      );
    }
  }

  private void verifyDescribeAclsRequest(ResourceType resourceType, AclTestParams params, short version) throws Exception {
    MultiTenantRequestContext context = newRequestContext(ApiKeys.DESCRIBE_ACLS, version);
    DescribeAclsRequest inbound = new DescribeAclsRequest.Builder(new AclBindingFilter(
        new ResourcePatternFilter(resourceType, params.resourceName(resourceType), params.patternType),
        new AccessControlEntryFilter(params.principal(), "*", AclOperation.CREATE, AclPermissionType.ALLOW)))
        .build(version);
    DescribeAclsRequest request = (DescribeAclsRequest) parseRequest(context, inbound);
    assertEquals(resourceType, request.filter().patternFilter().resourceType());

    assertEquals(params.tenantPrincipal(), request.filter().entryFilter().principal());
    assertEquals(params.tenantResourceName(resourceType), request.filter().patternFilter().name());
  }

  @Test
  public void testDescribeAclsResponse() throws Exception {
    for (short ver = ApiKeys.DESCRIBE_ACLS.oldestVersion(); ver <= ApiKeys.DESCRIBE_ACLS.latestVersion(); ver++) {
      final short version = ver;
      AclTestParams.aclTestParams(ver).forEach(params -> {
        try {
          verifyDescribeAclsResponse(params, version);
        } catch (Throwable e) {
          throw new RuntimeException("DescribeAclsResponse test failed with " + params, e);
        }
      });
    }
  }

  private void verifyDescribeAclsResponse(AclTestParams params, short version) throws Exception {
    MultiTenantRequestContext context = newRequestContext(ApiKeys.DESCRIBE_ACLS, version);
    AccessControlEntry ace = new AccessControlEntry(params.tenantPrincipal(), "*", AclOperation.CREATE, AclPermissionType.ALLOW);
    DescribeAclsResponse outbound = new DescribeAclsResponse(12, ApiError.NONE,
        AclTestParams.RESOURCE_TYPES.stream().map(resourceType ->
            new AclBinding(new ResourcePattern(resourceType, params.tenantResourceName(resourceType),
                params.tenantPatternType(resourceType)), ace)).collect(Collectors.toList()));

    Struct struct = parseResponse(ApiKeys.DESCRIBE_ACLS, version, context.buildResponse(outbound));
    DescribeAclsResponse intercepted = new DescribeAclsResponse(struct);
    assertEquals(4, intercepted.acls().size());
    intercepted.acls().forEach(acl -> {
      ResourcePattern pattern = acl.pattern();
      assertEquals(params.resourceName(pattern.resourceType()), pattern.name());
      assertEquals(params.patternType, pattern.patternType());
      assertEquals(params.principal(), acl.entry().principal());
    });

    verifyResponseMetrics(ApiKeys.DESCRIBE_ACLS, Errors.NONE);
  }

  @Test
  public void testAddPartitionsToTxnRequest() {
    for (short ver = ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(); ver <= ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.ADD_PARTITIONS_TO_TXN, ver);
      AddPartitionsToTxnRequest inbound = new AddPartitionsToTxnRequest.Builder("tr", 23L, (short) 15,
          asList(new TopicPartition("foo", 0), new TopicPartition("bar", 0))).build(ver);
      AddPartitionsToTxnRequest intercepted = (AddPartitionsToTxnRequest) parseRequest(context, inbound);
      assertEquals(mkSet(new TopicPartition("tenant_foo", 0), new TopicPartition("tenant_bar", 0)),
          new HashSet<>(intercepted.partitions()));
      assertEquals("tenant_tr", intercepted.transactionalId());
      verifyRequestMetrics(ApiKeys.ADD_PARTITIONS_TO_TXN);
    }
  }

  @Test
  public void testAddPartitionsToTxnResponse() throws IOException {
    for (short ver = ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(); ver <= ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.ADD_PARTITIONS_TO_TXN, ver);
      Map<TopicPartition, Errors> partitionErrors = new HashMap<>();
      partitionErrors.put(new TopicPartition("tenant_foo", 0), Errors.NONE);
      partitionErrors.put(new TopicPartition("tenant_bar", 0), Errors.NONE);
      AddPartitionsToTxnResponse outbound = new AddPartitionsToTxnResponse(0, partitionErrors);
      Struct struct = parseResponse(ApiKeys.ADD_PARTITIONS_TO_TXN, ver, context.buildResponse(outbound));
      AddPartitionsToTxnResponse intercepted = new AddPartitionsToTxnResponse(struct);
      assertEquals(mkSet(new TopicPartition("foo", 0), new TopicPartition("bar", 0)),
          intercepted.errors().keySet());
      verifyResponseMetrics(ApiKeys.ADD_PARTITIONS_TO_TXN, Errors.NONE);
    }
  }

  @Test
  public void testAddOffsetsToTxnRequest() {
    for (short ver = ApiKeys.ADD_OFFSETS_TO_TXN.oldestVersion(); ver <= ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.ADD_OFFSETS_TO_TXN, ver);
      AddOffsetsToTxnRequest inbound = new AddOffsetsToTxnRequest.Builder("tr", 23L, (short) 15, "group").build(ver);
      AddOffsetsToTxnRequest intercepted = (AddOffsetsToTxnRequest) parseRequest(context, inbound);
      assertEquals("tenant_tr", intercepted.transactionalId());
      assertEquals("tenant_group", intercepted.consumerGroupId());
      verifyRequestMetrics(ApiKeys.ADD_OFFSETS_TO_TXN);
    }
  }

  @Test
  public void testEndTxnRequest() {
    for (short ver = ApiKeys.END_TXN.oldestVersion(); ver <= ApiKeys.END_TXN.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.END_TXN, ver);
      EndTxnRequest inbound = new EndTxnRequest.Builder(
              new EndTxnRequestData()
                      .setTransactionalId("tr")
                      .setProducerId(23L)
                      .setProducerEpoch((short) 15)
                      .setCommitted(true))
              .build(ver);
      EndTxnRequest intercepted = (EndTxnRequest) parseRequest(context, inbound);
      assertEquals("tenant_tr", intercepted.data.transactionalId());
      verifyRequestMetrics(ApiKeys.END_TXN);
    }
  }

  @Test
  public void testTxnOffsetCommitRequest() {
    for (short ver = ApiKeys.TXN_OFFSET_COMMIT.oldestVersion(); ver <= ApiKeys.TXN_OFFSET_COMMIT.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.TXN_OFFSET_COMMIT, ver);
      List<TxnOffsetCommitRequestTopic> topics = new ArrayList<>();
      topics.add(new TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(singletonList(new TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(0)
              .setCommittedMetadata("")
              .setCommittedLeaderEpoch(-1))));
      topics.add(new TxnOffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(singletonList(new TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(0)
              .setCommittedMetadata("")
              .setCommittedLeaderEpoch(-1))));
      TxnOffsetCommitRequest inbound = new TxnOffsetCommitRequest.Builder(new TxnOffsetCommitRequestData()
          .setTransactionalId("tr")
          .setGroupId("group")
          .setProducerId(23L)
          .setProducerEpoch((short) 15)
          .setTopics(topics)).build(ver);
      TxnOffsetCommitRequest intercepted = (TxnOffsetCommitRequest) parseRequest(context, inbound);
      assertEquals("tenant_tr", intercepted.data.transactionalId());
      assertEquals("tenant_group", intercepted.data.groupId());
      assertEquals(mkSet(new TopicPartition("tenant_foo", 0), new TopicPartition("tenant_bar", 0)),
          intercepted.offsets().keySet());
      verifyRequestMetrics(ApiKeys.TXN_OFFSET_COMMIT);
    }
  }

  @Test
  public void testTxnOffsetCommitResponse() throws IOException {
    for (short ver = ApiKeys.TXN_OFFSET_COMMIT.oldestVersion(); ver <= ApiKeys.TXN_OFFSET_COMMIT.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.TXN_OFFSET_COMMIT, ver);
      Map<TopicPartition, Errors> partitionErrors = new HashMap<>();
      partitionErrors.put(new TopicPartition("tenant_foo", 0), Errors.NONE);
      partitionErrors.put(new TopicPartition("tenant_bar", 0), Errors.NONE);
      TxnOffsetCommitResponse outbound = new TxnOffsetCommitResponse(0, partitionErrors);
      Struct struct = parseResponse(ApiKeys.TXN_OFFSET_COMMIT, ver, context.buildResponse(outbound));
      TxnOffsetCommitResponse intercepted = new TxnOffsetCommitResponse(struct, ver);
      assertEquals(mkSet(new TopicPartition("foo", 0), new TopicPartition("bar", 0)),
          intercepted.errors().keySet());
      verifyResponseMetrics(ApiKeys.TXN_OFFSET_COMMIT, Errors.NONE);
    }
  }

  @Test
  public void testDeleteRecordsRequest() {
    for (short ver = ApiKeys.DELETE_RECORDS.oldestVersion(); ver <= ApiKeys.DELETE_RECORDS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.DELETE_RECORDS, ver);
      Map<TopicPartition, Long> requestPartitions = new HashMap<>();
      requestPartitions.put(new TopicPartition("foo", 0), 0L);
      requestPartitions.put(new TopicPartition("bar", 0), 0L);
      DeleteRecordsRequest inbound = new DeleteRecordsRequest.Builder(30000, requestPartitions).build(ver);
      DeleteRecordsRequest intercepted = (DeleteRecordsRequest) parseRequest(context, inbound);
      assertEquals(mkSet(new TopicPartition("tenant_foo", 0), new TopicPartition("tenant_bar", 0)),
          intercepted.partitionOffsets().keySet());
      verifyRequestMetrics(ApiKeys.DELETE_RECORDS);
    }
  }

  @Test
  public void testDeleteRecordsResponse() throws IOException {
    for (short ver = ApiKeys.DELETE_RECORDS.oldestVersion(); ver <= ApiKeys.DELETE_RECORDS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.DELETE_RECORDS, ver);
      Map<TopicPartition, DeleteRecordsResponse.PartitionResponse> partitionErrors = new HashMap<>();
      partitionErrors.put(new TopicPartition("tenant_foo", 0), new DeleteRecordsResponse.PartitionResponse(0L, Errors.NONE));
      partitionErrors.put(new TopicPartition("tenant_bar", 0), new DeleteRecordsResponse.PartitionResponse(0L, Errors.NONE));
      DeleteRecordsResponse outbound = new DeleteRecordsResponse(0, partitionErrors);
      Struct struct = parseResponse(ApiKeys.DELETE_RECORDS, ver, context.buildResponse(outbound));
      DeleteRecordsResponse intercepted = new DeleteRecordsResponse(struct);
      assertEquals(mkSet(new TopicPartition("foo", 0), new TopicPartition("bar", 0)),
          intercepted.responses().keySet());
      verifyResponseMetrics(ApiKeys.DELETE_RECORDS, Errors.NONE);
    }
  }

  @Test
  public void testCreatePartitionsRequest() throws Exception {
    testCluster.setPartitionLeaders("tenant_foo", 0, 2, 1);
    testCluster.setPartitionLeaders("tenant_bar", 0, 2, 1);
    partitionAssignor.updateClusterMetadata(testCluster.cluster());
    for (short ver = ApiKeys.CREATE_PARTITIONS.oldestVersion(); ver <= ApiKeys.CREATE_PARTITIONS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.CREATE_PARTITIONS, ver);
      List<CreatePartitionsTopic> requestTopics = new ArrayList<>();
      requestTopics.add(new CreatePartitionsTopic().setName("foo").setCount(4));
      List<CreatePartitionsAssignment> unbalancedAssignment = asList(
              new CreatePartitionsAssignment().setBrokerIds(singletonList(1)),
              new CreatePartitionsAssignment().setBrokerIds(singletonList(1)));
      requestTopics.add(new CreatePartitionsTopic()
              .setName("bar")
              .setCount(4)
              .setAssignments(unbalancedAssignment));
      requestTopics.add(new CreatePartitionsTopic().setName("invalid").setCount(4));

      CreatePartitionsRequest inbound =
              new CreatePartitionsRequest.Builder(new CreatePartitionsRequestData()
                      .setTopics(requestTopics)
                      .setTimeoutMs(30000)
                      .setValidateOnly(false))
                      .build(ver);

      CreatePartitionsRequest request = (CreatePartitionsRequest) parseRequest(context, inbound);

      Map<String, List<CreatePartitionsRequestData.CreatePartitionsAssignment>> assignments =
              request.data().topics().stream().collect(Collectors.toMap(CreatePartitionsTopic::name,
                      CreatePartitionsTopic::assignments));

      assertEquals(mkSet("tenant_foo", "tenant_bar", "tenant_invalid"), assignments.keySet());
      assertEquals(2, assignments.get("tenant_foo").size());
      assertEquals(2, assignments.get("tenant_bar").size());
      assertNotEquals(unbalancedAssignment, assignments.get("tenant_bar"));
      assertTrue(assignments.get("tenant_invalid").isEmpty());
      verifyRequestMetrics(ApiKeys.CREATE_PARTITIONS);
    }
  }

  @Test
  public void testCreatePartitionsRequestWithoutPartitionAssignor() throws Exception {
    partitionAssignor = null;
    for (short ver = ApiKeys.CREATE_PARTITIONS.oldestVersion(); ver <= ApiKeys.CREATE_PARTITIONS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.CREATE_PARTITIONS, ver);

      List<CreatePartitionsTopic> requestTopics = new ArrayList<>();
      requestTopics.add(new CreatePartitionsTopic().setName("foo").setCount(4));
      List<CreatePartitionsAssignment> unbalancedAssignment = asList(
              new CreatePartitionsAssignment().setBrokerIds(singletonList(1)),
              new CreatePartitionsAssignment().setBrokerIds(singletonList(1)));
      requestTopics.add(new CreatePartitionsTopic()
              .setName("bar")
              .setCount(4)
              .setAssignments(unbalancedAssignment));
      requestTopics.add(new CreatePartitionsTopic().setName("invalid").setCount(4));

      CreatePartitionsRequest inbound =
              new CreatePartitionsRequest.Builder(new CreatePartitionsRequestData()
                      .setTopics(requestTopics)
                      .setTimeoutMs(30000)
                      .setValidateOnly(false))
                      .build(ver);

      CreatePartitionsRequest request = (CreatePartitionsRequest) parseRequest(context, inbound);
      Map<String, List<CreatePartitionsRequestData.CreatePartitionsAssignment>> assignments =
              request.data().topics().stream().collect(Collectors.toMap(CreatePartitionsTopic::name,
                      CreatePartitionsTopic::assignments));

      assertEquals(mkSet("tenant_foo", "tenant_bar", "tenant_invalid"), assignments.keySet());
      assertTrue(assignments.get("tenant_foo").isEmpty());
      assertEquals(2, assignments.get("tenant_bar").size());
      assertEquals(unbalancedAssignment, assignments.get("tenant_bar"));
      assertTrue(assignments.get("tenant_invalid").isEmpty());
      verifyRequestMetrics(ApiKeys.CREATE_PARTITIONS);
    }
  }

  @Test
  public void testCreatePartitionsPolicyFailure() throws Exception {
    for (short ver = ApiKeys.CREATE_PARTITIONS.oldestVersion(); ver <= ApiKeys.CREATE_PARTITIONS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.CREATE_PARTITIONS, ver);
      CreatePartitionsResponseData responseData =
              new CreatePartitionsResponseData()
                      .setResults(asList(
                              new CreatePartitionsTopicResult()
                                      .setName("foo")
                                      .setErrorCode(Errors.POLICY_VIOLATION.code())
                                      .setErrorMessage("Topic tenant_foo is not permitted"),
                              new CreatePartitionsTopicResult()
                                      .setName("bar")
                                      .setErrorCode(Errors.NONE.code())));

      CreatePartitionsResponse outbound = new CreatePartitionsResponse(responseData);
      Struct struct = parseResponse(ApiKeys.CREATE_PARTITIONS, ver, context.buildResponse(outbound));
      CreatePartitionsResponse intercepted = new CreatePartitionsResponse(struct, ver);

      Map<String, CreatePartitionsTopicResult> results = intercepted.data().results().stream()
              .collect(Collectors.toMap(CreatePartitionsTopicResult::name, Function.identity()));

      assertEquals(mkSet("foo", "bar"), results.keySet());
      assertEquals(Errors.NONE.code(), results.get("bar").errorCode());
      assertEquals(Errors.POLICY_VIOLATION.code(), results.get("foo").errorCode());
      String errorMessage = results.get("foo").errorMessage();
      assertTrue(errorMessage != null);
      assertFalse(errorMessage.contains("tenant_"));
    }
  }

  @Test
  public void testDescribeConfigsRequest() {
    for (short ver = ApiKeys.DESCRIBE_CONFIGS.oldestVersion(); ver <= ApiKeys.DESCRIBE_CONFIGS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.DESCRIBE_CONFIGS, ver);
      Map<ConfigResource, Collection<String>> requestedResources = new HashMap<>();
      requestedResources.put(new ConfigResource(ConfigResource.Type.TOPIC, "foo"), Collections.emptyList());
      requestedResources.put(new ConfigResource(ConfigResource.Type.BROKER, "blah"), Collections.emptyList());
      requestedResources.put(new ConfigResource(ConfigResource.Type.TOPIC, "bar"), Collections.emptyList());
      DescribeConfigsRequest inbound = new DescribeConfigsRequest.Builder(requestedResources).build(ver);
      DescribeConfigsRequest intercepted = (DescribeConfigsRequest) parseRequest(context, inbound);
      assertEquals(mkSet(new ConfigResource(ConfigResource.Type.TOPIC, "tenant_foo"),
          new ConfigResource(ConfigResource.Type.BROKER, "blah"),
          new ConfigResource(ConfigResource.Type.TOPIC, "tenant_bar")), new HashSet<>(intercepted.resources()));
      verifyRequestMetrics(ApiKeys.DESCRIBE_CONFIGS);
    }
  }

  @Test
  public void testDescribeConfigsResponseWithFilteredBrokerConfigs() throws IOException {
    testDescribeConfigsResponse(false);
  }

  @Test
  public void testDescribeConfigsResponseWithAllBrokerConfigs() throws IOException {
    principal = new MultiTenantPrincipal("user", new TenantMetadata("tenant", "tenant_cluster_id", true));
    testDescribeConfigsResponse(true);
  }

  public void testDescribeConfigsResponse(boolean allowDescribeBrokerConfigs) throws IOException {
    DescribeConfigsResponse.ConfigSource brokerSource = DescribeConfigsResponse.ConfigSource.STATIC_BROKER_CONFIG;
    DescribeConfigsResponse.ConfigSource topicSource = DescribeConfigsResponse.ConfigSource.TOPIC_CONFIG;
    Set<DescribeConfigsResponse.ConfigSynonym> emptySynonyms = Collections.emptySet();
    Collection<DescribeConfigsResponse.ConfigEntry> brokerConfigEntries = asList(
      new DescribeConfigsResponse.ConfigEntry("message.max.bytes", "10000", brokerSource, false, false, emptySynonyms),
      new DescribeConfigsResponse.ConfigEntry("num.network.threads", "5", brokerSource, false, false, emptySynonyms),
      new DescribeConfigsResponse.ConfigEntry("broker.interceptor.class", "bar", brokerSource, false, false, emptySynonyms),
      new DescribeConfigsResponse.ConfigEntry("confluent.append.record.interceptor.classes", "foo,bar", brokerSource, false, false, emptySynonyms)
    );
    Collection<DescribeConfigsResponse.ConfigEntry> topicConfigEntries = asList(
      new DescribeConfigsResponse.ConfigEntry("retention.bytes", "10000000", topicSource, false, false, emptySynonyms),
      new DescribeConfigsResponse.ConfigEntry("min.insync.replicas", "2", topicSource, false, false, emptySynonyms),
      new DescribeConfigsResponse.ConfigEntry("min.cleanable.dirty.ratio", "0.5", topicSource, false, false, emptySynonyms),
      new DescribeConfigsResponse.ConfigEntry("confluent.tier.enable", "true", topicSource, false, false, emptySynonyms),
      new DescribeConfigsResponse.ConfigEntry("confluent.key.schema.validation", "true", brokerSource, false, false, emptySynonyms)
    );

    for (short ver = ApiKeys.DESCRIBE_CONFIGS.oldestVersion(); ver <= ApiKeys.DESCRIBE_CONFIGS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.DESCRIBE_CONFIGS, ver);
      Map<ConfigResource, DescribeConfigsResponse.Config> resourceErrors = new HashMap<>();
      resourceErrors.put(new ConfigResource(ConfigResource.Type.TOPIC, "tenant_foo"), new DescribeConfigsResponse.Config(new ApiError(Errors.NONE, ""),
          topicConfigEntries));
      resourceErrors.put(new ConfigResource(ConfigResource.Type.BROKER, "blah"), new DescribeConfigsResponse.Config(new ApiError(Errors.NONE, ""),
          brokerConfigEntries));
      resourceErrors.put(new ConfigResource(ConfigResource.Type.TOPIC, "tenant_bar"), new DescribeConfigsResponse.Config(new ApiError(Errors.NONE, ""),
          Collections.emptyList()));

      DescribeConfigsResponse outbound = new DescribeConfigsResponse(0, resourceErrors);
      Struct struct = parseResponse(ApiKeys.DESCRIBE_CONFIGS, ver, context.buildResponse(outbound));
      DescribeConfigsResponse intercepted = new DescribeConfigsResponse(struct);
      assertEquals(mkSet(new ConfigResource(ConfigResource.Type.TOPIC, "foo"),
          new ConfigResource(ConfigResource.Type.BROKER, "blah"),
          new ConfigResource(ConfigResource.Type.TOPIC, "bar")), intercepted.configs().keySet());

      Collection<DescribeConfigsResponse.ConfigEntry> interceptedTopicConfigs =
              intercepted.configs().get(new ConfigResource(ConfigResource.Type.TOPIC, "foo")).entries();

      assertTrue(intercepted.configs().get(new ConfigResource(ConfigResource.Type.TOPIC, "bar")).entries().isEmpty());

      Map<String, Boolean> topicReadOnlyMap = new HashMap<>();
      for (DescribeConfigsResponse.ConfigEntry configEntry : interceptedTopicConfigs) {
        topicReadOnlyMap.put(configEntry.name(), configEntry.isReadOnly());
      }
      if (allowDescribeBrokerConfigs) {
        assertEquals(
            mkMap(
                mkEntry("retention.bytes", Boolean.FALSE),
                mkEntry("min.insync.replicas", Boolean.FALSE),
                mkEntry("min.cleanable.dirty.ratio", Boolean.FALSE),
                mkEntry("confluent.tier.enable", Boolean.FALSE),
                mkEntry("confluent.key.schema.validation", Boolean.FALSE)),
            topicReadOnlyMap);
      } else {
        assertEquals(
            mkMap(
                mkEntry("retention.bytes", Boolean.FALSE),
                mkEntry("min.insync.replicas", Boolean.FALSE),
                mkEntry("min.cleanable.dirty.ratio", Boolean.TRUE)),
            topicReadOnlyMap);
      }

      Collection<DescribeConfigsResponse.ConfigEntry> interceptedBrokerConfigs =
              intercepted.configs().get(new ConfigResource(ConfigResource.Type.BROKER, "blah")).entries();
      Set<String> interceptedEntries = new HashSet<>();
      for (DescribeConfigsResponse.ConfigEntry configEntry : interceptedBrokerConfigs) {
        interceptedEntries.add(configEntry.name());
      }
      if (allowDescribeBrokerConfigs) {
        assertEquals(
            mkSet(
                "message.max.bytes",
                "num.network.threads",
                "broker.interceptor.class",
                "confluent.append.record.interceptor.classes"),
            interceptedEntries);
      } else {
        assertEquals(mkSet("message.max.bytes"), interceptedEntries);
      }
      verifyResponseMetrics(ApiKeys.DESCRIBE_CONFIGS, Errors.NONE);
    }
  }

  @Test
  public void testAlterConfigsRequest() {
    for (short ver = ApiKeys.ALTER_CONFIGS.oldestVersion(); ver <= ApiKeys.ALTER_CONFIGS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.ALTER_CONFIGS, ver);
      Map<ConfigResource, AlterConfigsRequest.Config> resourceConfigs = new HashMap<>();

      HashSet<AlterConfigsRequest.ConfigEntry> configEntries = new HashSet<>();
      testConfigs().forEach(c -> configEntries.add(new AlterConfigsRequest.ConfigEntry(c.name(), c.value())));

      resourceConfigs.put(new ConfigResource(ConfigResource.Type.TOPIC, "foo"),
          new AlterConfigsRequest.Config(configEntries));
      resourceConfigs.put(new ConfigResource(ConfigResource.Type.BROKER, "blah"), new AlterConfigsRequest.Config(
          Collections.<AlterConfigsRequest.ConfigEntry>emptyList()));
      resourceConfigs.put(new ConfigResource(ConfigResource.Type.TOPIC, "bar"), new AlterConfigsRequest.Config(
          Collections.<AlterConfigsRequest.ConfigEntry>emptyList()));
      AlterConfigsRequest inbound = new AlterConfigsRequest.Builder(resourceConfigs, false).build(ver);
      AlterConfigsRequest intercepted = (AlterConfigsRequest) parseRequest(context, inbound);
      assertEquals(mkSet(new ConfigResource(ConfigResource.Type.TOPIC, "tenant_foo"),
          new ConfigResource(ConfigResource.Type.BROKER, "blah"),
          new ConfigResource(ConfigResource.Type.TOPIC, "tenant_bar")), intercepted.configs().keySet());

      HashMap<String, String> expected = new HashMap<>();
      transformedTestConfigs().forEach(c -> expected.put(c.name(), c.value()));

      HashMap<String, String> actual = new HashMap<>();
      intercepted.configs()
              .get(new ConfigResource(ConfigResource.Type.TOPIC, "tenant_foo")).entries()
              .forEach(c -> actual.put(c.name(), c.value()));

      // Configs should be transformed by removing non-updateable configs, except for min.insync.replicas
      assertEquals(expected, actual);

      verifyRequestMetrics(ApiKeys.ALTER_CONFIGS);
    }
  }

  @Test
  public void testAlterConfigsResponse() throws IOException {
    for (short ver = ApiKeys.ALTER_CONFIGS.oldestVersion(); ver <= ApiKeys.ALTER_CONFIGS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.ALTER_CONFIGS, ver);
      Map<ConfigResource, ApiError> resourceErrors = new HashMap<>();
      resourceErrors.put(new ConfigResource(ConfigResource.Type.TOPIC, "tenant_foo"), new ApiError(Errors.NONE, ""));
      resourceErrors.put(new ConfigResource(ConfigResource.Type.BROKER, "blah"), new ApiError(Errors.NONE, ""));
      resourceErrors.put(new ConfigResource(ConfigResource.Type.TOPIC, "tenant_bar"), new ApiError(Errors.NONE, ""));
      AlterConfigsResponse outbound = new AlterConfigsResponse(0, resourceErrors);
      Struct struct = parseResponse(ApiKeys.ALTER_CONFIGS, ver, context.buildResponse(outbound));
      AlterConfigsResponse intercepted = new AlterConfigsResponse(struct);
      assertEquals(mkSet(new ConfigResource(ConfigResource.Type.TOPIC, "foo"),
          new ConfigResource(ConfigResource.Type.BROKER, "blah"),
          new ConfigResource(ConfigResource.Type.TOPIC, "bar")), intercepted.errors().keySet());
      verifyResponseMetrics(ApiKeys.ALTER_CONFIGS, Errors.NONE);
    }
  }

  @Test
  public void testRequestResponseMetrics() throws Exception {
    int minSleepTimeMs = 1;
    int maxSleepTimeMs = 3;
    for (int i = 0; i < 2; i++) {
      short ver = ApiKeys.FETCH.latestVersion();
      MultiTenantRequestContext context = newRequestContext(ApiKeys.FETCH, ver);
      LinkedHashMap<TopicPartition, FetchRequest.PartitionData> partitions = new LinkedHashMap<>();
      partitions.put(new TopicPartition("foo", 0), new FetchRequest.PartitionData(0L, -1, 1, Optional.empty()));

      FetchRequest inbound = FetchRequest.Builder.forConsumer(0, 0, partitions).build(ver);
      FetchRequest intercepted = (FetchRequest) parseRequest(context, inbound);

      AbstractResponse outbound = intercepted.getErrorResponse(new NotLeaderForPartitionException());
      time.sleep(i == 0 ? minSleepTimeMs : maxSleepTimeMs);
      parseResponse(ApiKeys.FETCH, ver, context.buildResponse(outbound));
    }

    Map<String, KafkaMetric> metrics = verifyRequestAndResponseMetrics(ApiKeys.FETCH, Errors.NOT_LEADER_FOR_PARTITION);
    assertEquals(minSleepTimeMs, (double) metrics.get("response-time-ns-min").metricValue() / 1000000, 0.001);
    assertEquals(maxSleepTimeMs, (double) metrics.get("response-time-ns-max").metricValue() / 1000000, 0.001);
    Set<Sensor> sensors = verifySensors(ApiKeys.FETCH, Errors.NOT_LEADER_FOR_PARTITION);
    time.sleep(ApiSensorBuilder.EXPIRY_SECONDS * 1000 + 1);
    for (Sensor sensor : sensors)
      assertTrue("Sensor should have expired", sensor.hasExpired());
  }

  @Test
  public void testIncrementalAlterConfigsRequest() {
    for (short ver = ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(); ver <= ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.INCREMENTAL_ALTER_CONFIGS, ver);
      IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection resourceConfigs =
              new IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection();

      IncrementalAlterConfigsRequestData.AlterableConfigCollection configEntries =
              new IncrementalAlterConfigsRequestData.AlterableConfigCollection();
      testConfigs().forEach(c -> configEntries.add(
              new IncrementalAlterConfigsRequestData.AlterableConfig()
                      .setName(c.name())
                      .setValue(c.value())
                      .setConfigOperation((byte) 0)));

      resourceConfigs.add(new IncrementalAlterConfigsRequestData.AlterConfigsResource()
              .setResourceType(ConfigResource.Type.TOPIC.id())
              .setResourceName("foo")
              .setConfigs(configEntries));
      resourceConfigs.add(new IncrementalAlterConfigsRequestData.AlterConfigsResource()
              .setResourceType(ConfigResource.Type.BROKER.id())
              .setResourceName("blah")
              .setConfigs(new IncrementalAlterConfigsRequestData.AlterableConfigCollection()));
      resourceConfigs.add(new IncrementalAlterConfigsRequestData.AlterConfigsResource()
              .setResourceType(ConfigResource.Type.TOPIC.id())
              .setResourceName("bar")
              .setConfigs(new IncrementalAlterConfigsRequestData.AlterableConfigCollection()));
      IncrementalAlterConfigsRequest inbound = new IncrementalAlterConfigsRequest.Builder(
              new IncrementalAlterConfigsRequestData()
                      .setResources(resourceConfigs)
                      .setValidateOnly(false))
              .build(ver);

      IncrementalAlterConfigsRequest actual = (IncrementalAlterConfigsRequest) parseRequest(context, inbound);

      IncrementalAlterConfigsRequestData.AlterableConfigCollection expectedConfigs =
              new IncrementalAlterConfigsRequestData.AlterableConfigCollection();
      transformedTestConfigs().forEach(c -> expectedConfigs.add(
              new IncrementalAlterConfigsRequestData.AlterableConfig()
                      .setName(c.name())
                      .setValue(c.value())
                      .setConfigOperation((byte) 0)));

      IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection expectedResources =
              new IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection();
      expectedResources.add(new IncrementalAlterConfigsRequestData.AlterConfigsResource()
              .setResourceType(ConfigResource.Type.TOPIC.id())
              .setResourceName("tenant_foo")
              .setConfigs(expectedConfigs));
      expectedResources.add(new IncrementalAlterConfigsRequestData.AlterConfigsResource()
              .setResourceType(ConfigResource.Type.BROKER.id())
              .setResourceName("blah")
              .setConfigs(new IncrementalAlterConfigsRequestData.AlterableConfigCollection()));
      expectedResources.add(new IncrementalAlterConfigsRequestData.AlterConfigsResource()
              .setResourceType(ConfigResource.Type.TOPIC.id())
              .setResourceName("tenant_bar")
              .setConfigs(new IncrementalAlterConfigsRequestData.AlterableConfigCollection()));
      IncrementalAlterConfigsRequest expected =
              new IncrementalAlterConfigsRequest.Builder(new IncrementalAlterConfigsRequestData()
                      .setResources(expectedResources)
                      .setValidateOnly(false))
              .build(ver);

      // Configs should be transformed by removing non-updateable configs, except for min.insync.replicas
      assertEquals(expected.data().resources().valuesSet(), actual.data().resources().valuesSet());

      // AlterConfigsResource only checks for name/type equality, so we need to extract the config values
      assertEquals(
              new HashSet<>(expected.data().resources().find(ConfigResource.Type.TOPIC.id(), "tenant_foo").configs()),
              new HashSet<>(actual.data().resources().find(ConfigResource.Type.TOPIC.id(), "tenant_foo").configs()));

      verifyRequestMetrics(ApiKeys.INCREMENTAL_ALTER_CONFIGS);
    }
  }

  @Test
  public void testIncrementalAlterConfigsResponse() throws IOException {
    for (short ver = ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(); ver <= ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion(); ver++) {
      MultiTenantRequestContext context = newRequestContext(ApiKeys.INCREMENTAL_ALTER_CONFIGS, ver);

      List<IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse> responses =
              new ArrayList<>();
      responses.add(new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
              .setResourceType(ConfigResource.Type.TOPIC.id())
              .setResourceName("tenant_foo")
              .setErrorCode(Errors.NONE.code())
              .setErrorMessage(""));
      responses.add(new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
              .setResourceType(ConfigResource.Type.BROKER.id())
              .setResourceName("blah")
              .setErrorCode(Errors.NONE.code())
              .setErrorMessage(""));
      responses.add(new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
              .setResourceType(ConfigResource.Type.TOPIC.id())
              .setResourceName("tenant_bar")
              .setErrorCode(Errors.NONE.code())
              .setErrorMessage(""));
      IncrementalAlterConfigsResponse outbound = new IncrementalAlterConfigsResponse(
              new IncrementalAlterConfigsResponseData()
                      .setResponses(responses));

      Struct struct = parseResponse(ApiKeys.INCREMENTAL_ALTER_CONFIGS, ver, context.buildResponse(outbound));
      IncrementalAlterConfigsResponse intercepted = new IncrementalAlterConfigsResponse(struct, ver);
      assertEquals(mkSet("foo", "blah", "bar"),
              intercepted.data().responses().stream()
                      .map(IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse::resourceName)
                      .collect(Collectors.toSet()));

      verifyResponseMetrics(ApiKeys.INCREMENTAL_ALTER_CONFIGS, Errors.NONE);
    }
  }

  private AbstractRequest parseRequest(MultiTenantRequestContext context, AbstractRequest request) {
    ByteBuffer requestBuffer = toByteBuffer(request);
    AbstractRequest parsed = context.parseRequest(requestBuffer).request;
    assertFalse(requestBuffer.hasRemaining());
    return parsed;
  }

  private MultiTenantRequestContext newRequestContext(ApiKeys api, short version) {
    RequestHeader header = new RequestHeader(api, version, "clientId", 23);
    return new MultiTenantRequestContext(header, "1", null, principal, listenerName,
        securityProtocol, ClientInformation.EMPTY, time, metrics, tenantMetrics, partitionAssignor);
  }

  private ByteBuffer toByteBuffer(AbstractRequest request) {
    Struct struct = RequestInternals.toStruct(request);
    ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
    struct.writeTo(buffer);
    buffer.flip();
    return buffer;
  }

  private Struct parseResponse(ApiKeys api, short version, Send send) throws IOException {
    ByteBufferChannel channel = new ByteBufferChannel(send.size());
    send.writeTo(channel);
    channel.close();
    ByteBuffer buffer = channel.buffer();
    buffer.getInt();
    ResponseHeader.parse(buffer, api.responseHeaderVersion(version));
    Struct struct = api.parseResponse(version, buffer.slice());
    assertEquals(buffer.remaining(), struct.sizeOf());
    return struct;
  }

  private Map<String, KafkaMetric> verifyRequestMetrics(ApiKeys apiKey) {
    return verifyTenantMetrics(apiKey, null, true, false,
            "request-byte-min", "request-byte-avg", "request-byte-max", "request-rate",
            "request-total", "request-byte-rate", "request-byte-total");
  }

  private void verifyResponseMetrics(ApiKeys apiKey, Errors error) {
    verifyTenantMetrics(apiKey, error, false, true,
        "response-time-ns-min", "response-time-ns-avg", "response-time-ns-max",
        "response-byte-min", "response-byte-avg", "response-byte-max",
        "response-byte-rate", "response-byte-total",
        "error-rate", "error-total");
  }

  private Map<String, KafkaMetric> verifyRequestAndResponseMetrics(ApiKeys apiKey, Errors error) {
    return verifyTenantMetrics(apiKey, error,
        true, true,
        "request-rate", "request-total",
        "request-byte-rate", "request-byte-total",
        "response-time-ns-min", "response-time-ns-avg", "response-time-ns-max",
        "response-byte-min", "response-byte-avg", "response-byte-max",
        "response-byte-rate", "response-byte-total",
        "error-rate", "error-total");
  }

  /**
   * Given a list of metric names, this method verifies that:
   *  every metric exists, has a tenant and user tag, has some non-default value
   *  and that Sensors associated with the metrics exist.
   *
   * @param expectedMetrics the name of the metrics that this tenant must have.
   * @return A map of KafkaMetric instances accessible by their name. e.g { "produced-bytes": KafkaMetric(...) }.
   *         Only contains the metrics in the expectedMetrics argument
   */
  private Map<String, KafkaMetric> verifyTenantMetrics(ApiKeys apiKey, Errors error, boolean hasRequests, boolean hasResponses, String... expectedMetrics) {
    Set<String> tenantMetrics = new HashSet<>();
    Map<String, KafkaMetric> metricsByName = new HashMap<>();
    List<String> expectedMetricsList = asList(expectedMetrics);
    for (Map.Entry<MetricName, KafkaMetric> entry : metrics.metrics().entrySet()) {
      MetricName metricName = entry.getKey();
      String tenant = metricName.tags().get("tenant");
      boolean toIgnore = tenant == null
              || (!hasRequests && metricName.name().startsWith("request"))
              || (!hasResponses && metricName.name().startsWith("response"))
              || !expectedMetricsList.contains(metricName.name());
      if (toIgnore) {
        continue;
      }
      KafkaMetric metric = entry.getValue();
      metricsByName.put(metricName.name(), metric);
      tenantMetrics.add(metricName.name());
      assertEquals("tenant", tenant);
      assertEquals("user", metricName.tags().get("user"));
      assertEquals(apiKey.name, metricName.tags().get("request"));
      double value = (Double) metric.metricValue();
      if (metricName.name().contains("time-"))
        assertTrue("Invalid metric value " + value, value >= 0.0);
      else
        assertTrue(String.format("Metric (%s) not recorded: %s", metricName.name(), value), value > 0.0);
      if (metricName.name().startsWith("error"))
        assertEquals(error.name(), metricName.tags().get("error"));
    }
    assertEquals(mkSet(expectedMetrics), tenantMetrics);

    verifySensors(apiKey, error, expectedMetrics);
    return metricsByName;
  }

  private Set<Sensor> verifySensors(ApiKeys apiKey, Errors error, String... expectedMetrics) {
    Set<Sensor> sensors = new HashSet<>();
    for (String metricName : expectedMetrics) {
      String name = metricName.substring(0, metricName.lastIndexOf('-')); // remove -rate/-total
      if (name.equals("error"))
        name += ":error-" + error.name();
      String sensorName = String.format("%s:request-%s:tenant-tenant:user-user", name, apiKey.name);
      Sensor sensor = metrics.getSensor(sensorName);
      assertNotNull("Sensor not found " + sensorName, sensor);
      sensors.add(sensor);
    }
    return sensors;
  }

  // Returns the test config map with compression.type stripped out
  private CreateableTopicConfigCollection transformedTestConfigs() {
    CreateableTopicConfigCollection transformedConfigs = testConfigs();
    transformedConfigs.remove(new CreateableTopicConfig().setName(TopicConfig.COMPRESSION_TYPE_CONFIG).setValue("lz4"));
    transformedConfigs.remove(new CreateableTopicConfig().setName(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG));
    return transformedConfigs;
  }

  // Gets a map of configs containing all modifiable configs, plus min.insync.replicas, plus
  // an unmodifiable config (compression.type)
  private CreateableTopicConfigCollection testConfigs() {
    CreateableTopicConfigCollection configs = new CreateableTopicConfigCollection();
    configs.add(new CreateableTopicConfig().setName(TopicConfig.CLEANUP_POLICY_CONFIG).setValue("compact"));
    configs.add(new CreateableTopicConfig().setName(TopicConfig.MAX_MESSAGE_BYTES_CONFIG).setValue("16777216"));
    configs.add(new CreateableTopicConfig().setName(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG).setValue("31536000000"));
    configs.add(new CreateableTopicConfig().setName(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG).setValue("LogAppendTime"));
    configs.add(new CreateableTopicConfig().setName(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG).setValue("0"));
    configs.add(new CreateableTopicConfig().setName(TopicConfig.RETENTION_BYTES_CONFIG).setValue("107374182400"));
    configs.add(new CreateableTopicConfig().setName(TopicConfig.RETENTION_MS_CONFIG).setValue("86400000"));
    configs.add(new CreateableTopicConfig().setName(TopicConfig.DELETE_RETENTION_MS_CONFIG).setValue("31536000000"));
    configs.add(new CreateableTopicConfig().setName(TopicConfig.SEGMENT_BYTES_CONFIG).setValue("1024"));
    configs.add(new CreateableTopicConfig().setName(TopicConfig.SEGMENT_MS_CONFIG).setValue("100"));
    configs.add(new CreateableTopicConfig().setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).setValue("3"));
    configs.add(new CreateableTopicConfig().setName(TopicConfig.COMPRESSION_TYPE_CONFIG).setValue("lz4"));
    configs.add(new CreateableTopicConfig().setName(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG).setValue("{}"));

    return configs;
  }
}
