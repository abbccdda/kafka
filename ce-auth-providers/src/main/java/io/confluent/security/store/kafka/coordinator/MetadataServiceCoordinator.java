// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.coordinator;

import io.confluent.security.store.kafka.coordinator.MetadataServiceAssignment.AssignmentError;
import io.confluent.security.authorizer.utils.JsonMapper;
import java.net.URL;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

/**
 * Coordinator for the Metadata Service used to track active nodes
 * and elect master writer.
 */
public class MetadataServiceCoordinator extends AbstractCoordinator {

  private static final String PROTOCOL_TYPE = "metadata-service";
  public static final String PROTOCOL = "v0";

  private final Logger log;

  private final MetadataServiceRebalanceListener rebalanceListener;
  private final NodeMetadata nodeMetadata;
  private final AtomicBoolean isAlive;
  private MetadataServiceAssignment currentAssignment;

  public MetadataServiceCoordinator(LogContext logContext,
      ConsumerNetworkClient client,
      NodeMetadata nodeMetadata,
      ConsumerConfig consumerConfig,
      Metrics metrics,
      String metricGrpPrefix,
      Time time,
      MetadataServiceRebalanceListener rebalanceListener) {
    super(new GroupRebalanceConfig(consumerConfig, GroupRebalanceConfig.ProtocolType.CONSUMER),
            logContext,
            client,
            metrics,
            metricGrpPrefix,
            time);

    this.log = logContext.logger(MetadataServiceCoordinator.class);
    this.rebalanceListener = Objects.requireNonNull(rebalanceListener, "rebalanceListener");
    this.nodeMetadata = Objects.requireNonNull(nodeMetadata, "nodeMetadata");
    this.isAlive = new AtomicBoolean(true);
  }

  @Override
  protected String protocolType() {
    return PROTOCOL_TYPE;
  }

  @Override
  protected JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
    JoinGroupRequestData.JoinGroupRequestProtocolCollection protocolSet = new JoinGroupRequestData.JoinGroupRequestProtocolCollection();
    protocolSet.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName(PROTOCOL)
            .setMetadata(nodeMetadata.serialize().array()));
    return protocolSet;
  }

  @Override
  protected void onJoinPrepare(int generation, String memberId) {
    rebalanceListener.onRevoked(generation);
    this.currentAssignment = null;
  }

  @Override
  protected Map<String, ByteBuffer> performAssignment(String coordinationLeaderId,
                                                      String protocol,
                                                      List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata) {
    if (!PROTOCOL.equals(protocol))
      throw new IllegalArgumentException("Invalid protocol received for join complete");

    Map<String, NodeMetadata> allMembers = allMemberMetadata.stream()
        .collect(Collectors.toMap(e -> e.memberId(), e -> NodeMetadata.deserialize(ByteBuffer.wrap(e.metadata()))));
    log.debug("Perform assignment on leader {} members {}", coordinationLeaderId,
        allMemberMetadata);

    AssignmentError error = AssignmentError.NONE;
    Set<URL> memberUrls = new HashSet<>();
    for (NodeMetadata nodeMetadata : allMembers.values()) {
      if (nodeMetadata.urls().stream().anyMatch(memberUrls::contains)) {
        error = AssignmentError.DUPLICATE_URLS;
        log.error("Some members are using duplicate URL: {}. Every Metadata Service instance "
            + " must be configured with a unique URL.", allMembers);
        break;
      }
      memberUrls.addAll(nodeMetadata.urls());
    }

    Entry<String, NodeMetadata> writerEntry =
        Collections.min(allMembers.entrySet(), Comparator.comparing(Entry::getValue));
    String writerMemberId = writerEntry.getKey();
    NodeMetadata writerNodeMetdata = writerEntry.getValue();
    MetadataServiceAssignment newAssignment = new MetadataServiceAssignment(error.errorCode,
        allMembers,
        writerMemberId,
        writerNodeMetdata);

    log.debug("Node {} with memberId {} elected as writer", writerNodeMetdata, writerMemberId);
    return allMemberMetadata.stream()
        .collect(Collectors.toMap(e -> e.memberId(), e -> newAssignment.serialize()));
  }

  @Override
  protected void onJoinComplete(int generation,
                                String memberId,
                                String protocol,
                                ByteBuffer memberAssignment) {
    if (!PROTOCOL.equals(protocol))
      throw new IllegalArgumentException("Invalid protocol received for join complete");
    this.currentAssignment = JsonMapper.fromByteBuffer(memberAssignment, MetadataServiceAssignment.class);
    rebalanceListener.onAssigned(currentAssignment, generation);
  }

  void poll(Duration timeout) {
    long now = time.milliseconds();
    long endMs = now + timeout.toMillis();

    do {
      if (coordinatorUnknown()) {
        ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
        now = time.milliseconds();
      }

      if (rejoinNeededOrPending() && isAlive.get()) {
        ensureActiveGroup();
        now = time.milliseconds();
      }

      if (isAlive.get()) {
        pollHeartbeat(now);
        client.poll(time.timer(Math.min(Math.max(0, endMs - now), timeToNextHeartbeat(now))));
      }

      now = time.milliseconds();
    } while (now < endMs && isAlive.get());
  }

  @Override
  protected boolean rejoinNeededOrPending() {
    return super.rejoinNeededOrPending() || currentAssignment == null;
  }

  @Override
  protected void close(Timer timer) {
    this.isAlive.set(false);
    super.close(timer);
  }

  void onWriterResigned() {
    this.currentAssignment = null;
  }

  void wakeup() {
    client.wakeup();
  }
}
