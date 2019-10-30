package io.confluent.security.audit.router;

import static io.confluent.security.audit.router.AuditLogRouter.SUPPRESSED;

import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.crn.ConfluentResourceName.Element;
import io.confluent.crn.CrnSyntaxException;
import io.confluent.events.CloudEventUtils;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.AuditLogUtils;
import io.confluent.security.audit.AuthenticationInfo;
import io.confluent.security.authorizer.AuthorizeResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditLogCategoryResultRouter implements Router {

  private static final Logger log = LoggerFactory.getLogger(AuditLogCategoryResultRouter.class);

  public static final String OTHER_CATEGORY = "other";
  public static final String PRODUCE_CATEGORY = "produce";
  public static final String CONSUME_CATEGORY = "consume";
  public static final String INTERBROKER_CATEGORY = "interbroker";
  public static final String AUTHORIZE_CATEGORY = "authorize";
  public static final String DESCRIBE_CATEGORY = "describe";
  public static final String HEARTBEAT_CATEGORY = "heartbeat";

  public static final Map<String, String> METHOD_CATEGORIES = Utils.mkMap(
      Utils.mkEntry("kafka.AddPartitionToTxn", PRODUCE_CATEGORY),
      Utils.mkEntry("kafka.EndTxn", PRODUCE_CATEGORY),
      Utils.mkEntry("kafka.InitProducerId", PRODUCE_CATEGORY),
      Utils.mkEntry("kafka.Produce", PRODUCE_CATEGORY),

      Utils.mkEntry("kafka.AddOffsetsToTxn", CONSUME_CATEGORY),
      Utils.mkEntry("kafka.FetchConsumer", CONSUME_CATEGORY),
      Utils.mkEntry("kafka.JoinGroup", CONSUME_CATEGORY),
      Utils.mkEntry("kafka.LeaveGroup", CONSUME_CATEGORY),
      Utils.mkEntry("kafka.ListOffsets", CONSUME_CATEGORY),
      Utils.mkEntry("kafka.OffsetCommit", CONSUME_CATEGORY),
      Utils.mkEntry("kafka.OffsetFetch", CONSUME_CATEGORY),
      Utils.mkEntry("kafka.SyncGroup", CONSUME_CATEGORY),
      Utils.mkEntry("kafka.TxnOffsetCommit", CONSUME_CATEGORY),

      Utils.mkEntry("kafka.ControlledShutdown", INTERBROKER_CATEGORY),
      Utils.mkEntry("kafka.FetchFollower", INTERBROKER_CATEGORY),
      Utils.mkEntry("kafka.LeaderAndIsr", INTERBROKER_CATEGORY),
      Utils.mkEntry("kafka.StopReplica", INTERBROKER_CATEGORY),
      Utils.mkEntry("kafka.UpdateMetadata", INTERBROKER_CATEGORY),
      Utils.mkEntry("kafka.WriteTxnMarkers", INTERBROKER_CATEGORY),

      Utils.mkEntry("mds.Authorize", AUTHORIZE_CATEGORY),

      Utils.mkEntry("kafka.DescribeAcls", DESCRIBE_CATEGORY),
      Utils.mkEntry("kafka.DescribeConfigs", DESCRIBE_CATEGORY),
      Utils.mkEntry("kafka.DescribeDelegationToken", DESCRIBE_CATEGORY),
      Utils.mkEntry("kafka.DescribeGroups", DESCRIBE_CATEGORY),
      Utils.mkEntry("kafka.DescribeLogDirs", DESCRIBE_CATEGORY),
      Utils.mkEntry("kafka.DescribeToken", DESCRIBE_CATEGORY),
      Utils.mkEntry("kafka.FindCoordinator", DESCRIBE_CATEGORY),
      Utils.mkEntry("kafka.ListGroups", DESCRIBE_CATEGORY),
      Utils.mkEntry("kafka.ListPartitionReassignments", DESCRIBE_CATEGORY),
      Utils.mkEntry("kafka.Metadata", DESCRIBE_CATEGORY),
      Utils.mkEntry("kafka.OffsetForLeaderEpoch", DESCRIBE_CATEGORY),

      Utils.mkEntry("kafka.Heartbeat", HEARTBEAT_CATEGORY)
  );
  public static final Set<String> CATEGORIES;

  static {
    CATEGORIES = new HashSet<>(METHOD_CATEGORIES.values());
    CATEGORIES.add(OTHER_CATEGORY);
  }

  public static final Set<String> DEFAULT_ENABLED_CATEGORIES =
      Utils.mkSet(OTHER_CATEGORY, AUTHORIZE_CATEGORY);

  private final HashMap<String, HashMap<AuthorizeResult, String>> routes = new HashMap<>();

  public AuditLogCategoryResultRouter setRoute(String category, AuthorizeResult result,
      String topic) {
    routes.computeIfAbsent(category, k -> new HashMap<>())
        .put(result, topic);
    return this;
  }

  private String category(AuditLogEntry entry) {
    return METHOD_CATEGORIES
        .getOrDefault(entry.getMethodName(), OTHER_CATEGORY);
  }

  private AuthorizeResult authorizeResult(AuditLogEntry entry) {
    return entry.getAuthorizationInfo().getGranted()
        ? AuthorizeResult.ALLOWED
        : AuthorizeResult.DENIED;
  }

  @Override
  public Optional<String> topic(CloudEvent<AttributesImpl, AuditLogEntry> event) {
    try {
      AuditLogEntry auditLogEntry = event.getData().get();
      String category = category(auditLogEntry);
      if (!routes.containsKey(category)) {
        return Optional.empty();
      }
      AuthorizeResult result = authorizeResult(auditLogEntry);
      Optional<String> topic = Optional.ofNullable(routes.get(category).get(result));
      if (topic.isPresent() && !topic.get().isEmpty()
          && (CONSUME_CATEGORY.equals(category) || PRODUCE_CATEGORY.equals(category))) {
        /*
        Check for produce or consume logging on the same topic. This would create
        a loop.

        Note that there is still a class of loops that this check will not detect:
        Loops where consumption on audit log topic A results in a message on audit
        log topic B, which results in a message on audit log topic A, etc.
        Hopefully, if someone is going to the trouble of setting up such a cycle
        they've thought through the consequences. The documentation will contain a
        note about avoiding these cases.
         */
        Element resource = AuditLogUtils.resourceNameElement(auditLogEntry);
        if (resource.resourceType().equals("topic") &&
            resource.encodedResourceName().equals(topic.get())) {
          AuthenticationInfo info = auditLogEntry.getAuthenticationInfo();
          String principal = info == null ? "Unknown" : info.getPrincipal();
          log.error(
              "Audit log event for {} event on audit log topic {} was routed to "
                  + "same topic. This indicates that there may be a feedback loop. "
                  + "Principal {} should be excluded from audit logging or this event should be "
                  + "routed to a different topic. Event: {}",
              auditLogEntry.getMethodName(),
              topic.get(), principal, CloudEventUtils.toJsonString(event));
          return Optional.of(SUPPRESSED);
        }
      }
      return topic;
    } catch (CrnSyntaxException | NoSuchElementException e) {
      log.debug("Attempted to route a invalid AuditLogEntry", e);
      return Optional.empty();
    }
  }

  @Override
  public String toString() {
    return "AuditLogCategoryResultRouter(" + routes + ")";
  }
}
