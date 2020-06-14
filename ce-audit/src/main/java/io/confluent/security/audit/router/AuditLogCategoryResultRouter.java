// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.security.audit.router;

import static io.confluent.security.audit.AuditLogUtils.AUTHENTICATION_EVENT_NAME;
import static io.confluent.security.audit.router.AuditLogRouter.SUPPRESSED;
import static org.apache.kafka.common.protocol.ApiKeys.ADD_OFFSETS_TO_TXN;
import static org.apache.kafka.common.protocol.ApiKeys.ADD_PARTITIONS_TO_TXN;
import static org.apache.kafka.common.protocol.ApiKeys.CONTROLLED_SHUTDOWN;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_ACLS;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_DELEGATION_TOKEN;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_GROUPS;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_LOG_DIRS;
import static org.apache.kafka.common.protocol.ApiKeys.END_TXN;
import static org.apache.kafka.common.protocol.ApiKeys.FIND_COORDINATOR;
import static org.apache.kafka.common.protocol.ApiKeys.HEARTBEAT;
import static org.apache.kafka.common.protocol.ApiKeys.INIT_PRODUCER_ID;
import static org.apache.kafka.common.protocol.ApiKeys.JOIN_GROUP;
import static org.apache.kafka.common.protocol.ApiKeys.LEADER_AND_ISR;
import static org.apache.kafka.common.protocol.ApiKeys.LEAVE_GROUP;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_GROUPS;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_OFFSETS;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_PARTITION_REASSIGNMENTS;
import static org.apache.kafka.common.protocol.ApiKeys.METADATA;
import static org.apache.kafka.common.protocol.ApiKeys.OFFSET_COMMIT;
import static org.apache.kafka.common.protocol.ApiKeys.OFFSET_FETCH;
import static org.apache.kafka.common.protocol.ApiKeys.OFFSET_FOR_LEADER_EPOCH;
import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.apache.kafka.common.protocol.ApiKeys.STOP_REPLICA;
import static org.apache.kafka.common.protocol.ApiKeys.SYNC_GROUP;
import static org.apache.kafka.common.protocol.ApiKeys.TXN_OFFSET_COMMIT;
import static org.apache.kafka.common.protocol.ApiKeys.UPDATE_METADATA;
import static org.apache.kafka.common.protocol.ApiKeys.WRITE_TXN_MARKERS;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.confluent.crn.ConfluentResourceName.Element;
import io.confluent.crn.CrnSyntaxException;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.AuditLogUtils;
import io.confluent.security.audit.AuthenticationInfo;
import io.confluent.security.authorizer.RequestContext;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.audit.AuditEventStatus;
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
  public static final String AUTHENTICATION_CATEGORY = "authentication";

  public static final Map<String, String> METHOD_CATEGORIES;
  static {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.<String,String>builder()
        //PRODUCE
        .put(entry(ADD_PARTITIONS_TO_TXN, PRODUCE_CATEGORY))
        .put(entry(END_TXN, PRODUCE_CATEGORY))
        .put(entry(INIT_PRODUCER_ID, PRODUCE_CATEGORY))
        .put(entry(PRODUCE, PRODUCE_CATEGORY))
        //CONSUME
        .put(entry(ADD_OFFSETS_TO_TXN, CONSUME_CATEGORY))
        .put(entry(JOIN_GROUP, CONSUME_CATEGORY))
        .put(entry(LEAVE_GROUP, CONSUME_CATEGORY))
        .put(entry(LIST_OFFSETS, CONSUME_CATEGORY))
        .put(entry(OFFSET_COMMIT, CONSUME_CATEGORY))
        .put(entry(OFFSET_FETCH, CONSUME_CATEGORY))
        .put(entry(SYNC_GROUP, CONSUME_CATEGORY))
        .put(entry(TXN_OFFSET_COMMIT, CONSUME_CATEGORY))
        //INTERBROKER
        .put(entry(CONTROLLED_SHUTDOWN, INTERBROKER_CATEGORY))
        .put(entry(LEADER_AND_ISR, INTERBROKER_CATEGORY))
        .put(entry(STOP_REPLICA, INTERBROKER_CATEGORY))
        .put(entry(UPDATE_METADATA, INTERBROKER_CATEGORY))
        .put(entry(WRITE_TXN_MARKERS, INTERBROKER_CATEGORY))
        //DESCRIBE
        .put(entry(DESCRIBE_ACLS, DESCRIBE_CATEGORY))
        .put(entry(DESCRIBE_CONFIGS, DESCRIBE_CATEGORY))
        .put(entry(DESCRIBE_DELEGATION_TOKEN, DESCRIBE_CATEGORY))
        .put(entry(DESCRIBE_GROUPS, DESCRIBE_CATEGORY))
        .put(entry(DESCRIBE_LOG_DIRS, DESCRIBE_CATEGORY))
        .put(entry(FIND_COORDINATOR, DESCRIBE_CATEGORY))
        .put(entry(LIST_GROUPS, DESCRIBE_CATEGORY))
        .put(entry(LIST_PARTITION_REASSIGNMENTS, DESCRIBE_CATEGORY))
        .put(entry(METADATA, DESCRIBE_CATEGORY))
        .put(entry(OFFSET_FOR_LEADER_EPOCH, DESCRIBE_CATEGORY))
        //HEARTBEAT
        .put(entry(HEARTBEAT, HEARTBEAT_CATEGORY))
        //AUTHENTICATION
        .put(AUTHENTICATION_EVENT_NAME, AUTHENTICATION_CATEGORY);
    for (RequestNameOverrides override : RequestNameOverrides.values()) {
      builder.put(entry(override));
    }
    METHOD_CATEGORIES = builder.build();
  }

  private static Map.Entry<String, String> entry(ApiKeys k, String category) {
    return Maps.immutableEntry(RequestContext.KAFKA + '.' + k.name, category);
  }

  private static Map.Entry<String, String> entry(RequestNameOverrides override) {
    return Maps.immutableEntry(override.prefix + override.name, override.category);
  }

  public enum RequestNameOverrides {
    MDS_AUTHORIZE(RequestContext.MDS, "Authorize", AUTHORIZE_CATEGORY),
    KAFKA_FETCH_CONSUMER(RequestContext.KAFKA, "FetchConsumer", CONSUME_CATEGORY),
    KAFKA_FETCH_FOLLOWER(RequestContext.KAFKA, "FetchFollower", INTERBROKER_CATEGORY);
    public final String prefix;
    public final String name;
    public final String category;
    RequestNameOverrides(String context, String name, String category) {
      this.prefix = context + ".";
      this.name = name;
      this.category = category;
    }
  }

  public static final Set<String> CATEGORIES;

  static {
    CATEGORIES = new HashSet<>(METHOD_CATEGORIES.values());
    CATEGORIES.add(OTHER_CATEGORY);
  }

  public static final Set<String> DEFAULT_ENABLED_CATEGORIES =
      Utils.mkSet(OTHER_CATEGORY, AUTHORIZE_CATEGORY, AUTHENTICATION_CATEGORY);

  private final HashMap<String, HashMap<AuditLogRouterResult, String>> routes = new HashMap<>();

  public AuditLogCategoryResultRouter setRoute(String category, AuditLogRouterResult result,
      String topic) {
    routes.computeIfAbsent(category, k -> new HashMap<>())
        .put(result, topic);
    return this;
  }

  private String category(AuditLogEntry entry) {
    return METHOD_CATEGORIES
        .getOrDefault(entry.getMethodName(), OTHER_CATEGORY);
  }

  private AuditLogRouterResult auditLogRouterResult(AuditLogEntry entry) {
    if (AUTHENTICATION_EVENT_NAME.equals(entry.getMethodName())) {
      String status = entry.getResult().getStatus();
      return AuditEventStatus.SUCCESS == AuditEventStatus.valueOf(status)
          ? AuditLogRouterResult.ALLOWED
          : AuditLogRouterResult.DENIED;
    } else {
      return entry.getAuthorizationInfo().getGranted()
          ? AuditLogRouterResult.ALLOWED
          : AuditLogRouterResult.DENIED;
    }
  }

  @Override
  public Optional<String> topic(AuditLogEntry auditLogEntry) {
    try {
      String category = category(auditLogEntry);
      if (!routes.containsKey(category)) {
        return Optional.empty();
      }
      AuditLogRouterResult result = auditLogRouterResult(auditLogEntry);
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
                  + "routed to a different topic.",
              auditLogEntry.getMethodName(), topic.get(), principal);
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
