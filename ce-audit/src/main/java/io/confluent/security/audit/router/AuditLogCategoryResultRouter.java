package io.confluent.security.audit.router;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.CloudEvent;
import io.confluent.security.authorizer.AuthorizeResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditLogCategoryResultRouter implements EventTopicRouter {

  private static final Logger log = LoggerFactory.getLogger(AuditLogCategoryResultRouter.class);

  public static final String OTHER_CATEGORY = "other";
  public static final String PRODUCE_CATEGORY = "produce";
  public static final String CONSUME_CATEGORY = "consume";
  public static final String INTERBROKER_CATEGORY = "interbroker";
  public static final String AUTHORIZE_CATEGORY = "authorize";
  public static final Map<String, String> METHOD_CATEGORIES = Utils.mkMap(
      Utils.mkEntry("Kafka.Produce", PRODUCE_CATEGORY),
      Utils.mkEntry("Kafka.AddPartitionToTxn", PRODUCE_CATEGORY),
      Utils.mkEntry("Kafka.FetchConsumer", CONSUME_CATEGORY),
      Utils.mkEntry("Kafka.OffsetCommit", CONSUME_CATEGORY),
      Utils.mkEntry("Kafka.AddOffsetsToTxn", CONSUME_CATEGORY),
      Utils.mkEntry("Kafka.TxnOffsetCommit", CONSUME_CATEGORY),
      Utils.mkEntry("Kafka.FetchFollower", INTERBROKER_CATEGORY),
      Utils.mkEntry("Kafka.LeaderAndIsr", INTERBROKER_CATEGORY),
      Utils.mkEntry("Kafka.StopReplica", INTERBROKER_CATEGORY),
      Utils.mkEntry("Kafka.UpdateMetadata", INTERBROKER_CATEGORY),
      Utils.mkEntry("Kafka.ControlledShutdown", INTERBROKER_CATEGORY),
      Utils.mkEntry("Kafka.WriteTxnMarkers", INTERBROKER_CATEGORY),
      Utils.mkEntry("Mds.Authorize", AUTHORIZE_CATEGORY)
  );
  public static final Set<String> CATEGORIES;

  static {
    CATEGORIES = new HashSet<>(METHOD_CATEGORIES.values());
    CATEGORIES.add(OTHER_CATEGORY);
  }

  private final HashMap<String, HashMap<AuthorizeResult, String>> routes = new HashMap<>();

  public AuditLogCategoryResultRouter setRoute(String category, AuthorizeResult result,
      String topic) {
    routes.computeIfAbsent(category, k -> new HashMap<>())
        .put(result, topic);
    return this;
  }

  @Override
  public Optional<String> topic(CloudEvent event) {
    try {
      AuditLogEntry auditLogEntry = event.getData().unpack(AuditLogEntry.class);
      String category = METHOD_CATEGORIES
          .getOrDefault(auditLogEntry.getMethodName(), OTHER_CATEGORY);
      if (!routes.containsKey(category)) {
        return Optional.empty();
      }
      AuthorizeResult result =
          auditLogEntry.getAuthorizationInfo().getGranted()
              ? AuthorizeResult.ALLOWED
              : AuthorizeResult.DENIED;
      return Optional.ofNullable(routes.get(category).get(result));
    } catch (InvalidProtocolBufferException e) {
      log.debug("Attempted to route a invalid AuditLogEntry", e);
      return Optional.empty();
    }
  }
}
