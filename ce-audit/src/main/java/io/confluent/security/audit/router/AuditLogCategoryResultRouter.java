package io.confluent.security.audit.router;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.crn.ConfluentResourceName;
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
  public static final Map<String, String> METHOD_CATEGORIES = Utils.mkMap(
      Utils.mkEntry("Kafka.Produce", "produce"),
      Utils.mkEntry("Kafka.AddPartitionToTxn", "produce"),
      Utils.mkEntry("Kafka.FetchConsumer", "consume"),
      Utils.mkEntry("Kafka.OffsetCommit", "consume"),
      Utils.mkEntry("Kafka.AddOffsetsToTxn", "consume"),
      Utils.mkEntry("Kafka.TxnOffsetCommit", "consume"),
      Utils.mkEntry("Kafka.FetchFollower", "interbroker"),
      Utils.mkEntry("Kafka.LeaderAndIsr", "interbroker"),
      Utils.mkEntry("Kafka.StopReplica", "interbroker"),
      Utils.mkEntry("Kafka.UpdateMetadata", "interbroker"),
      Utils.mkEntry("Kafka.ControlledShutdown", "interbroker"),
      Utils.mkEntry("Kafka.WriteTxnMarkers", "interbroker"),
      Utils.mkEntry("Mds.Authorize", "authorize")
  );
  public static final Set<String> CATEGORIES;

  static {
    CATEGORIES = new HashSet<>(METHOD_CATEGORIES.values());
    CATEGORIES.add(OTHER_CATEGORY);
  }

  private final ConfluentResourceName pattern;
  private final HashMap<String, HashMap<AuthorizeResult, String>> routes = new HashMap<>();

  public AuditLogCategoryResultRouter(ConfluentResourceName pattern) {
    this.pattern = pattern;
  }

  public void setRoute(String category, AuthorizeResult result, String topic) {
    routes.computeIfAbsent(category, k -> new HashMap<AuthorizeResult, String>())
        .put(result, topic);
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
