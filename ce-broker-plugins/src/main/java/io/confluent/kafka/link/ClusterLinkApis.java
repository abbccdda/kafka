// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.kafka.link;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.schema.AbstractTransformableType;
import io.confluent.kafka.multitenant.schema.TenantContext;
import io.confluent.kafka.multitenant.schema.TenantContext.ValueType;
import io.confluent.kafka.multitenant.schema.TransformableSchema;
import io.confluent.kafka.multitenant.schema.TransformableType;
import io.confluent.kafka.multitenant.utils.Optional;
import java.util.EnumMap;
import kafka.security.authorizer.AclEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.CommonFields;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;

/**
 * A cache of {@link TransformableSchema} for Cluster Link requests to the source cluster which
 * apply multi-tenant transformations and in future may also be used to support topic renaming.
 * Only topics and ACLs are transformed.
 */
public class ClusterLinkApis {

  private static final EnumMap<ApiKeys, TransformableType<LinkContext>[]> REQUEST_SCHEMAS =
      new EnumMap<>(ApiKeys.class);
  private static final EnumMap<ApiKeys, TransformableType<LinkContext>[]> RESPONSE_SCHEMAS =
      new EnumMap<>(ApiKeys.class);

  static {
    for (ApiKeys api : ApiKeys.values()) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      TransformableType<LinkContext>[] apiRequestSchemas =
          new TransformableType[api.latestVersion() + 1];
      @SuppressWarnings({"unchecked", "rawtypes"})
      TransformableType<LinkContext>[] apiResponseSchemas =
          new TransformableType[api.latestVersion() + 1];
      LinkRequestSchemaSelector requestFieldSelector = new LinkRequestSchemaSelector(api);
      LinkResponseSchemaSelector responseSchemaSelector = new LinkResponseSchemaSelector(api);

      for (short version = api.oldestVersion(); version <= api.latestVersion(); version++) {
        apiRequestSchemas[version] = TransformableSchema.transformSchema(
            api.requestSchema(version), requestFieldSelector);
        apiResponseSchemas[version] = TransformableSchema.transformSchema(
            api.responseSchema(version), responseSchemaSelector);
      }

      REQUEST_SCHEMAS.put(api, apiRequestSchemas);
      RESPONSE_SCHEMAS.put(api, apiResponseSchemas);
    }
  }

  public static boolean isApiAllowed(ApiKeys api) {
    switch (api) {
      case FETCH:
      case LIST_OFFSETS:
      case METADATA:
      case OFFSET_FETCH:
      case FIND_COORDINATOR:
      case DESCRIBE_GROUPS:
      case LIST_GROUPS:
      case SASL_HANDSHAKE:
      case API_VERSIONS:
      case DESCRIBE_CONFIGS:
      case SASL_AUTHENTICATE:
      case DESCRIBE_ACLS:
      case OFFSET_FOR_LEADER_EPOCH:
      case REPLICA_STATUS:
        return true;

      // Allowed for multi-tenant, but not allowed for cluster link
      case PRODUCE:
      case OFFSET_COMMIT:
      case JOIN_GROUP:
      case HEARTBEAT:
      case LEAVE_GROUP:
      case SYNC_GROUP:
      case CREATE_TOPICS:
      case DELETE_TOPICS:
      case DELETE_GROUPS:
      case DELETE_RECORDS:
      case INIT_PRODUCER_ID:
      case ADD_PARTITIONS_TO_TXN:
      case ADD_OFFSETS_TO_TXN:
      case END_TXN:
      case TXN_OFFSET_COMMIT:
      case ALTER_CONFIGS:
      case INCREMENTAL_ALTER_CONFIGS:
      case CREATE_ACLS:
      case DELETE_ACLS:
      case CREATE_PARTITIONS:
      case OFFSET_DELETE:
      case CREATE_CLUSTER_LINKS:
      case LIST_CLUSTER_LINKS:
      case DELETE_CLUSTER_LINKS:

      // Not allowed for multi-tenant
      case CONTROLLED_SHUTDOWN:
      case LEADER_AND_ISR:
      case UPDATE_METADATA:
      case STOP_REPLICA:
      case WRITE_TXN_MARKERS:
      case ALTER_REPLICA_LOG_DIRS:
      case DESCRIBE_LOG_DIRS:
      case CREATE_DELEGATION_TOKEN:
      case DESCRIBE_DELEGATION_TOKEN:
      case RENEW_DELEGATION_TOKEN:
      case EXPIRE_DELEGATION_TOKEN:
      case ELECT_LEADERS:
      case LIST_PARTITION_REASSIGNMENTS:
      case ALTER_PARTITION_REASSIGNMENTS:
      case DESCRIBE_CLIENT_QUOTAS:
      case ALTER_CLIENT_QUOTAS:
      case REMOVE_BROKERS:
        return false;

      default:
        throw new IllegalArgumentException("Unexpected api key " + api);
    }
  }

  public static TransformableType<LinkContext> requestSchema(ApiKeys api, short version) {
    return REQUEST_SCHEMAS.get(api)[version];
  }

  public static TransformableType<LinkContext> responseSchema(ApiKeys api, short version) {
    return RESPONSE_SCHEMAS.get(api)[version];
  }

  private static ValueType commonTransformableType(Field field) {
    if (field != null) {
      if (field.name.equals(CommonFields.TOPIC_NAME.name)) {
        return TenantContext.ValueType.TOPIC;
      } else if (field.name.equals(CommonFields.GROUP_ID.name)) {
        return TenantContext.ValueType.GROUP;
      }
    }
    return null;
  }

  private static void ensureStringType(Type type) {
    if (type != Type.STRING && type != Type.NULLABLE_STRING &&
        type != Type.COMPACT_STRING && type != Type.COMPACT_NULLABLE_STRING) {
      throw new IllegalArgumentException("Unexpected transform type " + type);
    }
  }

  private static void ensureArrayType(Type type) {
    if (!type.isArray()) {
      throw new IllegalArgumentException("Unexpected transform type " + type);
    }
  }

  private static class LinkRequestSchemaSelector
      implements TransformableSchema.FieldSelector<LinkContext> {
    private final ApiKeys api;

    public LinkRequestSchemaSelector(ApiKeys api) {
      this.api = api;
    }

    @Override
    public Optional<TransformableType<LinkContext>> maybeAddTransformableType(
        Field field, Type type) {
      switch (api) {
        case METADATA:
          if (field != null && field.name.equals("name")) {
            return Optional.some(
                new StringLinkTransformer(type, LinkTransform.DEST_TO_SOURCE));
          }
          break;

        case DESCRIBE_GROUPS:
          if (field != null && field.name.equals("groups")) {
            return Optional.some(
                new ArrayLinkTransformer(type, LinkTransform.DEST_TO_SOURCE));
          }
          break;

        case FIND_COORDINATOR:
          if (field != null && field.name.equals("key")) {
            return Optional.some(
                new StringLinkTransformer(type, LinkTransform.DEST_TO_SOURCE));
          }
          break;

        case OFFSET_FETCH:
          if (field != null) {
            if (field.name.equals("group_id")) {
              return Optional.some(new StringLinkTransformer(type,
                      LinkTransform.DEST_TO_SOURCE));
            } else if (field.name.equals("name")) {
              return Optional.some(new StringLinkTransformer(type,
                      LinkTransform.DEST_TO_SOURCE));
            }
          }
          break;

        case DESCRIBE_CONFIGS:
          if (field != null && field.name.equals("resources") && type instanceof Schema) {
            return Optional.some(
                new ConfigResourceLinkTransformer(type, LinkTransform.DEST_TO_SOURCE));
          }
          break;

        case DESCRIBE_ACLS:
          // Describe ACLS requests are not transformed since they are generated using
          // filters that are not prefixed
          break;

        default:
          // fall through
      }

      TenantContext.ValueType valueType = commonTransformableType(field);
      if (valueType != null) {
        return Optional.some(
            new StringLinkTransformer(type, LinkTransform.DEST_TO_SOURCE));
      }

      return Optional.none();
    }
  }

  private static class LinkResponseSchemaSelector
      implements TransformableSchema.FieldSelector<LinkContext> {
    private final ApiKeys api;

    public LinkResponseSchemaSelector(ApiKeys api) {
      this.api = api;
    }

    @Override
    public Optional<TransformableType<LinkContext>> maybeAddTransformableType(
        Field field, Type type) {
      switch (api) {
        case METADATA:
          if (field != null && field.name.equals("name")) {
            return Optional.some(
                new StringLinkTransformer(type, LinkTransform.SOURCE_TO_DEST));
          }
          break;

        case DESCRIBE_CONFIGS:
          if (field != null && field.name.equals("resources") && type instanceof Schema) {
            return Optional.some(
                new ConfigResourceLinkTransformer(type, LinkTransform.SOURCE_TO_DEST));
          }
          break;

        case DESCRIBE_GROUPS:
          if ((field != null) && field.name.equals("group_id")) {
            return Optional.some(
                new StringLinkTransformer(type, LinkTransform.SOURCE_TO_DEST));
          }
          break;

        case OFFSET_FETCH:
          if (field != null && field.name.equals("name")) {
              return Optional.some(
                      new StringLinkTransformer(type, LinkTransform.SOURCE_TO_DEST));
          }
          break;

        case DESCRIBE_ACLS:
          if (field != null && field.name.equals("resources") && type instanceof Schema) {
            return Optional.some(
                new AclLinkTransformer(type, LinkTransform.SOURCE_TO_DEST, ApiKeys.DESCRIBE_ACLS));
          }
          break;

        default:
          // fall through
      }

      TenantContext.ValueType valueType = commonTransformableType(field);
      if (valueType != null) {
        return Optional.some(
            new StringLinkTransformer(type, LinkTransform.SOURCE_TO_DEST));
      }

      return Optional.none();
    }
  }

  private enum LinkTransform {
    DEST_TO_SOURCE, SOURCE_TO_DEST
  }

  private abstract static class AbstractLinkTransformer
      extends AbstractTransformableType<LinkContext> {
    protected final LinkTransform transform;

    AbstractLinkTransformer(Type type, LinkTransform transform) {
      super(type);
      this.transform = transform;
    }

    String transformString(String value, LinkContext ctx) {
      switch (transform) {
        case DEST_TO_SOURCE:
          return ctx.destToSource(value);
        case SOURCE_TO_DEST:
          return ctx.sourceToDest(value);
        default:
          throw new IllegalArgumentException("Unhandled transform type " + transform);
      }
    }

    int sizeDelta(LinkContext ctx) {
      switch (transform) {
        case DEST_TO_SOURCE:
          return ctx.destToSourceDeltaBytes();
        case SOURCE_TO_DEST:
          return -ctx.destToSourceDeltaBytes();
        default:
          throw new IllegalArgumentException("Unhandled transform type " + transform);
      }
    }

    protected void ensureStructField(Schema schema, String fieldName) {
      if (schema.get(fieldName) == null) {
        throw new IllegalArgumentException("Expected type " + schema + " to have "
            + fieldName + " field");
      }
    }
  }

  private static class StringLinkTransformer extends AbstractLinkTransformer {

    private StringLinkTransformer(Type fieldType, LinkTransform transform) {
      super(fieldType, transform);
      ensureStringType(fieldType);
    }

    @Override
    public Object transform(Object value, LinkContext ctx) {
      return value == null ? null : transformString((String) value, ctx);
    }

    @Override
    public int sizeOf(Object o, LinkContext ctx) {
      if (o == null) {
        return type.sizeOf(null);
      } else {
        return type.sizeOf(o) + sizeDelta(ctx);
      }
    }
  }

  private static class ArrayLinkTransformer extends AbstractLinkTransformer {

    private ArrayLinkTransformer(Type type, LinkTransform transform) {
      super(type, transform);
      ensureArrayType(type);
      ensureStringType(type.arrayElementType().get());
    }

    @Override
    public Object transform(Object value, LinkContext ctx) {
      Object[] array = (Object[]) value;
      if (array != null) {
        for (int i = 0; i < array.length; i++) {
          array[i] = transformString((String) array[i], ctx);
        }
      }
      return array;
    }

    @Override
    public int sizeOf(Object o, LinkContext ctx) {
      Object[] array = (Object[]) o;
      if (array == null) {
        return type.sizeOf(null);
      } else {
        return type.sizeOf(array) + array.length * sizeDelta(ctx);
      }
    }
  }

  private abstract static class ResourceLinkTransformer extends AbstractLinkTransformer {
    private static final String RESOURCE_TYPE = "resource_type";
    static final String RESOURCE_NAME = "resource_name";
    static final String FILTER_SUFFIX = "_filter";

    final String resourceTypeField;
    final String resourceNameField;

    private ResourceLinkTransformer(Type type, LinkTransform transform) {
      this(type, transform, false);
    }

    private ResourceLinkTransformer(Type type, LinkTransform transform, boolean isFilter) {
      super(type, transform);

      if (!(type instanceof Schema)) {
        throw new IllegalArgumentException("Unexpected transform type " + type);
      }

      Schema schema = (Schema) type;
      this.resourceTypeField = isFilter ? RESOURCE_TYPE + FILTER_SUFFIX : RESOURCE_TYPE;
      this.resourceNameField = isFilter ? RESOURCE_NAME + FILTER_SUFFIX : RESOURCE_NAME;
      ensureStructField(schema, resourceTypeField);
      ensureStructField(schema, resourceNameField);
    }

    @Override
    public Object transform(Object value, LinkContext ctx) {
      Struct struct = (Struct) value;
      if (prefixableResource(struct)) {
        String name = struct.getString(resourceNameField);
        struct.set(resourceNameField, name == null ? null : transformString(name, ctx));
      }
      return struct;
    }

    @Override
    public int sizeOf(Object value, LinkContext ctx) {
      int size = type.sizeOf(value);
      Struct struct = (Struct) value;
      if (prefixableResource(struct)) {
        String name = struct.getString(resourceNameField);
        if (name != null) {
          size += sizeDelta(ctx);
        }
      }
      return size;
    }

    abstract boolean prefixableResource(Struct struct);
  }

  private static class ConfigResourceLinkTransformer extends ResourceLinkTransformer {
    public ConfigResourceLinkTransformer(Type type, LinkTransform transform) {
      super(type, transform);
    }

    @Override
    boolean prefixableResource(Struct struct) {
      if (!struct.hasField(resourceTypeField)) {
        throw new IllegalArgumentException("Unexpected transform type " + struct);
      }

      ConfigResource.Type resourceType = ConfigResource.Type.forId(struct.getByte(resourceTypeField));
      switch (resourceType) {
        case TOPIC:
          return true;
        default:
          return false;
      }
    }
  }

  /**
   * ACL requests do not require transformation since we create ACL filters without prefix.
   * Responses are transformed to include tenant prefix.
   * ACLs contain:
   *   (resourceType, resourceName): Resource names are prefixed using common code from ResourceLinkTransformer.
   *   principal: These are transformed from User:userId to TenantUser:clusterId_userId
   *   Wildcard ACLs are transformed to prefixed ACLs: LITERAL:* becomes PREFIXED:clusterId_
   */
  private static class AclLinkTransformer extends ResourceLinkTransformer {
    private static final String PATTERN_TYPE = "pattern_type";
    private static final String PRINCIPAL = "principal";
    private static final String ACLS = "acls";

    public AclLinkTransformer(Type type, LinkTransform transform, ApiKeys apiKey) {
      super(type, transform, isFilter(transform, apiKey));
      if (transform != LinkTransform.SOURCE_TO_DEST || apiKey != ApiKeys.DESCRIBE_ACLS)
        throw new IllegalArgumentException("Only describeACLs responses are transformed, got " + transform + " " + apiKey);
      Schema schema = (Schema) type;
      ensureStructField(schema, ACLS);
    }

    @Override
    public Object transform(Object value, LinkContext ctx) {
      Struct struct = (Struct) super.transform(value, ctx);
      ensurePatternType(struct.schema());

      PatternType patternType = PatternType.fromCode(struct.getByte(PATTERN_TYPE));
      String resourceName = struct.getString(RESOURCE_NAME);
      String wildcard = ctx.destPrefix() + ResourcePattern.WILDCARD_RESOURCE;
      if (patternType == PatternType.LITERAL && wildcard.equals(resourceName)) {
        struct.set(RESOURCE_NAME, ctx.destPrefix());
        struct.set(PATTERN_TYPE, PatternType.PREFIXED.code());
      }

      Object[] acls = struct.getArray(ACLS);
      if (acls != null) {
        for (Object acl : acls) {
          Struct aclStruct = (Struct) acl;
          String principal = aclStruct.getString(PRINCIPAL);
          aclStruct.set(PRINCIPAL, transformPrincipal(principal, ctx));
        }
      }
      return struct;
    }

    @Override
    public int sizeOf(Object value, LinkContext ctx) {
      int size = super.sizeOf(value, ctx);
      Struct struct = (Struct) value;
      ensurePatternType(struct.schema());

      PatternType patternType = PatternType.fromCode(struct.getByte(PATTERN_TYPE));
      String resourceName = struct.getString(RESOURCE_NAME);
      String wildcard = ctx.destPrefix() + ResourcePattern.WILDCARD_RESOURCE;
      if (patternType == PatternType.LITERAL && wildcard.equals(resourceName)) {
        size += ctx.destPrefix().length() - 1;
      }

      Object[] acls = struct.getArray(ACLS);
      if (acls != null) {
        for (Object acl : acls) {
          Struct aclStruct = (Struct) acl;
          String principal = aclStruct.getString(PRINCIPAL);
          if (principal != null) {
            size += transformPrincipal(principal, ctx).length() - principal.length();
          }
        }
      }
      return size;
    }

    protected boolean prefixableResource(Struct struct) {
      if (!struct.hasField(resourceTypeField)) {
        throw new IllegalArgumentException("Unexpected transform type " + struct);
      }

      org.apache.kafka.common.resource.ResourceType resourceType =
          org.apache.kafka.common.resource.ResourceType.fromCode(struct.getByte(resourceTypeField));
      switch (resourceType) {
        case TOPIC:
        case GROUP:
        case TRANSACTIONAL_ID:
        case CLUSTER:
        case ANY:
          return true;
        default:
          return false;
      }
    }

    String transformPrincipal(String principal, LinkContext ctx) {
      if (principal == null) {
        return null;
      }
      KafkaPrincipal kafkaPrincipal = SecurityUtils.parseKafkaPrincipal(principal);
      switch (transform) {
        case SOURCE_TO_DEST:
          if (kafkaPrincipal.equals(AclEntry.WildcardPrincipal())) {
            return MultiTenantPrincipal.TENANT_WILDCARD_USER_TYPE + ":" + ctx.destPrefix();
          } else {
            String transformed = ctx.sourceToDest(kafkaPrincipal.getName());
            return MultiTenantPrincipal.TENANT_USER_TYPE + ":" + transformed;
          }
        case DEST_TO_SOURCE:
          return principal;
        default:
          throw new IllegalArgumentException("Unhandled transform type " + transform);
      }
    }

    private static boolean isFilter(LinkTransform transform, ApiKeys apiKey) {
      return transform == LinkTransform.DEST_TO_SOURCE && apiKey == ApiKeys.DESCRIBE_ACLS;
    }

    private void ensurePatternType(Schema schema) {
      if (schema.get(PATTERN_TYPE) == null) {
        throw new IllegalStateException("Expected type " + schema + " to have " + PATTERN_TYPE + " field");
      }
    }
  }
}
