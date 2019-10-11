// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.schema;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.utils.Optional;
import kafka.security.auth.Acl$;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.CommonFields;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;

import java.util.EnumMap;

/**
 * A cache of {@link TransformableSchema} for Kafka APIs which apply the multi-tenant
 * transformations needed to enforce tenant isolation. In particular, topic names, consumer groups,
 * and producer transactional ids are prefixed with the tenant name on request deserialization.
 * On response serialization, this prefix is stripped off.
 */
public class MultiTenantApis {

  private static final EnumMap<ApiKeys, TransformableType<TenantContext>[]> REQUEST_SCHEMAS =
      new EnumMap<>(ApiKeys.class);
  private static final EnumMap<ApiKeys, TransformableType<TenantContext>[]> RESPONSE_SCHEMAS =
      new EnumMap<>(ApiKeys.class);

  static {
    for (ApiKeys api : ApiKeys.values()) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      TransformableType<TenantContext>[] apiRequestSchemas =
          new TransformableType[api.latestVersion() + 1];
      @SuppressWarnings({"unchecked", "rawtypes"})
      TransformableType<TenantContext>[] apiResponseSchemas =
          new TransformableType[api.latestVersion() + 1];
      TenantRequestSchemaSelector requestFieldSelector = new TenantRequestSchemaSelector(api);
      TenantResponseSchemaSelector responseSchemaSelector = new TenantResponseSchemaSelector(api);

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

  /**
   * Check whether an API is supported. Internal broker and ACL APIs are not allowed through the
   * interceptor. There is no reason to expose the former and the latter requires finer-grained
   * authorization handling than is currently possible. In particular, we need to restrict the
   * resource types that the user is allowed to modify, but this cannot be done through an ACL
   * (the CreateAcl API is only authorized by Alter(Cluster)), and cannot be done through the
   * interceptor itself unless we're willing to fail the full request.
   */
  public static boolean isApiAllowed(ApiKeys api) {
    switch (api) {
      case PRODUCE:
      case FETCH:
      case LIST_OFFSETS:
      case METADATA:
      case OFFSET_COMMIT:
      case OFFSET_FETCH:
      case FIND_COORDINATOR:
      case JOIN_GROUP:
      case HEARTBEAT:
      case LEAVE_GROUP:
      case SYNC_GROUP:
      case DESCRIBE_GROUPS:
      case LIST_GROUPS:
      case DELETE_GROUPS:
      case SASL_HANDSHAKE:
      case API_VERSIONS:
      case CREATE_TOPICS:
      case DELETE_TOPICS:
      case DELETE_RECORDS:
      case INIT_PRODUCER_ID:
      case ADD_PARTITIONS_TO_TXN:
      case ADD_OFFSETS_TO_TXN:
      case END_TXN:
      case TXN_OFFSET_COMMIT:
      case DESCRIBE_CONFIGS:
      case ALTER_CONFIGS:
      case INCREMENTAL_ALTER_CONFIGS:
      case SASL_AUTHENTICATE:
      case CREATE_ACLS:
      case DESCRIBE_ACLS:
      case DELETE_ACLS:
      case CREATE_PARTITIONS:
      case OFFSET_FOR_LEADER_EPOCH:
      case REPLICA_STATUS:
        return true;

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
      case CONFLUENT_LEADER_AND_ISR:
      case TIER_LIST_OFFSET:
      case LIST_PARTITION_REASSIGNMENTS:
      case ALTER_PARTITION_REASSIGNMENTS:
      // Temporarily disabled until we verify that it's OK to enable
      case OFFSET_DELETE:
        return false;

      default:
        throw new IllegalArgumentException("Unexpected api key " + api);
    }
  }

  public static TransformableType<TenantContext> requestSchema(ApiKeys api, short version) {
    return REQUEST_SCHEMAS.get(api)[version];
  }

  public static TransformableType<TenantContext> responseSchema(ApiKeys api, short version) {
    return RESPONSE_SCHEMAS.get(api)[version];
  }

  private static TenantContext.ValueType commonTransformableType(Field field) {
    if (field != null) {
      if (field.name.equals(CommonFields.TOPIC_NAME.name)) {
        return TenantContext.ValueType.TOPIC;
      } else if (field.name.equals(CommonFields.GROUP_ID.name)) {
        return TenantContext.ValueType.GROUP;
      } else if (field.name.equals(CommonFields.TRANSACTIONAL_ID.name)) {
        return TenantContext.ValueType.TRANSACTIONAL_ID;
      }
    }
    return null;
  }

  private static void ensureStringType(Type type) {
    if (type != Type.STRING && type != Type.NULLABLE_STRING) {
      throw new IllegalArgumentException("Unexpected transform type " + type);
    }
  }

  private static void ensureArrayType(Type type) {
    if (!(type instanceof ArrayOf)) {
      throw new IllegalArgumentException("Unexpected transform type " + type);
    }
  }

  private static class TenantRequestSchemaSelector
      implements TransformableSchema.FieldSelector<TenantContext> {
    private final ApiKeys api;

    public TenantRequestSchemaSelector(ApiKeys api) {
      this.api = api;
    }

    @Override
    public Optional<TransformableType<TenantContext>> maybeAddTransformableType(
        Field field, Type type) {
      switch (api) {
        case METADATA:
          if (field != null && field.name.equals("name")) {
            return Optional.some(
                new StringTenantTransformer(type, TenantTransform.ADD_PREFIX));
          }
          break;

        case DELETE_TOPICS:
          if (field != null && field.name.equals("topic_names")) {
            return Optional.some(
                new ArrayTenantTransformer(type, TenantTransform.ADD_PREFIX));
          }
          break;

        case DESCRIBE_GROUPS:
          if (field != null && field.name.equals("groups")) {
            return Optional.some(
                new ArrayTenantTransformer(type, TenantTransform.ADD_PREFIX));
          }
          break;

        case DELETE_GROUPS:
          if (field != null && field.name.equals("groups_names")) {
            return Optional.some(
                new ArrayTenantTransformer(type, TenantTransform.ADD_PREFIX));
          }
          break;

        case FIND_COORDINATOR:
          if (field != null && field.name.equals("key")) {
            return Optional.some(
                new StringTenantTransformer(type, TenantTransform.ADD_PREFIX));
          }
          break;

        case OFFSET_COMMIT:
        case OFFSET_FETCH:
        case TXN_OFFSET_COMMIT:
          if (field != null) {
            if (field.name.equals("group_id")) {
              return Optional.some(new StringTenantTransformer(type,
                      TenantTransform.ADD_PREFIX));
            } else if (field.name.equals("name")) {
              return Optional.some(new StringTenantTransformer(type,
                      TenantTransform.ADD_PREFIX));
            }
          }
          break;

        case LEAVE_GROUP:
          // An explicit transformer is necessary because the relevant message type is
          // auto-generated from schema; it does not leverage CommonFields.GROUP_ID.
          if (field != null && field.name.equals("group_id")) {
            return Optional.some(
                    new StringTenantTransformer(type, TenantTransform.ADD_PREFIX));
          }
          break;

        case DESCRIBE_CONFIGS:
        case ALTER_CONFIGS:
        case INCREMENTAL_ALTER_CONFIGS:
          if (field != null && field.name.equals("resources") && type instanceof Schema) {
            return Optional.some(
                new ConfigResourceTenantTransformer(type, TenantTransform.ADD_PREFIX));
          }
          break;

        case CREATE_ACLS:
          if (field != null && field.name.equals("creations") && type instanceof Schema) {
            return Optional.some(
                new AclTenantTransformer(type, TenantTransform.ADD_PREFIX, false));
          }
          break;

        case DELETE_ACLS:
          if (field != null && field.name.equals("filters") && type instanceof Schema) {
            return Optional.some(
                new AclTenantTransformer(type, TenantTransform.ADD_PREFIX, false));
          }
          break;

        case DESCRIBE_ACLS:
          // The resource type and name are located in the root schema, which has no field
          if (field == null) {
            return Optional.some(
                new AclTenantTransformer(type, TenantTransform.ADD_PREFIX, false));
          }
          break;

        default:
          // fall through
      }

      TenantContext.ValueType valueType = commonTransformableType(field);
      if (valueType != null) {
        return Optional.some(
            new StringTenantTransformer(type, TenantTransform.ADD_PREFIX));
      }

      return Optional.none();
    }
  }

  private static class TenantResponseSchemaSelector
      implements TransformableSchema.FieldSelector<TenantContext> {
    private final ApiKeys api;

    public TenantResponseSchemaSelector(ApiKeys api) {
      this.api = api;
    }

    @Override
    public Optional<TransformableType<TenantContext>> maybeAddTransformableType(
        Field field, Type type) {
      switch (api) {
        case METADATA:
          if (field != null && field.name.equals("cluster_id")) {
            // Unlike the usual paths, the cluster id actually needs the tenant prefix
            // added in the response to ensure that each tenant sees a different id.
            return Optional.some(new ClusterIdSubstitution(type));
          }
          if (field != null && field.name.equals("name")) {
            return Optional.some(
                new StringTenantTransformer(type, TenantTransform.REMOVE_PREFIX));
          }

          break;

        case INCREMENTAL_ALTER_CONFIGS:
          if (field != null && field.name.equals("responses") && type instanceof Schema) {
            return Optional.some(
                    new ConfigResourceTenantTransformer(type, TenantTransform.REMOVE_PREFIX));
          }
          break;

        case ALTER_CONFIGS:
        case DESCRIBE_CONFIGS:
          if (field != null && field.name.equals("resources") && type instanceof Schema) {
            return Optional.some(
                new ConfigResourceTenantTransformer(type, TenantTransform.REMOVE_PREFIX));
          }
          break;

        case DELETE_ACLS:
          if (field != null && field.name.equals("matching_acls") && type instanceof Schema) {
            return Optional.some(
                new AclTenantTransformer(type, TenantTransform.REMOVE_PREFIX, false));
          }
          break;

        case DESCRIBE_ACLS:
          if (field != null && field.name.equals("resources") && type instanceof Schema) {
            return Optional.some(
                new AclTenantTransformer(type, TenantTransform.REMOVE_PREFIX, true));
          }
          break;

        case CREATE_TOPICS:
          if (field != null) {
            if (field.name.equals("error_message")) {
              return Optional.some(new ErrorMessageSanitizer(type));
            } else if (field.name.equals("name")) {
              return Optional.some(
                      new StringTenantTransformer(type, TenantTransform.REMOVE_PREFIX));
            }
          }
          break;

        case DELETE_TOPICS:
          if (field != null && field.name.equals("name")) {
            return Optional.some(
                    new StringTenantTransformer(type, TenantTransform.REMOVE_PREFIX));
          }
          break;

        case DESCRIBE_GROUPS:
          if ((field != null) && field.name.equals("group_id")) {
            return Optional.some(
                new StringTenantTransformer(type, TenantTransform.REMOVE_PREFIX));
          }
          break;

        case OFFSET_COMMIT:
        case OFFSET_FETCH:
        case TXN_OFFSET_COMMIT:
          if (field != null && field.name.equals("name")) {
              return Optional.some(new StringTenantTransformer(type,
                      TenantTransform.REMOVE_PREFIX));
          }
          break;

        default:
          // fall through
      }

      TenantContext.ValueType valueType = commonTransformableType(field);
      if (valueType != null) {
        return Optional.some(
            new StringTenantTransformer(type, TenantTransform.REMOVE_PREFIX));
      }

      if (field != null && field.name.equals(CommonFields.ERROR_MESSAGE.name)) {
        return Optional.some(new ErrorMessageSanitizer(type));
      }

      return Optional.none();
    }
  }

  private static class ClusterIdSubstitution extends AbstractTransformableType<TenantContext> {

    public ClusterIdSubstitution(Type type) {
      super(type);
      ensureStringType(type);
    }

    @Override
    public Object transform(Object value, TenantContext ctx) {
      return ctx.principal.tenantMetadata().clusterId;
    }

    @Override
    public int sizeOf(Object o, TenantContext ctx) {
      return type.sizeOf(ctx.principal.tenantMetadata().clusterId);
    }
  }

  private enum TenantTransform {
    ADD_PREFIX, REMOVE_PREFIX
  }

  private static class ErrorMessageSanitizer
      extends AbstractTransformableType<TenantContext> {
    ErrorMessageSanitizer(Type type) {
      super(type);
      ensureStringType(type);
    }

    @Override
    public Object transform(Object value, TenantContext ctx) {
      if (value == null) {
        return null;
      }
      return ctx.removeAllTenantPrefixes((String) value);
    }

    @Override
    public int sizeOf(Object value, TenantContext ctx) {
      if (value == null) {
        return type.sizeOf(null);
      }
      return type.sizeOf(value) - ctx.sizeOfRemovedPrefixes((String) value);
    }
  }

  private abstract static class AbstractTenantTransformer
      extends AbstractTransformableType<TenantContext> {
    protected final TenantTransform transform;

    AbstractTenantTransformer(Type type, TenantTransform transform) {
      super(type);
      this.transform = transform;
    }

    String transformString(String value, TenantContext ctx) {
      switch (transform) {
        case ADD_PREFIX:
          return ctx.addTenantPrefix(value);
        case REMOVE_PREFIX:
          return ctx.removeTenantPrefix(value);
        default:
          throw new IllegalArgumentException("Unhandled transform type " + transform);
      }
    }

    int sizeDelta(TenantContext ctx) {
      switch (transform) {
        case ADD_PREFIX:
          return ctx.prefixSizeInBytes;
        case REMOVE_PREFIX:
          return -ctx.prefixSizeInBytes;
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

  private static class StringTenantTransformer extends AbstractTenantTransformer {

    private StringTenantTransformer(Type fieldType, TenantTransform transform) {
      super(fieldType, transform);
      ensureStringType(fieldType);
    }

    @Override
    public Object transform(Object value, TenantContext ctx) {
      return value == null ? null : transformString((String) value, ctx);
    }

    @Override
    public int sizeOf(Object o, TenantContext ctx) {
      if (o == null) {
        return type.sizeOf(null);
      } else {
        return type.sizeOf(o) + sizeDelta(ctx);
      }
    }

  }

  private static class ArrayTenantTransformer extends AbstractTenantTransformer {

    private ArrayTenantTransformer(Type type, TenantTransform transform) {
      super(type, transform);
      ensureArrayType(type);
      ensureStringType(type.arrayElementType().get());
    }

    @Override
    public Object transform(Object value, TenantContext ctx) {
      Object[] array = (Object[]) value;
      if (array != null) {
        for (int i = 0; i < array.length; i++) {
          array[i] = transformString((String) array[i], ctx);
        }
      }
      return array;
    }

    @Override
    public int sizeOf(Object o, TenantContext ctx) {
      Object[] array = (Object[]) o;
      if (array == null) {
        return type.sizeOf(null);
      } else {
        return type.sizeOf(array) + array.length * sizeDelta(ctx);
      }
    }

  }

  private abstract static class ResourceTenantTransformer extends AbstractTenantTransformer {
    static final String RESOURCE_TYPE = "resource_type";
    static final String RESOURCE_NAME = "resource_name";

    private ResourceTenantTransformer(Type type, TenantTransform transform) {
      super(type, transform);

      if (!(type instanceof Schema)) {
        throw new IllegalArgumentException("Unexpected transform type " + type);
      }

      Schema schema = (Schema) type;
      ensureStructField(schema, RESOURCE_NAME);
      ensureStructField(schema, RESOURCE_TYPE);
    }

    @Override
    public Object transform(Object value, TenantContext ctx) {
      Struct struct = (Struct) value;
      if (prefixableResource(struct)) {
        String name = struct.getString(RESOURCE_NAME);
        struct.set(RESOURCE_NAME, name == null ? null : transformString(name, ctx));
      }
      return struct;
    }

    @Override
    public int sizeOf(Object value, TenantContext ctx) {
      int size = type.sizeOf(value);
      Struct struct = (Struct) value;
      if (prefixableResource(struct)) {
        String name = struct.getString(RESOURCE_NAME);
        if (name != null) {
          size += sizeDelta(ctx);
        }
      }
      return size;
    }

    abstract boolean prefixableResource(Struct struct);
  }

  private static class ConfigResourceTenantTransformer extends ResourceTenantTransformer {
    public ConfigResourceTenantTransformer(Type type, TenantTransform transform) {
      super(type, transform);
    }

    @Override
    boolean prefixableResource(Struct struct) {
      if (!struct.hasField(RESOURCE_TYPE)) {
        throw new IllegalArgumentException("Unexpected transform type " + struct);
      }

      ConfigResource.Type resourceType = ConfigResource.Type.forId(struct.getByte(RESOURCE_TYPE));
      switch (resourceType) {
        case TOPIC:
          return true;
        default:
          return false;
      }
    }
  }

  /**
   * ACLs contain:
   *   (resourceType, resourceName): Resource names are prefixed/unprefixed here
   *       using common code from ResourceTenantTransformer.
   *       Further transformation into prefixed form for wildcards are done separately
   *       since the transformation relies on creating requests of version 1 or higher.
   *   principal: These are transformed here from User:userId to TenantUser:clusterId_userId
   */
  private static class AclTenantTransformer extends ResourceTenantTransformer {
    private static final String PRINCIPAL = "principal";
    private static final String ACLS = "acls";
    private final boolean isDescribeResponse;

    public AclTenantTransformer(Type type, TenantTransform transform, boolean isDescribeResponse) {
      super(type, transform);
      this.isDescribeResponse = isDescribeResponse;
      Schema schema = (Schema) type;
      ensureStructField(schema, isDescribeResponse ? ACLS : PRINCIPAL);
    }

    @Override
    public Object transform(Object value, TenantContext ctx) {
      Struct struct = (Struct) super.transform(value, ctx);
      if (!isDescribeResponse) {
        String principal = struct.getString(PRINCIPAL);
        struct.set(PRINCIPAL, transformPrincipal(principal, ctx));
      } else {
        Object[] acls = struct.getArray(ACLS);
        if (acls != null) {
          for (Object acl : acls) {
            Struct aclStruct = (Struct) acl;
            String principal = aclStruct.getString(PRINCIPAL);
            aclStruct.set(PRINCIPAL, transformPrincipal(principal, ctx));
          }
        }
      }
      return struct;
    }

    @Override
    public int sizeOf(Object value, TenantContext ctx) {
      int size = super.sizeOf(value, ctx);
      Struct struct = (Struct) value;

      if (!isDescribeResponse) {
        String principal = struct.getString(PRINCIPAL);
        if (principal != null) {
          size += transformPrincipal(principal, ctx).length() - principal.length();
        }
      } else {
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
      }
      return size;
    }

    protected boolean prefixableResource(Struct struct) {
      if (!struct.hasField(RESOURCE_TYPE)) {
        throw new IllegalArgumentException("Unexpected transform type " + struct);
      }

      org.apache.kafka.common.resource.ResourceType resourceType =
          org.apache.kafka.common.resource.ResourceType.fromCode(struct.getByte(RESOURCE_TYPE));
      switch (resourceType) {
        case TOPIC:
        case GROUP:
        case TRANSACTIONAL_ID:
        case CLUSTER:
        case ANY:
          // Prefix if not null.
          return true;
        default:
          return false;
      }
    }

    String transformPrincipal(String principal, TenantContext ctx) {
      // since all resource names are prefixed, it is safe to match all principals in filters
      if (principal == null) {
        return null;
      }
      KafkaPrincipal kafkaPrincipal;
      try {
        kafkaPrincipal = SecurityUtils.parseKafkaPrincipal(principal);
      } catch (IllegalArgumentException e) {
        if (transform == TenantTransform.ADD_PREFIX) {
          // If this exception is propagated, it will be handled as a SchemaException,
          // causing the connection to be closed. So return untransformed invalid principal
          // and error response will be generated later in MultiTenantRequestContext.
          return principal;
        } else {
          throw e;
        }
      }
      switch (transform) {
        case ADD_PREFIX:
          if (kafkaPrincipal.equals(Acl$.MODULE$.WildCardPrincipal())) {
            return MultiTenantPrincipal.TENANT_WILDCARD_USER_TYPE + ":" + ctx.prefix();
          } else {
            String transformed = ctx.addTenantPrefix(kafkaPrincipal.getName());
            return MultiTenantPrincipal.TENANT_USER_TYPE + ":" + transformed;
          }
        case REMOVE_PREFIX:
          String user = kafkaPrincipal.getName();
          boolean tenantWildcard = kafkaPrincipal.getPrincipalType()
              .equals(MultiTenantPrincipal.TENANT_WILDCARD_USER_TYPE);
          if (user.equals("*")) {
            throw new IllegalStateException("Non-tenant ACLs have not been filtered out");
          } else if (tenantWildcard) {
            if (!user.equals(ctx.prefix())) {
              throw new IllegalStateException("Wildcard with different tenant not filtered out");
            } else {
              return Acl$.MODULE$.WildCardPrincipal().toString();
            }
          } else {
            String transformed = ctx.removeTenantPrefix(kafkaPrincipal.getName());
            return KafkaPrincipal.USER_TYPE + ":" + transformed;
          }
        default:
          throw new IllegalArgumentException("Unhandled transform type " + transform);
      }
    }
  }
}
