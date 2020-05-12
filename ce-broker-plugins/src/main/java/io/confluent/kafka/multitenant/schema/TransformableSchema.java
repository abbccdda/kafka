// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.schema;

import io.confluent.kafka.multitenant.utils.Optional;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.ProtocolInternals;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.TaggedFields;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;
import org.apache.kafka.common.utils.ByteUtils;

/**
 * {@link TransformableSchema} provides a way to embed field transformations into an existing
 * {@link Schema}. By using {@link #buildSchema(Schema, FieldSelector)}, you can derive
 * a {@link TransformableSchema} which selectively embeds transformations that are applied
 * automatically during serialization through {@link #read(ByteBuffer, TransformContext)}
 * and {@link #write(ByteBuffer, Object, TransformContext)}. Note that the transformations must be
 * compatible with the original schema (transformations cannot change types).
 */
public class TransformableSchema<T extends TransformContext> implements TransformableType<T> {

  private final Schema origin;
  private final TransformableField<T>[] fields;

  public TransformableSchema(Schema origin, TransformableField<T>[] fields) {
    this.origin = origin;
    this.fields = fields;
  }

  /**
   * Transform and serialize an object to a buffer given the passed context.
   */
  @Override
  public void write(ByteBuffer buffer, Object o, T ctx) {
    Struct struct = (Struct) o;
    for (TransformableField<T> field : fields) {
      try {
        Object value = field.field.def.type.validate(struct.get(field.field));
        field.type.write(buffer, value, ctx);
      } catch (Exception e) {
        throw new SchemaException("Error writing field '" + field.field.def.name
            + "': " + (e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
      }
    }
  }

  /**
   * Read a schema from a buffer, applying transformations given the passed context. The result
   * is a {@link Struct} which references the original {@link Schema} that this
   * {@link TransformableSchema} instance was built from.
   */
  @Override
  public Struct read(ByteBuffer buffer, T ctx) {
    Object[] objects = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) {
      try {
        objects[i] = fields[i].type.read(buffer, ctx);
      } catch (ApiException e) {
        throw e;
      } catch (Exception e) {
        throw new SchemaException("Error reading field '" + fields[i].field.def.name
            + "': " + (e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
      }
    }
    return ProtocolInternals.newStruct(origin, objects);
  }

  /**
   * Get the serialized size in bytes of an object after transformations have been applied
   * given the passed context.
   */
  @Override
  public int sizeOf(Object o, T ctx) {
    Struct struct = (Struct) o;
    int size = 0;
    for (TransformableField<T> field : fields) {
      try {
        size += field.type.sizeOf(struct.get(field.field), ctx);
      } catch (Exception e) {
        throw new SchemaException("Error computing size for field '" + field.field.def.name
            + "': " + (e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
      }
    }
    return size;
  }

  /**
   * This interface is used to select the fields for transformation and to install the
   * transformer itself into the {@link TransformableSchema}.
   */
  public interface FieldSelector<T extends TransformContext> {

    /**
     * Optionally return a {@link TransformableType} if the field should be transformed.
     */
    Optional<TransformableType<T>> maybeAddTransformableType(Field field, Type type);
  }

  private static class TypeForwarder<T extends TransformContext> implements TransformableType<T> {
    private final Type type;

    public TypeForwarder(Type type) {
      this.type = type;
    }

    @Override
    public void write(ByteBuffer buffer, Object o, T ctx) {
      type.write(buffer, o);
    }

    @Override
    public Object read(ByteBuffer buffer, T ctx) {
      return type.read(buffer);
    }

    @Override
    public int sizeOf(Object o, T ctx) {
      return type.sizeOf(o);
    }
  }

  private static class TransformableField<T extends TransformContext> {
    final BoundField field;
    final TransformableType<T> type;

    private TransformableField(BoundField field, TransformableType<T> type) {
      this.field = field;
      this.type = type;
    }
  }

  public static <T extends TransformContext> TransformableType<T> transformSchema(
      Schema schema, FieldSelector<T> selector) {
    return buildTransformableType(null, schema, selector);
  }

  private static <T extends TransformContext> TransformableSchema<T> buildSchema(
      Schema schema, FieldSelector<T> selector) {
    BoundField[] fields = schema.fields();
    @SuppressWarnings({"unchecked", "rawtypes"})
    TransformableField<T>[] transformableFields = new TransformableField[fields.length];
    for (int i = 0; i < fields.length; i++) {
      BoundField field = fields[i];
      transformableFields[i] = new TransformableField<>(field, buildTransformableType(field.def,
          field.def.type, selector));
    }
    return new TransformableSchema<>(schema, transformableFields);
  }

  private static <T extends TransformContext> TransformableTaggedFields<T> buildTransformableTaggedFields(
      TaggedFields taggedFields, FieldSelector<T> selector) {
    Map<Integer, Field> fields = taggedFields.fields();
    @SuppressWarnings({"unchecked", "rawtypes"})
    Map<Integer, TransformableType<T>> transformableFields = new HashMap<>(fields.size());
    fields.forEach((tag, field) -> {
      TransformableType<T> transformableType = buildTransformableType(field, field.type, selector);
      transformableFields.put(tag, transformableType);
    });
    return new TransformableTaggedFields<T>(transformableFields);
  }

  private static <T extends TransformContext> TransformableType<T> buildTransformableType(
      Field field, Type type, FieldSelector<T> selector) {
    Optional<TransformableType<T>> optionalTransformableType =
        selector.maybeAddTransformableType(field, type);
    if (optionalTransformableType.isDefined()) {
      return optionalTransformableType.get();
    } else if (type instanceof Schema) {
      return buildSchema((Schema) type, selector);
    } else if (type instanceof ArrayOf) {
      return new TransformableArrayOf<>(buildTransformableType(field, type.arrayElementType().get(),
          selector), type.isNullable());
    } else if (type instanceof CompactArrayOf) {
      return new TransformableCompactArrayOf<>(buildTransformableType(field, type.arrayElementType().get(),
          selector), type.isNullable());
    } else if (type instanceof TaggedFields) {
      return buildTransformableTaggedFields((TaggedFields) type, selector);
    } else {
      return new TypeForwarder<>(type);
    }
  }

  private static class TransformableArrayOf<T extends TransformContext>
      implements TransformableType<T> {
    private final TransformableType<T> type;
    private final boolean nullable;

    public TransformableArrayOf(TransformableType<T> type, boolean nullable) {
      this.type = type;
      this.nullable = nullable;
    }

    @Override
    public void write(ByteBuffer buffer, Object o, T ctx) {
      if (o == null) {
        buffer.putInt(-1);
        return;
      }

      Object[] objs = (Object[]) o;
      int size = objs.length;
      buffer.putInt(size);

      for (Object obj : objs) {
        type.write(buffer, obj, ctx);
      }
    }

    @Override
    public Object read(ByteBuffer buffer, T ctx) {
      int size = buffer.getInt();
      if (size < 0 && nullable) {
        return null;
      } else if (size < 0) {
        throw new SchemaException("Array size " + size + " cannot be negative");
      }

      if (size > buffer.remaining()) {
        throw new SchemaException("Error reading array of size " + size + ", only "
            + buffer.remaining() + " bytes available");
      }
      Object[] objs = new Object[size];
      for (int i = 0; i < size; i++) {
        objs[i] = type.read(buffer, ctx);
      }
      return objs;
    }

    @Override
    public int sizeOf(Object o, T ctx) {
      int size = 4;
      if (o == null) {
        return size;
      }

      Object[] objs = (Object[]) o;
      for (Object obj : objs) {
        size += type.sizeOf(obj, ctx);
      }
      return size;
    }
  }

  private static class TransformableCompactArrayOf<T extends TransformContext>
      implements TransformableType<T> {
    private final TransformableType<T> type;
    private final boolean nullable;

    public TransformableCompactArrayOf(TransformableType<T> type, boolean nullable) {
      this.type = type;
      this.nullable = nullable;
    }

    @Override
    public void write(ByteBuffer buffer, Object o, T ctx) {
      if (o == null) {
        ByteUtils.writeUnsignedVarint(0, buffer);
        return;
      }
      Object[] objs = (Object[]) o;
      int size = objs.length;
      ByteUtils.writeUnsignedVarint(size + 1, buffer);

      for (Object obj : objs)
        type.write(buffer, obj, ctx);
    }

    @Override
    public Object read(ByteBuffer buffer, T ctx) {
      int n = ByteUtils.readUnsignedVarint(buffer);
      if (n == 0) {
        if (nullable) {
          return null;
        } else {
          throw new SchemaException("This array is not nullable.");
        }
      }
      int size = n - 1;
      if (size > buffer.remaining())
        throw new SchemaException("Error reading array of size " + size + ", only " + buffer.remaining() + " bytes available");
      Object[] objs = new Object[size];
      for (int i = 0; i < size; i++)
        objs[i] = type.read(buffer, ctx);
      return objs;
    }

    @Override
    public int sizeOf(Object o, T ctx) {
      if (o == null) {
        return 1;
      }
      Object[] objs = (Object[]) o;
      int size = ByteUtils.sizeOfUnsignedVarint(objs.length + 1);
      for (Object obj : objs) {
        size += type.sizeOf(obj, ctx);
      }
      return size;
    }

  }

  public static class TransformableTaggedFields<T extends TransformContext> implements TransformableType<T> {

    private final Map<Integer, TransformableType<T>> fields;

    public TransformableTaggedFields(Map<Integer, TransformableType<T>> fields) {
      this.fields = fields;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(ByteBuffer buffer, Object o, T ctx) {
      NavigableMap<Integer, Object> objects = (NavigableMap<Integer, Object>) o;
      ByteUtils.writeUnsignedVarint(objects.size(), buffer);
      for (Map.Entry<Integer, Object> entry : objects.entrySet()) {
        Integer tag = entry.getKey();
        ByteUtils.writeUnsignedVarint(tag, buffer);
        TransformableType<T> type = fields.get(tag);
        if (type == null) {
          RawTaggedField value = (RawTaggedField) entry.getValue();
          ByteUtils.writeUnsignedVarint(value.data().length, buffer);
          buffer.put(value.data());
        } else {
          ByteUtils.writeUnsignedVarint(type.sizeOf(entry.getValue(), ctx), buffer);
          type.write(buffer, entry.getValue(), ctx);
        }
      }
    }

    @Override
    public Object read(ByteBuffer buffer, T ctx) {
      int numTaggedFields = ByteUtils.readUnsignedVarint(buffer);
      if (numTaggedFields == 0) {
        return Collections.emptyNavigableMap();
      }
      NavigableMap<Integer, Object> objects = new TreeMap<>();
      int prevTag = -1;
      for (int i = 0; i < numTaggedFields; i++) {
        int tag = ByteUtils.readUnsignedVarint(buffer);
        if (tag <= prevTag) {
          throw new RuntimeException("Invalid or out-of-order tag " + tag);
        }
        prevTag = tag;
        int size = ByteUtils.readUnsignedVarint(buffer);
        TransformableType<T> type = fields.get(tag);
        if (type == null) {
          byte[] bytes = new byte[size];
          buffer.get(bytes);
          objects.put(tag, new RawTaggedField(tag, bytes));
        } else {
          objects.put(tag, type.read(buffer, ctx));
        }
      }
      return objects;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int sizeOf(Object o, T ctx) {
      int size = 0;
      NavigableMap<Integer, Object> objects = (NavigableMap<Integer, Object>) o;
      size += ByteUtils.sizeOfUnsignedVarint(objects.size());
      for (Map.Entry<Integer, Object> entry : objects.entrySet()) {
        Integer tag = entry.getKey();
        size += ByteUtils.sizeOfUnsignedVarint(tag);
        TransformableType<T> type = fields.get(tag);
        if (type == null) {
          RawTaggedField value = (RawTaggedField) entry.getValue();
          size += value.data().length + ByteUtils.sizeOfUnsignedVarint(value.data().length);
        } else {
          int valueSize = type.sizeOf(entry.getValue(), ctx);
          size += valueSize + ByteUtils.sizeOfUnsignedVarint(valueSize);
        }
      }
      return size;
    }
  }
}
