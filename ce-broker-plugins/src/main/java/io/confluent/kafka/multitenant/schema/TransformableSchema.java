// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.schema;

import io.confluent.kafka.multitenant.utils.Optional;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.ProtocolInternals;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;

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

}
