/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.http.server;

import static java.util.Objects.requireNonNull;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A binder that can be used to create a {@link KafkaHttpServerInjector} to be used by {@link
 * KafkaHttpServerInjector} for passing dependencies down to {@link KafkaHttpServer} at loading
 * time.
 *
 * <p><b>This class is for internal use of Confluent Server only. It should not be considered part
 * of its public interface. Changes to it can happen at any time without notice.</b>
 */
public final class KafkaHttpServerBinder {

  private final HashMap<BindingKey, Supplier<?>> bindings = new HashMap<>();

  /**
   * Binds {@code type} to the given {@code instance} if a binding for {@code type} does not exist
   * yet. Throws {@link IllegalArgumentException} otherwise.
   *
   * <p>Calling this method is equivalent to calling {@link #bindInstance(Class, Class, Object)
   * bindInstance(type, null, instance)}.
   */
  public synchronized <T> void bindInstance(Class<T> type, T instance) {
    bindInstance(type, /* annotation= */ null, instance);
  }

  /**
   * Binds {@code type}, annotated with {@code annotation}, to the given {@code instance} if a
   * binding for {@code (type, annotation)} does not exist yet. Throws {@link
   * IllegalArgumentException} otherwise.
   */
  public synchronized <T> void bindInstance(
      Class<T> type, Class<? extends Annotation> annotation, T instance) {
    bindSupplier(type, annotation, () -> instance);
  }

  /**
   * Binds {@code type} to the given {@code supplier} if a binding for {@code type} does not exist
   * yet. Throws {@link IllegalArgumentException} otherwise.
   *
   * <p>Calling this method is equivalent to calling {@link #bindSupplier(Class, Class, Supplier)
   * bindSupplier(type, null, supplier)}.
   */
  public synchronized <T> void bindSupplier(Class<T> type, Supplier<? extends T> supplier) {
    bindSupplier(type, /* annotation= */ null, supplier);
  }

  /**
   * Binds {@code type}, annotated with {@code annotation}, to the given {@code supplier} if a
   * binding for {@code (type, annotation)} does not exist yet. Throws {@link
   * IllegalArgumentException} otherwise.
   */
  public synchronized <T> void bindSupplier(
      Class<T> type, Class<? extends Annotation> annotation, Supplier<? extends T> supplier) {
    Supplier<?> existing =
        bindings.putIfAbsent(new BindingKey(type, annotation), requireNonNull(supplier));
    if (existing != null) {
      throw new IllegalArgumentException(
          String.format("A binding for %s annotated with %s already exists.", type, annotation));
    }
  }

  /**
   * Returns a {@link KafkaHttpServerInjector} containing the bindings configured in this injector.
   *
   * <p>The returned injector is <b>immutable</b>. Changes to this binder after injector creation
   * won't be reflected in the injector.
   */
  public synchronized KafkaHttpServerInjector createInjector() {
    return new KafkaHttpServerInjectorImpl(bindings);
  }

  private static final class BindingKey {

    private final Class<?> type;

    private final Class<? extends Annotation> annotation;

    private BindingKey(Class<?> type, Class<? extends Annotation> annotation) {
      this.type = requireNonNull(type);
      this.annotation = annotation;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BindingKey that = (BindingKey) o;
      return type.equals(that.type) && Objects.equals(annotation, that.annotation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, annotation);
    }
  }

  private static final class KafkaHttpServerInjectorImpl implements KafkaHttpServerInjector {

    private final Map<BindingKey, Supplier<?>> bindings;

    private KafkaHttpServerInjectorImpl(HashMap<BindingKey, Supplier<?>> bindings) {
      this.bindings = Collections.unmodifiableMap(new HashMap<>(bindings));
    }

    @Override
    public <T> T getInstance(Class<T> type) {
      return getInstance(type, /* annotation= */ null);
    }

    @Override
    public <T> T getInstance(Class<T> type, Class<? extends Annotation> annotation) {
      Supplier<T> supplier = getSupplier(type, annotation);
      try {
        return supplier.get();
      } catch (Throwable error) {
        throw new ProvisionException(
            String.format(
                "Error while getting instance for %s annotated with %s.", type, annotation),
            error);
      }
    }

    @Override
    public <T> Supplier<T> getSupplier(Class<T> type) {
      return getSupplier(type, /* annotation= */ null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Supplier<T> getSupplier(Class<T> type, Class<? extends Annotation> annotation) {
      Supplier<T> supplier = (Supplier<T>) bindings.get(new BindingKey(type, annotation));
      if (supplier == null) {
        // Most dependency injection frameworks would instead return a proxied supplier, and only
        // fail on the actual supplier.get(), to support delayed/JIT bindings. Since this injector
        // is immutable, i.e. it does not change bindings after it is created, it makes no sense to
        // return a delayed supplier since a non-existent binding now will never exist in the
        // future. That is why we fail-fast here, when getting the supplier.
        throw new ProvisionException(
            String.format("No binding found for %s annotated with %s.", type, annotation));
      }
      return supplier;
    }
  }
}
