package io.confluent.http.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import io.confluent.http.server.KafkaHttpServerInjector.ProvisionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KafkaHttpServerBinderTest {

  @Test
  public void bindInstance_noAnnotation_nonNull_bindsInstanceAndSupplier() {
    KafkaHttpServerBinder httpServerBinder = new KafkaHttpServerBinder();
    httpServerBinder.bindInstance(String.class, "foobar");

    KafkaHttpServerInjector injector = httpServerBinder.createInjector();
    assertEquals("foobar", injector.getInstance(String.class));
    assertEquals("foobar", injector.getSupplier(String.class).get());
  }

  @Test
  public void bindInstance_noAnnotation_null_bindsNullAndSupplier() {
    KafkaHttpServerBinder httpServerBinder = new KafkaHttpServerBinder();
    httpServerBinder.bindInstance(String.class, null);

    KafkaHttpServerInjector injector = httpServerBinder.createInjector();
    assertNull(injector.getInstance(String.class));
    assertNull(injector.getSupplier(String.class).get());
  }

  @Test
  public void bindInstance_withAnnotation_nonNull_bindsInstanceAndSupplier() {
    KafkaHttpServerBinder httpServerBinder = new KafkaHttpServerBinder();
    httpServerBinder.bindInstance(String.class, FooBar.class, "foobar");

    KafkaHttpServerInjector injector = httpServerBinder.createInjector();
    assertEquals("foobar", injector.getInstance(String.class, FooBar.class));
    assertEquals("foobar", injector.getSupplier(String.class, FooBar.class).get());
  }

  @Test
  public void bindInstance_withAnnotation_null_bindsNullAndSupplier() {
    KafkaHttpServerBinder httpServerBinder = new KafkaHttpServerBinder();
    httpServerBinder.bindInstance(String.class, FooBar.class, null);

    KafkaHttpServerInjector injector = httpServerBinder.createInjector();
    assertNull(injector.getInstance(String.class, FooBar.class));
    assertNull(injector.getSupplier(String.class, FooBar.class).get());
  }

  @Test
  public void bindSupplier_noAnnotation_nonNull_bindsInstanceAndSupplier() {
    KafkaHttpServerBinder httpServerBinder = new KafkaHttpServerBinder();
    httpServerBinder.bindSupplier(String.class, () -> "foobar");

    KafkaHttpServerInjector injector = httpServerBinder.createInjector();
    assertEquals("foobar", injector.getInstance(String.class));
    assertEquals("foobar", injector.getSupplier(String.class).get());
  }

  @Test
  public void bindSupplier_noAnnotation_null_bindsNullAndSupplier() {
    KafkaHttpServerBinder httpServerBinder = new KafkaHttpServerBinder();
    httpServerBinder.bindSupplier(String.class, () -> null);

    KafkaHttpServerInjector injector = httpServerBinder.createInjector();
    assertNull(injector.getInstance(String.class));
    assertNull(injector.getSupplier(String.class).get());
  }

  @Test
  public void bindSupplier_withAnnotation_nonNull_bindsInstanceAndSupplier() {
    KafkaHttpServerBinder httpServerBinder = new KafkaHttpServerBinder();
    httpServerBinder.bindSupplier(String.class, FooBar.class, () -> "foobar");

    KafkaHttpServerInjector injector = httpServerBinder.createInjector();
    assertEquals("foobar", injector.getInstance(String.class, FooBar.class));
    assertEquals("foobar", injector.getSupplier(String.class, FooBar.class).get());
  }

  @Test
  public void bindSupplier_withAnnotation_null_bindsNullAndSupplier() {
    KafkaHttpServerBinder httpServerBinder = new KafkaHttpServerBinder();
    httpServerBinder.bindSupplier(String.class, FooBar.class, () -> null);

    KafkaHttpServerInjector injector = httpServerBinder.createInjector();
    assertNull(injector.getInstance(String.class, FooBar.class));
    assertNull(injector.getSupplier(String.class, FooBar.class).get());
  }

  @Test
  public void bindingTwice_throwsException() {
    KafkaHttpServerBinder httpServerBinder = new KafkaHttpServerBinder();
    httpServerBinder.bindInstance(String.class, "foobar");

    try {
      httpServerBinder.bindInstance(String.class, "foobar");
      fail(
          "Expected exception IllegalArgumentException to be thrown, but no exception was thrown.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void getInstance_noBinding_throwsException() {
    KafkaHttpServerBinder httpServerBinder = new KafkaHttpServerBinder();
    KafkaHttpServerInjector injector = httpServerBinder.createInjector();

    try {
      injector.getInstance(String.class);
      fail("Expected exception ProvisionException to be thrown, but no exception was thrown.");
    } catch (ProvisionException e) {
    }
  }

  @Test
  public void getSupplier_noBinding_throwsException() {
    KafkaHttpServerBinder httpServerBinder = new KafkaHttpServerBinder();
    KafkaHttpServerInjector injector = httpServerBinder.createInjector();

    try {
      injector.getSupplier(String.class);
      fail("Expected exception ProvisionException to be thrown, but no exception was thrown.");
    } catch (ProvisionException e) {
    }
  }

  private @interface FooBar {
  }
}
