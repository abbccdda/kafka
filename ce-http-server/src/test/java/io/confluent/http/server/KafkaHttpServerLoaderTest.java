package io.confluent.http.server;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class KafkaHttpServerLoaderTest {

  @Test
  public void testServerImplServletImplReturnsServer() {
    Optional<KafkaHttpServer> httpServer =
        KafkaHttpServerLoader.load(
            configuration(/* server= */ "test", /* servlet= */ "test"),
            new KafkaHttpServerBinder().createInjector());

    assertTrue(httpServer.isPresent());
  }

  @Test(expected = KafkaHttpServletLoadingException.class)
  public void testServerImplServletImplAndThrowingServletReturnsServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "test", /* servlet= */ "test,throwing"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test(expected = KafkaHttpServletLoadingException.class)
  public void testServerImplThrowingServletDoesNotReturnServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "test", /* servlet= */ "throwing"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test
  public void testServerImplNoServletDoesNotReturnServer() {
    Optional<KafkaHttpServer> httpServer =
        KafkaHttpServerLoader.load(
            configuration(/* server= */ "test", /* servlet= */ ""),
            new KafkaHttpServerBinder().createInjector());

    assertFalse(httpServer.isPresent());
  }

  @Test(expected = KafkaHttpServerLoadingException.class)
  public void testThrowingServerServletImplReturnsServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "throwing", /* servlet= */ "test"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test(expected = KafkaHttpServletLoadingException.class)
  public void testThrowingServerServletImplAndThrowingServletReturnsServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "throwing", /* servlet= */ "test,throwing"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test(expected = KafkaHttpServletLoadingException.class)
  public void testThrowingServerThrowingServletDoesNotReturnServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "throwing", /* servlet= */ "throwing"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test
  public void testThrowingServerNoServletDoesNotReturnServer() {
    Optional<KafkaHttpServer> httpServer =
        KafkaHttpServerLoader.load(
            configuration(/* server= */ "throwing", /* servlet= */ ""),
            new KafkaHttpServerBinder().createInjector());

    assertFalse(httpServer.isPresent());
  }

  @Test
  public void testNoServerServletImplReturnsServer() {
    Optional<KafkaHttpServer> httpServer =
        KafkaHttpServerLoader.load(
            configuration(/* server= */ "", /* servlet= */ "test"),
            new KafkaHttpServerBinder().createInjector());

    assertFalse(httpServer.isPresent());
  }

  @Test(expected = KafkaHttpServletLoadingException.class)
  public void testNoServerServletImplAndThrowingServletReturnsServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "", /* servlet= */ "test,throwing"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test(expected = KafkaHttpServletLoadingException.class)
  public void testNoServerThrowingServletDoesNotReturnServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "", /* servlet= */ "throwing"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test
  public void testNoServerNoServletDoesNotReturnServer() {
    Optional<KafkaHttpServer> httpServer =
        KafkaHttpServerLoader.load(
            configuration(/* server= */ "", /* servlet= */ ""),
            new KafkaHttpServerBinder().createInjector());

    assertFalse(httpServer.isPresent());
  }

  @Test
  public void testInvalidConfigDoesNotReturnServer() {
    Optional<KafkaHttpServer> httpServer =
        KafkaHttpServerLoader.load(emptyMap(), new KafkaHttpServerBinder().createInjector());

    assertFalse(httpServer.isPresent());
  }

  private static Map<String, Object> configuration(String server, String servlet) {
    HashMap<String, Object> configuration = new HashMap<>();
    configuration.put("kafka.http.server", server);
    configuration.put("kafka.http.servlet", servlet);
    return configuration;
  }

  public static final class TestKafkaHttpServerProvider implements KafkaHttpServerProvider {

    @Override
    public Optional<KafkaHttpServer> provide(
        Map<String, Object> configuration, List<KafkaHttpServlet> applications) {
      if (configuration.getOrDefault("kafka.http.server", "").equals("test")) {
        return Optional.of(Mockito.mock(KafkaHttpServer.class));
      } else {
        return Optional.empty();
      }
    }
  }

  public static final class ThrowingKafkaHttpServerProvider implements KafkaHttpServerProvider {

    @Override
    public Optional<KafkaHttpServer> provide(
        Map<String, Object> configuration, List<KafkaHttpServlet> applications) {
      if (configuration.getOrDefault("kafka.http.server", "").equals("throwing")) {
        throw new RuntimeException();
      } else {
        return Optional.empty();
      }
    }
  }

  public static final class TestKafkaHttpServletProvider implements KafkaHttpServletProvider {

    @Override
    public Optional<KafkaHttpServlet> provide(
        Map<String, Object> configuration, KafkaHttpServerInjector injector) {
      if (configuration.getOrDefault("kafka.http.servlet", "").toString().contains("test")) {
        return Optional.of(Mockito.mock(KafkaHttpServlet.class));
      } else {
        return Optional.empty();
      }
    }
  }

  public static final class ThrowingKafkaHttpServletProvider implements KafkaHttpServletProvider {

    @Override
    public Optional<KafkaHttpServlet> provide(
        Map<String, Object> configuration, KafkaHttpServerInjector injector) {
      if (configuration.getOrDefault("kafka.http.servlet", "").toString().contains("throwing")) {
        throw new RuntimeException();
      } else {
        return Optional.empty();
      }
    }
  }
}
