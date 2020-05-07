package io.confluent.http.server;

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
  public void serverImpl_servletImpl_returnsServer() {
    Optional<KafkaHttpServer> httpServer =
        KafkaHttpServerLoader.load(
            configuration(/* server= */ "test", /* servlet= */ "test"),
            new KafkaHttpServerBinder().createInjector());

    assertTrue(httpServer.isPresent());
  }

  @Test(expected = KafkaHttpServletLoadingException.class)
  public void serverImpl_servletImplAndThrowingServlet_returnsServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "test", /* servlet= */ "test,throwing"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test(expected = KafkaHttpServletLoadingException.class)
  public void serverImpl_throwingServlet_doesNotReturnServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "test", /* servlet= */ "throwing"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test
  public void serverImpl_noServlet_doesNotReturnServer() {
    Optional<KafkaHttpServer> httpServer =
        KafkaHttpServerLoader.load(
            configuration(/* server= */ "test", /* servlet= */ ""),
            new KafkaHttpServerBinder().createInjector());

    assertFalse(httpServer.isPresent());
  }

  @Test(expected = KafkaHttpServerLoadingException.class)
  public void throwingServer_servletImpl_returnsServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "throwing", /* servlet= */ "test"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test(expected = KafkaHttpServletLoadingException.class)
  public void throwingServer_servletImplAndThrowingServlet_returnsServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "throwing", /* servlet= */ "test,throwing"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test(expected = KafkaHttpServletLoadingException.class)
  public void throwingServer_throwingServlet_doesNotReturnServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "throwing", /* servlet= */ "throwing"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test
  public void throwingServer_noServlet_doesNotReturnServer() {
    Optional<KafkaHttpServer> httpServer =
        KafkaHttpServerLoader.load(
            configuration(/* server= */ "throwing", /* servlet= */ ""),
            new KafkaHttpServerBinder().createInjector());

    assertFalse(httpServer.isPresent());
  }

  @Test
  public void noServer_servletImpl_returnsServer() {
    Optional<KafkaHttpServer> httpServer =
        KafkaHttpServerLoader.load(
            configuration(/* server= */ "", /* servlet= */ "test"),
            new KafkaHttpServerBinder().createInjector());

    assertFalse(httpServer.isPresent());
  }

  @Test(expected = KafkaHttpServletLoadingException.class)
  public void noServer_servletImplAndThrowingServlet_returnsServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "", /* servlet= */ "test,throwing"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test(expected = KafkaHttpServletLoadingException.class)
  public void noServer_throwingServlet_doesNotReturnServer() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "", /* servlet= */ "throwing"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test
  public void noServer_noServlet_doesNotReturnServer() {
    Optional<KafkaHttpServer> httpServer =
        KafkaHttpServerLoader.load(
            configuration(/* server= */ "", /* servlet= */ ""),
            new KafkaHttpServerBinder().createInjector());

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
      if (configuration.get("kafka.http.server").equals("test")) {
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
      if (configuration.get("kafka.http.server").equals("throwing")) {
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
      if (configuration.get("kafka.http.servlet").toString().contains("test")) {
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
      if (configuration.get("kafka.http.servlet").toString().contains("throwing")) {
        throw new RuntimeException();
      } else {
        return Optional.empty();
      }
    }
  }
}
