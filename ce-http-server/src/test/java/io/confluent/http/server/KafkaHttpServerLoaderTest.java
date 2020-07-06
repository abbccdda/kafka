package io.confluent.http.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class KafkaHttpServerLoaderTest {

  @Test
  public void testServerImplReturnServer() {
    Optional<KafkaHttpServer> httpServer =
        KafkaHttpServerLoader.load(
            configuration(/* server= */ "test"),
            new KafkaHttpServerBinder().createInjector());

    assertTrue(httpServer.isPresent());
  }

  @Test(expected = KafkaHttpServerLoadingException.class)
  public void testThrowingServerThrowsLoadingException() {
    KafkaHttpServerLoader.load(
        configuration(/* server= */ "throwing"),
        new KafkaHttpServerBinder().createInjector());
  }

  @Test
  public void testNoServerDoesNotReturnServer() {
    Optional<KafkaHttpServer> httpServer =
        KafkaHttpServerLoader.load(
            configuration(/* server= */ ""),
            new KafkaHttpServerBinder().createInjector());

    assertFalse(httpServer.isPresent());
  }

  private static Map<String, Object> configuration(String server) {
    HashMap<String, Object> configuration = new HashMap<>();
    configuration.put("kafka.http.server", server);
    return configuration;
  }

  public static final class TestKafkaHttpServerProvider implements KafkaHttpServerProvider {

    @Override
    public Optional<KafkaHttpServer> provide(
        Map<String, Object> configuration, KafkaHttpServerInjector injector) {
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
        Map<String, Object> configuration, KafkaHttpServerInjector injector) {
      if (configuration.getOrDefault("kafka.http.server", "").equals("throwing")) {
        throw new RuntimeException();
      } else {
        return Optional.empty();
      }
    }
  }
}
