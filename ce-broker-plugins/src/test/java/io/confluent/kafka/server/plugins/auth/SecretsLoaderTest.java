package io.confluent.kafka.server.plugins.auth;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.testing.FakeTicker;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class SecretsLoaderTest {
  File tempFile;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    final String origJson = "{\"keys\": {\"key1\": {" +
        "\"user_id\": \"user1\"," +
        "\"logical_cluster_id\": \"myCluster\"," +
        "\"sasl_mechanism\": \"PLAIN\"," +
        "\"hashed_secret\": \"no hash\"," +
        "\"hash_function\": \"none\"" +
        "}}}";
    tempFile = tempFolder.newFile("apikeys.json");
    Files.write(origJson, tempFile, Charsets.UTF_8);
  }

  @Test
  public void testLoaderRefreshes() throws Exception {
    final String newJson = "{\"keys\": {\"key1\": {" +
        "\"user_id\": \"user1\"," +
        "\"logical_cluster_id\": \"myCluster\"," +
        "\"sasl_mechanism\": \"covfefe\"," +
        "\"hashed_secret\": \"no hash\"," +
        "\"hash_function\": \"none\"" +
        "}}}";
    final long refreshMs = 1000;

    FakeTicker fakeTicker = new FakeTicker();
    SecretsLoader loader = new SecretsLoader(
        tempFile.getCanonicalPath(),
        refreshMs,
        fakeTicker
    );

    Map<String, KeyConfigEntry> keys = loader.get();
    assertEquals("PLAIN", keys.get("key1").saslMechanism);

    Files.write(newJson, tempFile, Charsets.UTF_8);

    keys = loader.get();
    assertEquals("Original value remains until refresh", "PLAIN", keys.get("key1").saslMechanism);

    fakeTicker.advance(refreshMs + 1, TimeUnit.MILLISECONDS);
    keys = loader.get();
    assertEquals("New value is read", "covfefe", keys.get("key1").saslMechanism);
  }

  @Test
  public void shouldLoadFile() throws Exception {
    TenantConfig tenantConfig = SecretsLoader.loadFile(tempFile.getPath());
    Map<String, KeyConfigEntry> keys = tenantConfig.apiKeys;
    assertEquals(1, keys.size());
    KeyConfigEntry entry = keys.get("key1");
    assertNotNull(entry);
    assertEquals("PLAIN", entry.saslMechanism);
    assertEquals("user1", entry.userId);
    assertEquals("myCluster", entry.logicalClusterId);
    assertEquals("no hash", entry.hashedSecret);
    assertEquals("none", entry.hashFunction);
    assertNull(tenantConfig.quotas);
  }

  @Test
  public void shouldThrowConfigExceptionOnFileNotFound() throws Exception {
    try {
      SecretsLoader.loadFile("/foo/bar/junk/stuff/not_there.92ljlk342");
      fail("Did not throw exception");
    } catch (ConfigException e) {
      assertTrue(e.getMessage().contains("file not found"));
    }
  }

  @Test
  public void shouldThrowConfigExceptionOnParseError() throws Exception {
    Files.write(":not=valid", tempFile, Charsets.UTF_8);
    try {
      SecretsLoader.loadFile(tempFile.getPath());
      fail("Did not throw exception");
    } catch (ConfigException e) {
      assertTrue(e.getMessage().contains("Error parsing secret file"));
    }
  }

  @Test
  public void shouldThrowConfigExceptionOnMissingValue() throws Exception {
    Files.write("{\"keys\": {\"key1\": null}}", tempFile, Charsets.UTF_8);
    try {
      SecretsLoader.loadFile(tempFile.getPath());
      fail("Did not throw exception");
    } catch (ConfigException e) {
      assertTrue(e.getMessage().contains("Missing value for key key1"));
    }
  }

  @Test
  public void shouldThrowConfigExceptionOnMissingFields() throws Exception {
    Files.write("{\"keys\": {\"key1\": {}}}", tempFile, Charsets.UTF_8);
    try {
      SecretsLoader.loadFile(tempFile.getPath());
      fail("Did not throw exception");
    } catch (ConfigException e) {
      assertTrue(e.getMessage().contains("user_id field is missing for key key1"));
    }
  }

}
