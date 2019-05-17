/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package io.confluent.kafka.security.config.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

public class SecurePassConfigProviderTest {
  private SecurePassConfigProvider configProvider;

  @Before
  public void setup() throws Exception {
    configProvider = new TestSecureConfigProvider();
  }

  @Test
  public void testGetAllKeysAtPath() throws Exception {
    ConfigData configData = configProvider.get("test");
    Map<String, String> result = new HashMap<>();
    result.put("ssl.keystore.password", "sslPass");
    result.put("truststore.keystore.password", "keystorePass");
    result.put("plainTextPassword", "password");
    result.put("invalid.pattern", "xxxxENC[AES/CBC/PKCS5Padding,data:hjj,iv:BV,type:str]yyyy");
    assertEquals(result, configData.data());
    assertEquals(null, configData.ttl());
  }

  @Test
  public void testGetOneKeyAtPathWithPlainTextValue() throws Exception {
    ConfigData configData = configProvider.get("test", Collections.singleton("plainTextPassword"));
    Map<String, String> result = new HashMap<>();
    result.put("plainTextPassword", "password");
    assertEquals(result, configData.data());
    assertEquals(null, configData.ttl());
  }

  @Test
  public void testGetOneKeyAtPathWithEncryptedValue() throws Exception {
    ConfigData configData = configProvider.get("test", Collections.singleton("ssl.keystore.password"));
    Map<String, String> result = new HashMap<>();
    result.put("ssl.keystore.password", "sslPass");
    assertEquals(result, configData.data());
    assertEquals(null, configData.ttl());
  }

  @Test
  public void testEmptyPath() throws Exception {
    ConfigData configData = configProvider.get("", Collections.singleton("demoPassword"));
    assertTrue(configData.data().isEmpty());
    assertEquals(null, configData.ttl());
  }

  @Test
  public void testInvalidKey() throws Exception {
    ConfigData configData = configProvider.get("test", Collections.singleton("invalidKey"));
    assertTrue(configData.data().isEmpty());
    assertEquals(null, configData.ttl());
  }

  @Test
  public void testInvalidCipherPattern() throws Exception {
    ConfigData configData = configProvider.get("test", Collections.singleton("invalid.pattern"));
    Map<String, String> result = new HashMap<>();
    result.put("invalid.pattern", "xxxxENC[AES/CBC/PKCS5Padding,data:hjj,iv:BV,type:str]yyyy");
    assertEquals(result, configData.data());
  }

  public static class TestSecureConfigProvider extends SecurePassConfigProvider {

    @Override
    protected DecryptionEngine initializeDecryptionEngine(Properties properties) {
      try {
        String dataKey = properties.getProperty(DATA_ENCRYPTION_KEY);
        String masterKeyEnvVar = properties.getProperty(MASTER_KEY_ENV_VAR);
        String keyLength = properties.getProperty(METADATA_KEY_LENGTH);
        return new MockDecryptionEngine(masterKeyEnvVar, dataKey, keyLength);
      } catch (Exception e) {
        throw new ConfigException("Failed to initialize the decryption engine.");
      }
    }

    @Override
    protected Reader reader(String path) throws IOException {
      return new StringReader("ssl.keystore.password=ENC[AES/CBC/PKCS5Padding,data:5yaZ8P0OeUEK6ht/yFl6Sw==,iv:tL5YpVVS7exMKwFKePskSQ==,type:str]\n"
          + "truststore.keystore.password=ENC[AES/CBC/PKCS5Padding,data:XkFJY46HRrtbhl90b3pseQ==,iv:BVwV0+8Nt1j/qBr95gVQmA==,type:str]\n"
          + "_metadata.symmetric_key.0.enc=ENC[AES/CBC/PKCS5Padding,data:S6ekfn3mHjGMLLqWfHninW6ZUzzS0VdUxmTtENiBZjiPXmF4EHnaSAc1axTDz8ur,iv:Imh6Ce3gy99dqX8A4YAtQg==,type:str]\n"
          + "_metadata.symmetric_key.0.envvar=CONFLUENT_SECURITY_MASTER_KEY\n"
          + "_metadata.symmetric_key.0.length=32\n"
          + "invalid.pattern=xxxxENC[AES/CBC/PKCS5Padding,data:hjj,iv:BV,type:str]yyyy\n"
          + "plainTextPassword=password"
      );
    }
  }
}
