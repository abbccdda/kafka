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

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* An implementation of {@link ConfigProvider} that represents a Properties file.
*
* SecurePassConfigProvider decrypts the 'values' in the properties file using EncryptionEngine.
*/
public class SecurePassConfigProvider implements ConfigProvider {
  private final Logger log = LoggerFactory.getLogger(getClass());

  public final static String DATA_ENCRYPTION_KEY = "_metadata.symmetric_key.0.enc";

  public final static String MASTER_KEY_ENV_VAR = "_metadata.symmetric_key.0.envvar";

  public final static String METADATA_KEY_LENGTH = "_metadata.symmetric_key.0.length";

  public static final String METADATA_PREFIX = "_metadata";

  public void configure(Map<String, ?> configs) {

  }

  protected DecryptionEngine initializeDecryptionEngine(Properties properties) {
    try {
      String dataKey = properties.getProperty(DATA_ENCRYPTION_KEY);
      String masterKeyEnvVar = properties.getProperty(MASTER_KEY_ENV_VAR);
      String keyLength = properties.getProperty(METADATA_KEY_LENGTH);
      return new DecryptionEngine(masterKeyEnvVar, dataKey, keyLength);
    } catch (Exception e) {
        log.warn("Failed to initialize the decryption engine.");
        throw new ConfigException("Failed to initialize the decryption engine.");
    }
  }

  private boolean isCipherText(String config) {
    Matcher matcher = DecryptionEngine.CIPHER_PATTERN.matcher(config);
    if (matcher.matches()) {
      return true;
    }
    return false;
  }

  /**
   * Retrieves the data at the given Properties file and decrypts the values using the CONFLUENT_MASTER_KEY.
   *
   * @param path the file where the data resides
   * @return the unencrypted configuration data
   */
  public ConfigData get(String path) {
    Map<String, String> data = new HashMap<>();

    if (path == null || path.isEmpty()) {
      return new ConfigData(data);
    }
    try (Reader reader = reader(path)) {
      Properties properties = new Properties();
      properties.load(reader);
      DecryptionEngine decryptionEngine = initializeDecryptionEngine(properties);
      Enumeration<Object> keys = properties.keys();
      while (keys.hasMoreElements()) {
        String key = keys.nextElement().toString();
        if (!key.startsWith(METADATA_PREFIX)) {
          String value = properties.getProperty(key);
          if (value != null && !value.isEmpty()) {
            if (isCipherText(value)) {
              try {
                value = decryptionEngine.decryptWithDEK(value);
              } catch (Exception e) {
                log.warn("Failed to decrypt the value.");
              }
            }
            data.put(key, value);
          }
        }
      }
      return new ConfigData(data);
    } catch (IOException e) {
      throw new ConfigException("Could not read properties from file " + path);
    } catch (ConfigException e) {
      throw e;
    }
  }

  /**
   * Retrieves the data with the given keys at the given Properties file and decrypts the values using the CONFLUENT_MASTER_KEY.
   *
   * @param path the file where the data resides
   * @param keys the keys whose values will be retrieved
   * @return the unencrypted configuration data
   */
  public ConfigData get(String path, Set<String> keys) {
    Map<String, String> data = new HashMap<>();
    if (path == null || path.isEmpty()) {
      return new ConfigData(data);
    }
    try (Reader reader = reader(path)) {
      Properties properties = new Properties();
      properties.load(reader);
      DecryptionEngine decryptionEngine = initializeDecryptionEngine(properties);
      for (String key : keys) {
        String value = properties.getProperty(key);
        if (value != null) {
          if (isCipherText(value)) {
            try {
              value = decryptionEngine.decryptWithDEK(value);
            } catch (Exception e) {
              log.warn("Failed to decrypt the value.");
            }
          }
          data.put(key, value);
        }
    }
      return new ConfigData(data);
    } catch (IOException e) {
        throw new ConfigException("Could not read properties from file " + path);
    } catch (ConfigException e) {
      throw e;
    }
  }

  // visible for testing
  protected Reader reader(String path) throws IOException {
    return Files.newBufferedReader(Paths.get(path));
  }

  public void close() {
  }
}
