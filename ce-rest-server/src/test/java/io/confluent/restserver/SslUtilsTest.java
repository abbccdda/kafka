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

package io.confluent.restserver;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SslConfigs;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Assert;
import org.junit.Test;

import static io.confluent.restserver.SslUtils.withServerSslSupport;

@SuppressWarnings("deprecation")
public class SslUtilsTest {
    private static final Map<String, String> DEFAULT_CONFIG = new HashMap<>();

    static {
        DEFAULT_CONFIG.put(KafkaRestServerConfig.LISTENERS_CONFIG, "http://localhost:8080");
    }

    private static class TestServerConfig extends AbstractConfig {
        private static final ConfigDef CONFIG;

        static {
            CONFIG = withServerSslSupport(new ConfigDef());
        }

        public TestServerConfig(Map<?, ?> props) {
            super(CONFIG, props);
        }
    }

    private static class TestClientConfig extends AbstractConfig {
        private static final ConfigDef CONFIG;

        static {
            CONFIG = withServerSslSupport(new ConfigDef());
        }

        public TestClientConfig(Map<?, ?> props) {
            super(CONFIG, props);
        }
    }

    @Test
    public void testCreateServerSideSslContextFactory() {
        Map<String, String> configMap = new HashMap<>(DEFAULT_CONFIG);
        configMap.put("ssl.keystore.location", "/path/to/keystore");
        configMap.put("ssl.keystore.password", "123456");
        configMap.put("ssl.key.password", "123456");
        configMap.put("ssl.truststore.location", "/path/to/truststore");
        configMap.put("ssl.truststore.password", "123456");
        configMap.put("ssl.provider", "SunJSSE");
        configMap.put("ssl.cipher.suites", "SSL_RSA_WITH_RC4_128_SHA,SSL_RSA_WITH_RC4_128_MD5");
        configMap.put("ssl.secure.random.implementation", "SHA1PRNG");
        configMap.put("ssl.client.auth", "required");
        configMap.put("ssl.endpoint.identification.algorithm", "HTTPS");
        configMap.put("ssl.keystore.type", "JKS");
        configMap.put("ssl.protocol", "TLS");
        configMap.put("ssl.truststore.type", "JKS");
        configMap.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1");
        configMap.put("ssl.keymanager.algorithm", "SunX509");
        configMap.put("ssl.trustmanager.algorithm", "PKIX");

        TestServerConfig config = new TestServerConfig(configMap);
        SslContextFactory ssl = SslUtils.createServerSideSslContextFactory(config);

        Assert.assertEquals("file:///path/to/keystore", ssl.getKeyStorePath());
        Assert.assertEquals("file:///path/to/truststore", ssl.getTrustStorePath());
        Assert.assertEquals("SunJSSE", ssl.getProvider());
        Assert.assertArrayEquals(new String[] {"SSL_RSA_WITH_RC4_128_SHA", "SSL_RSA_WITH_RC4_128_MD5"}, ssl.getIncludeCipherSuites());
        Assert.assertEquals("SHA1PRNG", ssl.getSecureRandomAlgorithm());
        Assert.assertTrue(ssl.getNeedClientAuth());
        Assert.assertFalse(ssl.getWantClientAuth());
        Assert.assertEquals("JKS", ssl.getKeyStoreType());
        Assert.assertEquals("JKS", ssl.getTrustStoreType());
        Assert.assertEquals("TLS", ssl.getProtocol());
        Assert.assertArrayEquals(new String[] {"TLSv1.2", "TLSv1.1", "TLSv1"}, ssl.getIncludeProtocols());
        Assert.assertEquals("SunX509", ssl.getKeyManagerFactoryAlgorithm());
        Assert.assertEquals("PKIX", ssl.getTrustManagerFactoryAlgorithm());
    }

    @Test
    public void testCreateClientSideSslContextFactory() {
        Map<String, String> configMap = new HashMap<>(DEFAULT_CONFIG);
        configMap.put("ssl.keystore.location", "/path/to/keystore");
        configMap.put("ssl.keystore.password", "123456");
        configMap.put("ssl.key.password", "123456");
        configMap.put("ssl.truststore.location", "/path/to/truststore");
        configMap.put("ssl.truststore.password", "123456");
        configMap.put("ssl.provider", "SunJSSE");
        configMap.put("ssl.cipher.suites", "SSL_RSA_WITH_RC4_128_SHA,SSL_RSA_WITH_RC4_128_MD5");
        configMap.put("ssl.secure.random.implementation", "SHA1PRNG");
        configMap.put("ssl.client.auth", "required");
        configMap.put("ssl.endpoint.identification.algorithm", "HTTPS");
        configMap.put("ssl.keystore.type", "JKS");
        configMap.put("ssl.protocol", "TLS");
        configMap.put("ssl.truststore.type", "JKS");
        configMap.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1");
        configMap.put("ssl.keymanager.algorithm", "SunX509");
        configMap.put("ssl.trustmanager.algorithm", "PKIX");

        TestClientConfig config = new TestClientConfig(configMap);
        SslContextFactory ssl = SslUtils.createClientSideSslContextFactory(config);

        Assert.assertEquals("file:///path/to/keystore", ssl.getKeyStorePath());
        Assert.assertEquals("file:///path/to/truststore", ssl.getTrustStorePath());
        Assert.assertEquals("SunJSSE", ssl.getProvider());
        Assert.assertArrayEquals(new String[] {"SSL_RSA_WITH_RC4_128_SHA", "SSL_RSA_WITH_RC4_128_MD5"}, ssl.getIncludeCipherSuites());
        Assert.assertEquals("SHA1PRNG", ssl.getSecureRandomAlgorithm());
        Assert.assertFalse(ssl.getNeedClientAuth());
        Assert.assertFalse(ssl.getWantClientAuth());
        Assert.assertEquals("JKS", ssl.getKeyStoreType());
        Assert.assertEquals("JKS", ssl.getTrustStoreType());
        Assert.assertEquals("TLS", ssl.getProtocol());
        Assert.assertArrayEquals(new String[] {"TLSv1.2", "TLSv1.1", "TLSv1"}, ssl.getIncludeProtocols());
        Assert.assertEquals("SunX509", ssl.getKeyManagerFactoryAlgorithm());
        Assert.assertEquals("PKIX", ssl.getTrustManagerFactoryAlgorithm());
    }

    @Test
    public void testCreateServerSideSslContextFactoryDefaultValues() {
        Map<String, String> configMap = new HashMap<>(DEFAULT_CONFIG);
        configMap.put("ssl.keystore.location", "/path/to/keystore");
        configMap.put("ssl.keystore.password", "123456");
        configMap.put("ssl.key.password", "123456");
        configMap.put("ssl.truststore.location", "/path/to/truststore");
        configMap.put("ssl.truststore.password", "123456");
        configMap.put("ssl.provider", "SunJSSE");
        configMap.put("ssl.cipher.suites", "SSL_RSA_WITH_RC4_128_SHA,SSL_RSA_WITH_RC4_128_MD5");
        configMap.put("ssl.secure.random.implementation", "SHA1PRNG");

        TestServerConfig config = new TestServerConfig(configMap);
        SslContextFactory ssl = SslUtils.createServerSideSslContextFactory(config);

        Assert.assertEquals(SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, ssl.getKeyStoreType());
        Assert.assertEquals(SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, ssl.getTrustStoreType());
        Assert.assertEquals(SslConfigs.DEFAULT_SSL_PROTOCOL, ssl.getProtocol());
        Assert.assertArrayEquals(
            Arrays.asList(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS.split("\\s*,\\s*")).toArray(), ssl.getIncludeProtocols());
        Assert.assertEquals(SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM, ssl.getKeyManagerFactoryAlgorithm());
        Assert.assertEquals(SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM, ssl.getTrustManagerFactoryAlgorithm());
        Assert.assertFalse(ssl.getNeedClientAuth());
        Assert.assertFalse(ssl.getWantClientAuth());
    }

    @Test
    public void testCreateClientSideSslContextFactoryDefaultValues() {
        Map<String, String> configMap = new HashMap<>(DEFAULT_CONFIG);
        configMap.put("ssl.keystore.location", "/path/to/keystore");
        configMap.put("ssl.keystore.password", "123456");
        configMap.put("ssl.key.password", "123456");
        configMap.put("ssl.truststore.location", "/path/to/truststore");
        configMap.put("ssl.truststore.password", "123456");
        configMap.put("ssl.provider", "SunJSSE");
        configMap.put("ssl.cipher.suites", "SSL_RSA_WITH_RC4_128_SHA,SSL_RSA_WITH_RC4_128_MD5");
        configMap.put("ssl.secure.random.implementation", "SHA1PRNG");

        TestClientConfig config = new TestClientConfig(configMap);
        SslContextFactory ssl = SslUtils.createClientSideSslContextFactory(config);

        Assert.assertEquals(SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, ssl.getKeyStoreType());
        Assert.assertEquals(SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, ssl.getTrustStoreType());
        Assert.assertEquals(SslConfigs.DEFAULT_SSL_PROTOCOL, ssl.getProtocol());
        Assert.assertArrayEquals(Arrays.asList(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS.split("\\s*,\\s*")).toArray(), ssl.getIncludeProtocols());
        Assert.assertEquals(SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM, ssl.getKeyManagerFactoryAlgorithm());
        Assert.assertEquals(SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM, ssl.getTrustManagerFactoryAlgorithm());
        Assert.assertFalse(ssl.getNeedClientAuth());
        Assert.assertFalse(ssl.getWantClientAuth());
    }
}

