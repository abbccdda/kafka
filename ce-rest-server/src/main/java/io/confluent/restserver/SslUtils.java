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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.List;

/**
 * Helper class for setting up SSL for RestServer and RestClient
 */
public class SslUtils {

    /**
     * SSL configuration for RestServer
     */
    @SuppressWarnings("deprecation")
    public static ConfigDef withServerSslSupport(ConfigDef config) {
        config.define(SslConfigs.SSL_PROTOCOL_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_PROTOCOL, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_PROTOCOL_DOC)
            .define(SslConfigs.SSL_PROVIDER_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_PROVIDER_DOC)
            .define(SslConfigs.SSL_CIPHER_SUITES_CONFIG, ConfigDef.Type.LIST, null, ConfigDef.Importance.LOW, SslConfigs.SSL_CIPHER_SUITES_DOC)
            .define(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, ConfigDef.Type.LIST, SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_ENABLED_PROTOCOLS_DOC)
            .define(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_KEYSTORE_TYPE_DOC)
            .define(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ConfigDef.Type.STRING, null,  ConfigDef.Importance.HIGH, SslConfigs.SSL_KEYSTORE_LOCATION_DOC)
            .define(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, SslConfigs.SSL_KEYSTORE_PASSWORD_DOC)
            .define(SslConfigs.SSL_KEY_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, SslConfigs.SSL_KEY_PASSWORD_DOC)
            .define(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_TRUSTSTORE_TYPE_DOC)
            .define(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC)
            .define(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC)
            .define(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM, ConfigDef.Importance.LOW, SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC)
            .define(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM, ConfigDef.Importance.LOW, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC)
            .define(SslConfigs.SSL_CLIENT_AUTH_CONFIG, ConfigDef.Type.STRING, "none", ConfigDef.Importance.LOW, SslConfigs.SSL_CLIENT_AUTH_DOC)
            .define(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, ConfigDef.Importance.LOW, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
            .define(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC);

        return config;
    }

    /**
     * Configures SSL/TLS for HTTPS Jetty Server
     */
    public static SslContextFactory createServerSideSslContextFactory(AbstractConfig config) {
        final SslContextFactory.Server ssl = new SslContextFactory.Server();

        configureSslContextFactoryKeyStore(ssl, config);
        configureSslContextFactoryTrustStore(ssl, config);
        configureSslContextFactoryAlgorithms(ssl, config);
        configureSslContextFactoryAuthentication(ssl, config);

        return ssl;
    }

    /**
     * Configures SSL/TLS for HTTPS Jetty Client
     */
    public static SslContextFactory createClientSideSslContextFactory(AbstractConfig config) {
        final SslContextFactory.Client ssl = new SslContextFactory.Client();

        configureSslContextFactoryKeyStore(ssl, config);
        configureSslContextFactoryTrustStore(ssl, config);
        configureSslContextFactoryAlgorithms(ssl, config);
        configureSslContextFactoryEndpointIdentification(ssl, config);

        return ssl;
    }

    /**
     * Configures KeyStore related settings in SslContextFactory
     */
    protected static void configureSslContextFactoryKeyStore(SslContextFactory ssl, AbstractConfig config) {
        ssl.setKeyStoreType(config.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG));

        String sslKeystoreLocation = config.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        if (sslKeystoreLocation != null)
            ssl.setKeyStorePath(sslKeystoreLocation);

        Password sslKeystorePassword = config.getPassword(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        if (sslKeystorePassword != null)
            ssl.setKeyStorePassword(sslKeystorePassword.value());

        Password sslKeyPassword = config.getPassword(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        if (sslKeyPassword != null)
            ssl.setKeyManagerPassword(sslKeyPassword.value());
    }

    /**
     * Configures TrustStore related settings in SslContextFactory
     */
    protected static void configureSslContextFactoryTrustStore(SslContextFactory ssl, AbstractConfig config) {
        ssl.setTrustStoreType(config.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));

        String sslTruststoreLocation = config.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        if (sslTruststoreLocation != null)
            ssl.setTrustStorePath(sslTruststoreLocation);

        Password sslTruststorePassword = config.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        if (sslTruststorePassword != null)
            ssl.setTrustStorePassword(sslTruststorePassword.value());
    }

    /**
     * Configures Protocol, Algorithm and Provider related settings in SslContextFactory
     */
    @SuppressWarnings("unchecked")
    protected static void configureSslContextFactoryAlgorithms(SslContextFactory ssl, AbstractConfig config) {
        List<String> sslEnabledProtocols = config.getList(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        ssl.setIncludeProtocols(sslEnabledProtocols.toArray(new String[sslEnabledProtocols.size()]));

        String sslProvider = config.getString(SslConfigs.SSL_PROVIDER_CONFIG);
        if (sslProvider != null)
            ssl.setProvider(sslProvider);

        ssl.setProtocol(config.getString(SslConfigs.SSL_PROTOCOL_CONFIG));

        List<String> sslCipherSuites = config.getList(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (sslCipherSuites != null)
            ssl.setIncludeCipherSuites(sslCipherSuites.toArray(new String[sslCipherSuites.size()]));

        ssl.setKeyManagerFactoryAlgorithm(config.getString(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG));

        String sslSecureRandomImpl = config.getString(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
        if (sslSecureRandomImpl != null)
            ssl.setSecureRandomAlgorithm(sslSecureRandomImpl);

        ssl.setTrustManagerFactoryAlgorithm(config.getString(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
    }

    /**
     * Configures Protocol, Algorithm and Provider related settings in SslContextFactory
     */
    protected static void configureSslContextFactoryEndpointIdentification(SslContextFactory ssl, AbstractConfig config) {
        String sslEndpointIdentificationAlg = config.getString(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        if (sslEndpointIdentificationAlg != null)
            ssl.setEndpointIdentificationAlgorithm(sslEndpointIdentificationAlg);
    }

    /**
     * Configures Authentication related settings in SslContextFactory
     */
    @SuppressWarnings("deprecation")
    protected static void configureSslContextFactoryAuthentication(SslContextFactory.Server ssl, AbstractConfig config) {
        // SslConfigs.SSL_CLIENT_AUTH_CONFIG is deprecated. Can we keep it?
        String sslClientAuth = config.getString(SslConfigs.SSL_CLIENT_AUTH_CONFIG);
        switch (sslClientAuth) {
            case "requested":
                ssl.setWantClientAuth(true);
                break;
            case "required":
                ssl.setNeedClientAuth(true);
                break;
            default:
                ssl.setNeedClientAuth(false);
                ssl.setWantClientAuth(false);
        }
    }
}

