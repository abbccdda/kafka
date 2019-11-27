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
package org.apache.kafka.common.security.ssl;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4JLoggerFactory;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import java.security.cert.X509Certificate;
import java.util.Arrays;

public class NettySslEngineBuilder {
    private static final Logger log = LoggerFactory.getLogger(NettySslEngineBuilder.class);

    private final SslContext sslContext;

    static {
        InternalLoggerFactory.setDefaultFactory(Log4JLoggerFactory.INSTANCE);
    }

    /**
     * Create a new NettySslEngineBuilder, if possible.
     *
     * @param ctx   The engine builder context to use.
     * @returns     A new NettySslEngineBuilder, or null if it could not be created.
     */
    public static NettySslEngineBuilder maybeCreate(SslEngineBuilder ctx) {
        if (!OpenSsl.isAvailable()) {
            log.warn("Disabling netty because no OpenSSL is available.");
            return null;
        }
        SslContext sslContext = createNettySslContext(ctx);
        return new NettySslEngineBuilder(sslContext);
    }

    private static SslContext createNettySslContext(SslEngineBuilder ctx) {
        try {
            if (ctx.keystore() == null) {
                throw new KafkaException("Whe using netty in server mode, a keystore must be configured.");
            }
            // The keystore should contain the private key as well as the
            // certificate chain for the server.
            SslEngineBuilder.PrivateKeyData keystorePrivateKeyData = ctx.keystore().loadPrivateKeyData();
            X509Certificate[] truststoreCerts = ctx.truststore() == null ?
                null : ctx.truststore().loadAllCertificates();

            SslContextBuilder builder = SslContextBuilder.
                forServer(keystorePrivateKeyData.key(), keystorePrivateKeyData.certificateChain()).
                applicationProtocolConfig(ApplicationProtocolConfig.DISABLED).
                sslProvider(SslProvider.OPENSSL).
                trustManager(truststoreCerts);
            if (ctx.enabledProtocols() != null) {
                builder.protocols(ctx.enabledProtocols());
            }
            if (ctx.cipherSuites() != null) {
                builder.ciphers(Arrays.asList(ctx.cipherSuites()));
            }
            switch (ctx.sslClientAuth()) {
                case NONE:
                    builder.clientAuth(ClientAuth.NONE);
                    break;
                case REQUIRED:
                    builder.clientAuth(ClientAuth.REQUIRE);
                    break;
                case REQUESTED:
                    builder.clientAuth(ClientAuth.OPTIONAL);
                    break;
            }
            // Note: we ignore endpointIdentificationAlgorithm here.
            // It is only relevant for client mode, and we are in server mode.
            log.info("netty is enabled for SSL context with keystore {}, truststore {}.",
                ctx.keystore(), ctx.truststore());
            return builder.build();
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    private NettySslEngineBuilder(SslContext sslContext) {
        this.sslContext = sslContext;
    }

    public SSLEngine newEngine(String peerHost, int peerPort) {
        return sslContext.newEngine(ByteBufAllocator.DEFAULT, peerHost, peerPort);
    }
}
