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

import io.netty.handler.ssl.ReferenceCountedOpenSslEngine;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestSslUtils;
import org.junit.Test;

import javax.net.ssl.SSLEngine;
import java.io.File;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NettySslFactoryTest extends SslFactoryTest {
    public NettySslFactoryTest(String tlsProtocol) {
        super(tlsProtocol);
    }

    @Override
    protected void configureSslBuilderClass(Map<String, Object> conf) {
        conf.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, NettySslEngineFactory.class);
    }

    @Test
    public void testSslFactoryConfiguration() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        configureSslBuilderClass(serverSslConfig);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        //host and port are hints
        SSLEngine engine = sslFactory.createSslEngine("localhost", 0);
        assertNotNull(engine);

        // SSLv2Hello is always enabled for OpenSSL
        // https://github.com/netty/netty/blob/b3fb2eb27f71de20cb53d64ab2281eb2d8d31aae/handler/src/main/java/io/netty/handler/ssl/OpenSsl.java#L331
        // Because of this, always add it to the set of protocol to check.
        Set<String> expected = Utils.mkSet(this.tlsProtocol);
        expected.add("SSLv2Hello");
        assertEquals(expected, Utils.mkSet(engine.getEnabledProtocols()));
        assertEquals(false, engine.getUseClientMode());
    }

    @Test
    public void testSslEngineCloser() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig =
            TestSslUtils.createSslConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        configureSslBuilderClass(serverSslConfig);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        //host and port are hints
        SSLEngine engine = sslFactory.createSslEngine("localhost", 0);
        assertNotNull(engine);
        assertTrue(engine instanceof ReferenceCountedOpenSslEngine);
        ReferenceCountedOpenSslEngine opensslEngine = (ReferenceCountedOpenSslEngine) engine;
        assertEquals(1, opensslEngine.refCnt());
        sslFactory.createCloseableSslEngine(opensslEngine).close();
        assertEquals(0, opensslEngine.refCnt());
    }
}
