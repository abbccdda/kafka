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

package io.confluent.security.authorizer.utils;

import io.confluent.security.authorizer.RequestContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

public class AuthorizerUtils {

    public static RequestContext newRequestContext(String source, KafkaPrincipal principal, String host) {
        return new RequestContext() {
            @Override
            public String requestSource() {
                return source;
            }

            @Override
            public String listenerName() {
                return null;
            }

            @Override
            public SecurityProtocol securityProtocol() {
                return null;
            }

            @Override
            public KafkaPrincipal principal() {
                return principal;
            }

            @Override
            public InetAddress clientAddress() {
                try {
                    return InetAddress.getByName(host);
                } catch (UnknownHostException e) {
                    throw new KafkaException(e);
                }
            }

            @Override
            public int requestType() {
                return -1;
            }

            @Override
            public int requestVersion() {
                return -1;
            }

            @Override
            public String clientId() {
                return null;
            }

            @Override
            public int correlationId() {
                return -1;
            }
        };
    }

    public static RequestContext kafkaRequestContext(AuthorizableRequestContext kafkaContext) {
        return new RequestContext() {
            @Override
            public String requestSource() {
                return RequestContext.KAFKA;
            }

            @Override
            public String listenerName() {
                return kafkaContext.listenerName();
            }

            @Override
            public SecurityProtocol securityProtocol() {
                return kafkaContext.securityProtocol();
            }

            @Override
            public KafkaPrincipal principal() {
                return kafkaContext.principal();
            }

            @Override
            public InetAddress clientAddress() {
                return kafkaContext.clientAddress();
            }

            @Override
            public int requestType() {
                return kafkaContext.requestType();
            }

            @Override
            public int requestVersion() {
                return kafkaContext.requestVersion();
            }

            @Override
            public String clientId() {
                return kafkaContext.clientId();
            }

            @Override
            public int correlationId() {
                return kafkaContext.correlationId();
            }
        };
    }
}
