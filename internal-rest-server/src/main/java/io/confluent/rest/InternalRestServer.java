/*
 Copyright 2020 Confluent Inc.
 */

package io.confluent.rest;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Binds a Rest Server to the provided port, serving the RollHandler API.
 */
public final class InternalRestServer {
    private final static Logger log = LoggerFactory.getLogger(InternalRestServer.class);
    private final BeginShutdownBrokerHandle beginShutdownBrokerHandle;
    private final int port;
    private Server server;

    public InternalRestServer(int port, BeginShutdownBrokerHandle beginShutdownBrokerHandle) {
        log.isDebugEnabled();
        this.port = port;
        this.beginShutdownBrokerHandle = Objects.requireNonNull(beginShutdownBrokerHandle, "null"
                 + " beginShutdownBrokerHandle");
    }

    /**
     * Bind the RestServer to the provided port.
     */
    public synchronized void start() throws Exception {
        if (server != null) {
            throw new IllegalStateException("Server is already running.");
        }
        log.info("Binding to port " + port);
        this.server = new Server(port);
        ContextHandler rollContext = new ContextHandler("/v1/roll");
        rollContext.setContextPath("/v1/roll");
        rollContext.setHandler(new RollHandler(beginShutdownBrokerHandle));
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(new Handler[]{rollContext, new DefaultHandler()});
        server.setHandler(contexts);
        server.start();
    }

    public synchronized void stop() throws Exception {
        if (server == null) {
            throw new IllegalStateException("Server is not running.");
        }
        log.info("Stopping");
        server.stop();
        server.join();
        server = null;
    }
}
