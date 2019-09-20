// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.restserver;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.rest.BrokerProxy;
import org.apache.kafka.server.rest.RestServer;
import org.apache.kafka.server.rest.RestServerException;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.restserver.resources.MetadataResource;

/**
 * Embedded server for the REST API of ce-kafka.
 */
public class KafkaRestServer implements RestServer {
    private static final Logger log = LoggerFactory.getLogger(KafkaRestServer.class);

    private static final Pattern LISTENER_PATTERN = Pattern.compile("^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)");
    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 60 * 1000;

    private static final String PROTOCOL_HTTP = "http";
    private static final String PROTOCOL_HTTPS = "https";

    private KafkaRestServerConfig config;
    private ContextHandlerCollection handlers;
    private Server jettyServer;

    /**
     * Create a REST server.
     */
    public KafkaRestServer() {
        jettyServer = new Server();
        handlers = new ContextHandlerCollection();
    }

    /**
     * Configure the REST server.
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        config = new KafkaRestServerConfig(configs);

        List<String> listeners = config.getList(KafkaRestServerConfig.LISTENERS_CONFIG);
        createConnectors(listeners);
    }

    /**
     * Start up the REST server.
     */
    @Override
    public void startup(BrokerProxy broker) throws RestServerException {
        initializeServer();
        initializeResources(broker);
    }

    /**
     * Shutdown the REST server.
     */
    @Override
    public void shutdown() {
        log.info("Stopping REST server");

        try {
            jettyServer.stop();
            jettyServer.join();
        } catch (Exception e) {
            jettyServer.destroy();
            throw new RestServerException("Unable to stop REST server", e);
        }

        log.info("REST server stopped");
    }

    /**
     * Return the URI that the server is listening on.
     */
    public URI serverUrl() {
        return jettyServer.getURI();
    }

    /**
     * Add Jetty connector for each configured listener
     */
    private void createConnectors(List<String> listeners) {
        List<Connector> connectors = new ArrayList<>();

        for (String listener : listeners) {
            if (!listener.isEmpty()) {
                Connector connector = createConnector(listener);
                connectors.add(connector);
                log.info("Added connector for " + listener);
            }
        }

        jettyServer.setConnectors(connectors.toArray(new Connector[connectors.size()]));
    }

    /**
     * Create Jetty connector according to configuration
     */
    private Connector createConnector(String listener) {
        Matcher listenerMatcher = LISTENER_PATTERN.matcher(listener);

        if (!listenerMatcher.matches())
            throw new ConfigException("Listener doesn't have the right format (protocol://hostname:port).");

        String protocol = listenerMatcher.group(1).toLowerCase(Locale.ENGLISH);

        if (!PROTOCOL_HTTP.equals(protocol) && !PROTOCOL_HTTPS.equals(protocol))
            throw new ConfigException(String.format("Listener protocol must be either \"%s\" or \"%s\".", PROTOCOL_HTTP, PROTOCOL_HTTPS));

        String hostname = listenerMatcher.group(2);
        int port = Integer.parseInt(listenerMatcher.group(3));

        ServerConnector connector;

        if (PROTOCOL_HTTPS.equals(protocol)) {
            SslContextFactory ssl = SslUtils.createServerSideSslContextFactory(config);
            connector = new ServerConnector(jettyServer, ssl);
            connector.setName(String.format("%s_%s%d", PROTOCOL_HTTPS, hostname, port));
        } else {
            connector = new ServerConnector(jettyServer);
            connector.setName(String.format("%s_%s%d", PROTOCOL_HTTP, hostname, port));
        }

        if (!hostname.isEmpty())
            connector.setHost(hostname);

        connector.setPort(port);

        return connector;
    }

    /**
     * Initialize and start the REST server.
     */
    private void initializeServer() {
        log.info("Initializing REST server");

        /* Needed for graceful shutdown as per `setStopTimeout` documentation */
        StatisticsHandler statsHandler = new StatisticsHandler();
        statsHandler.setHandler(handlers);
        jettyServer.setHandler(statsHandler);
        jettyServer.setStopTimeout(GRACEFUL_SHUTDOWN_TIMEOUT_MS);
        jettyServer.setStopAtShutdown(true);

        try {
            jettyServer.start();
        } catch (Exception e) {
            throw new RestServerException("Unable to initialize REST server", e);
        }

        log.info("REST server listening at " + jettyServer.getURI());
    }

    /**
     * Initialize the resources exposed by the REST server.
     */
    private void initializeResources(BrokerProxy broker) {
        log.info("Initializing REST resources");

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(new JacksonJsonProvider());

        resourceConfig.register(new MetadataResource(broker));

        resourceConfig.register(KafkaRestServerExceptionMapper.class);
        resourceConfig.property(ServerProperties.WADL_FEATURE_DISABLE, true);

        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletHolder servletHolder = new ServletHolder(servletContainer);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(servletHolder, "/*");

        applyServletInitializers(context);

        RequestLogHandler requestLogHandler = new RequestLogHandler();
        Slf4jRequestLogWriter slf4jRequestLogWriter = new Slf4jRequestLogWriter();
        slf4jRequestLogWriter.setLoggerName(KafkaRestServer.class.getCanonicalName());
        CustomRequestLog requestLog = new CustomRequestLog(slf4jRequestLogWriter, CustomRequestLog.EXTENDED_NCSA_FORMAT + " %msT");
        requestLogHandler.setRequestLog(requestLog);

        handlers.setHandlers(new Handler[]{context, new DefaultHandler(), requestLogHandler});
        try {
            context.start();
        } catch (Exception e) {
            throw new RestServerException("Unable to initialize REST resources", e);
        }

        log.info("REST resources initialized; server is started and ready to handle requests");
    }

    @SuppressWarnings("unchecked")
    private void applyServletInitializers(ServletContextHandler context) {
        List<Consumer> servletInitializers = config.getConfiguredInstances(
            KafkaRestServerConfig.INITIALIZERS_CLASSES_CONFIG,
            Consumer.class
        );
        for (Consumer<?> servletInitializer : servletInitializers) {
            log.info("Creating rest initializer {}", servletInitializer);
            try {
                ((Consumer<ServletContextHandler>) servletInitializer).accept(context);
            } catch (final Throwable e) {
                throw new RestServerException(
                    "Exception from custom servlet initializer "
                        + servletInitializer.getClass().getName(),
                    e
                );
            }
        }
    }
}
