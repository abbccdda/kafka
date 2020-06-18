/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package org.apache.kafka.jmh.audit;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.multitenant.utils.TenantSanitizer;
import io.confluent.kafka.server.plugins.auth.PlainSaslServer;
import io.confluent.security.audit.provider.ConfluentAuditLogProvider;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.apache.kafka.server.audit.AuthenticationErrorInfo;
import org.apache.kafka.server.audit.DefaultAuthenticationEvent;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Benchmarks for tuning authentication Audit Logging performance. This calls the logEvent
 * method with a number of different configurations:
 * - not logging any messages (with the defaults set to "")
 * This measures the cost of creating an AuditLogEntry and using the router
 * to determine its disposition
 * - Logging only successful authentications
 * This measures the cost of creating event logs only for success events
 * - Logging failure authentications
 * This measures the cost of creating event logs only for failure events
 * - Logging both success and failure events
 * This measures the cost of creating event logs for both success and failure events
 */

@State(org.openjdk.jmh.annotations.Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MultiTenantProviderAuthenticationEventBenchmark {
    private static final String TENANT_ID = "lkc-1234";

    private long counter = 0;

    private final AuthenticationEventArguments[] args = new AuthenticationEventArguments[ProviderBenchmarkDefaults.DISTINCT_KEYS];
    private ConfluentAuditLogProvider provider;

    @Setup(Level.Trial)
    public void setUp() {
        TenantMetadata metadata = new TenantMetadata(TENANT_ID, TENANT_ID);
        for (int i = 0; i < ProviderBenchmarkDefaults.DISTINCT_KEYS; i++) {
            KafkaPrincipal principal = new MultiTenantPrincipal("" + i % ProviderBenchmarkDefaults.USERS, metadata);
            PlainSaslServer server = mock(PlainSaslServer.class);
            when(server.getMechanismName()).thenReturn(PlainSaslServer.PLAIN_MECHANISM);

            if (i % 2 == 0) {
                when(server.userIdentifier()).thenReturn("APIKEY" + i % ProviderBenchmarkDefaults.USERS);
                AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
                    InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
                args[i] = new AuthenticationEventArguments(principal, authenticationContext, AuditEventStatus.SUCCESS, null);
            } else {
                AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
                    InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
                AuthenticationErrorInfo errorInfo = new
                    AuthenticationErrorInfo(AuditEventStatus.UNAUTHENTICATED, "", "APIKEY" + i % ProviderBenchmarkDefaults.USERS, "clusterId1");
                SaslAuthenticationException authenticationException = new SaslAuthenticationException("password not specified", errorInfo);
                args[i] = new AuthenticationEventArguments(principal, authenticationContext, AuditEventStatus.UNAUTHENTICATED, authenticationException);
            }
        }

        ProviderBenchmarkDefaults.noneProvider.setSanitizer(TenantSanitizer::tenantAuditEvent);
        ProviderBenchmarkDefaults.allowProvider.setSanitizer(TenantSanitizer::tenantAuditEvent);
        ProviderBenchmarkDefaults.denyProvider.setSanitizer(TenantSanitizer::tenantAuditEvent);
        ProviderBenchmarkDefaults.everythingLogProvider.setSanitizer(TenantSanitizer::tenantAuditEvent);
    }

    @Benchmark
    public void testLogAuthenticationNone() {
        provider = ProviderBenchmarkDefaults.noneProvider;
        counter++;
        int index = (int) (counter % ProviderBenchmarkDefaults.DISTINCT_KEYS);
        AuthenticationEventArguments arg = args[index];
        provider.logEvent(new DefaultAuthenticationEvent(arg.principal, arg.authenticationContext, arg.auditEventStatus, arg.authenticationException));
    }

    @Benchmark
    public void testLogAuthenticationSuccess() {
        provider = ProviderBenchmarkDefaults.allowProvider;
        counter++;
        int index = (int) (counter % ProviderBenchmarkDefaults.DISTINCT_KEYS);
        AuthenticationEventArguments arg = args[index];
        provider.logEvent(new DefaultAuthenticationEvent(arg.principal, arg.authenticationContext, arg.auditEventStatus, arg.authenticationException));
    }

    @Benchmark
    public void testLogAuthenticationFailure() {
        provider = ProviderBenchmarkDefaults.denyProvider;
        counter++;
        int index = (int) (counter % ProviderBenchmarkDefaults.DISTINCT_KEYS);
        AuthenticationEventArguments arg = args[index];
        provider.logEvent(new DefaultAuthenticationEvent(arg.principal, arg.authenticationContext, arg.auditEventStatus, arg.authenticationException));
    }

    @Benchmark
    public void testLogAuthenticationEvents() {
        provider = ProviderBenchmarkDefaults.everythingLogProvider;
        counter++;
        int index = (int) (counter % ProviderBenchmarkDefaults.DISTINCT_KEYS);
        AuthenticationEventArguments arg = args[index];
        provider.logEvent(new DefaultAuthenticationEvent(arg.principal, arg.authenticationContext, arg.auditEventStatus, arg.authenticationException));
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(MultiTenantProviderAuthenticationEventBenchmark.class.getSimpleName())
            .forks(2)
            .build();

        new Runner(opt).run();
    }
}
