package org.apache.kafka.jmh.audit;

import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.provider.ConfluentAuditLogProvider;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import java.net.InetAddress;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import io.confluent.security.authorizer.provider.ConfluentAuthorizationEvent;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks for tuning Audit Logging performance. This calls the logEvent
 * method with a number of different configurations:
 * - not logging any messages (with the defaults set to "")
 *   This measures the cost of creating an AuditLogEntry and using the router
 *   to determine its disposition
 * - Logging only Create authorizations
 *   This simulates the default "Management only" auditing, with 1/33 of the
 *   authorizations being "Management" operations
 * - Logging Create authorizations and 1/97 of Produce authorizations
 *   Very selective Produce logging
 * - Logging Create and 11/97 of Produce authorizations
 *   Not selective Produce logging
 * - Logging Create and 97/97 of Produce authorizations
 *   Comprehensive Produce logging
 * - Logging Everything
 *   Comprehensive Produce and consume logging
 */

@State(org.openjdk.jmh.annotations.Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ProviderLogAuthorizationBenchmark {

  private long counter = 0;

  private final LogAuthorizationArguments[] args = new LogAuthorizationArguments[ProviderBenchmarkDefaults.DISTINCT_KEYS];
  private ConfluentAuditLogProvider provider;

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {

    for (int i = 0; i < ProviderBenchmarkDefaults.DISTINCT_KEYS; i++) {
      Scope scope = Scope.kafkaClusterScope(ProviderBenchmarkDefaults.CLUSTER_ID);
      KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user" + i % ProviderBenchmarkDefaults.USERS);
      ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"), "topic" + i % ProviderBenchmarkDefaults.TOPICS,
          PatternType.LITERAL);
      RequestContext context = new MockRequestContext(
          new RequestHeader(
              ProviderBenchmarkDefaults.API_KEYS[i % ProviderBenchmarkDefaults.API_KEYS.length], (short) 1, "", i), "",
          InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
          SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
      Action action = new Action(scope, topic.resourceType(), topic.name(),
          new Operation(
              ProviderBenchmarkDefaults.ACTIONS[i % ProviderBenchmarkDefaults.ACTIONS.length]));
      AuthorizeResult result =
          i % 2 == 0 ? AuthorizeResult.ALLOWED : AuthorizeResult.DENIED;
      AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
          principal);

      args[i] = new LogAuthorizationArguments(scope, context, action, result, policy);
    }

    ProviderBenchmarkDefaults.noneProvider.setSanitizer(null);
    ProviderBenchmarkDefaults.createProvider.setSanitizer(null);
    ProviderBenchmarkDefaults.createProduceOneLogProvider.setSanitizer(null);
    ProviderBenchmarkDefaults.createProduceSomeLogProvider.setSanitizer(null);
    ProviderBenchmarkDefaults.createProduceAllLogProvider.setSanitizer(null);
    ProviderBenchmarkDefaults.everythingLogProvider.setSanitizer(null);
  }

  @SuppressWarnings("unchecked")
  @TearDown(Level.Trial)
  public void tearDown() {
    System.out.println("Topic Deliveries:");
    CountExporter<AuditLogEntry> ce = (CountExporter) provider.getEventLogger().eventExporter();
    ce.counts.entrySet().stream()
        .sorted(Comparator.comparing(Entry::getKey))
        .forEach(e -> System.out.println(e.getKey() + "\t" + e.getValue()));
  }

  @Benchmark
  public void testLogAuthorizationNone() {
    provider = ProviderBenchmarkDefaults.noneProvider;
    counter++;
    int index = (int) (counter % ProviderBenchmarkDefaults.DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logEvent(new ConfluentAuthorizationEvent(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy));
  }

  @Benchmark
  public void testLogAuthorizationCreate() {
    provider = ProviderBenchmarkDefaults.createProvider;
    counter++;
    int index = (int) (counter % ProviderBenchmarkDefaults.DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logEvent(new ConfluentAuthorizationEvent(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy));
  }

  @Benchmark
  public void testLogAuthorizationCreateProduceOne() {
    provider = ProviderBenchmarkDefaults.createProduceOneLogProvider;
    counter++;
    int index = (int) (counter % ProviderBenchmarkDefaults.DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logEvent(new ConfluentAuthorizationEvent(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy));
  }

  @Benchmark
  public void testLogAuthorizationCreateProduceSome() {
    provider = ProviderBenchmarkDefaults.createProduceSomeLogProvider;
    counter++;
    int index = (int) (counter % ProviderBenchmarkDefaults.DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logEvent(new ConfluentAuthorizationEvent(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy));
  }

  @Benchmark
  public void testLogAuthorizationCreateProduceAll() {
    provider = ProviderBenchmarkDefaults.createProduceAllLogProvider;
    counter++;
    int index = (int) (counter % ProviderBenchmarkDefaults.DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logEvent(new ConfluentAuthorizationEvent(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy));
  }

  @Benchmark
  public void testLogAuthorizationEverything() {
    provider = ProviderBenchmarkDefaults.everythingLogProvider;
    counter++;
    int index = (int) (counter % ProviderBenchmarkDefaults.DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logEvent(new ConfluentAuthorizationEvent(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy));
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(ProviderLogAuthorizationBenchmark.class.getSimpleName())
        .forks(2)
        .build();

    new Runner(opt).run();
  }

}
