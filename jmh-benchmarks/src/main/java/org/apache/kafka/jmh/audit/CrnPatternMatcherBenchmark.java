package org.apache.kafka.jmh.audit;

import io.confluent.crn.ConfluentResourceName;
import io.confluent.crn.CrnPatternMatcher;
import io.confluent.crn.CrnSyntaxException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CrnPatternMatcherBenchmark {

  private static final int DISTINCT_KEYS = 10_000;
  private static final int PATTERNS = 1000;
  private static final int CLUSTERS = 10;

  private static final String KEY = "the_key_to_use";

  private static final String VALUE = "the quick brown fox jumped over the lazy dog the olympics are about to start";

  private final ConfluentResourceName[] keys = new ConfluentResourceName[DISTINCT_KEYS];

  private final String[] values = new String[DISTINCT_KEYS];

  private CrnPatternMatcher<String> matcher;

  private long counter = 0;

  @Setup(Level.Trial)
  public void setUp() throws CrnSyntaxException {
    matcher = new CrnPatternMatcher<>();
    for (int i = 0; i < PATTERNS; i++) {
      // PATTERNS should be at least CLUSTERS * 10 so that each digit is represented in each %d
      String patternString = String.format("crn://confluent.cloud/kafka=%d*/topic=clicks%d*",
          i % CLUSTERS, i / CLUSTERS);
      values[i] = VALUE + i;
      matcher.setPattern(ConfluentResourceName.fromString(patternString), values[i]);
    }
    for (int i = 0; i < DISTINCT_KEYS; i++) {
      // we're prefix matching and each first digit is represented
      keys[i] = ConfluentResourceName
          .fromString(String.format("crn://confluent.cloud/kafka=%d/topic=clicks%d",
              i % CLUSTERS, i / CLUSTERS));
    }
  }

  @Benchmark
  public String testMatcherPerformance() {
    counter++;
    int index = (int) (counter % DISTINCT_KEYS);
    ConfluentResourceName key = keys[index];
    return matcher.match(key);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(CrnPatternMatcherBenchmark.class.getSimpleName())
        .forks(2)
        .build();

    new Runner(opt).run();
  }

}
