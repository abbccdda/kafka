package io.confluent.crn;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class CrnPatternMatcherTest {

  @Test
  public void testMatch() throws CrnSyntaxException {
    List<String> patterns = Arrays.asList(
        "crn:///kafka=lkc-a1b2c3/topic=clacks",
        "crn:///kafka=lkc-b2c3a1/topic=clacks",
        "crn:///kafka=lkc-b2c3a1/topic=*",
        "crn:///organization=123/kafka=lkc-b2c3a1/topic=*",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks2",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clocks",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks*",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=cli*",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*",
        "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3/topic=clicks",
        "crn://confluent.cloud/organization=123/kafka=*/topic=clicks",
        "crn://confluent.cloud/organization=123/kafka=*/topic=*",
        "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3",
        "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3/topic=*",
        "crn://confluent.cloud/organization=123/kafka=lkc-e5g6f7/topic=*");

    CrnPatternMatcher.Builder<String> builder = CrnPatternMatcher.builder();
    patterns.stream()
        .flatMap(s -> {
          try {
            return Stream.of(ConfluentResourceName.fromString(s));
          } catch (CrnSyntaxException e) {
            return Stream.empty();
          }
        })
        .forEach(p -> builder.setPattern(p, p.toString()));
    CrnPatternMatcher<String> matcher = builder.build();

    Assert.assertEquals(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks")));
    Assert.assertEquals(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks2",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks2")));
    Assert.assertEquals(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks*",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks3")));
    Assert.assertEquals(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=cli*",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clips")));
    Assert.assertEquals(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=claps")));
    Assert.assertEquals(
        "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3/topic=clicks",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3/topic=clicks")));
    Assert.assertEquals(
        "crn://confluent.cloud/organization=123/kafka=*/topic=clicks",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/organization=123/kafka=lkc-d4e5f6/topic=clicks")));
    Assert.assertEquals(
        "crn://confluent.cloud/organization=123/kafka=*/topic=*",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/organization=123/kafka=lkc-d4e5f6/topic=claps")));
    Assert.assertEquals(
        "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3")));
    Assert.assertEquals(
        "crn://confluent.cloud/organization=123/kafka=lkc-e5g6f7/topic=*",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/organization=123/kafka=lkc-e5g6f7/topic=clicks")));
    Assert.assertEquals(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clacks")));
    Assert.assertEquals(
        "crn:///kafka=lkc-b2c3a1/topic=clacks",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-b2c3a1/topic=clacks")));
    Assert.assertEquals(
        "crn:///kafka=lkc-b2c3a1/topic=*",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-b2c3a1/topic=clicks")));
    Assert.assertEquals(
        "crn://confluent.cloud/organization=123/kafka=*/topic=*",
        matcher.match(ConfluentResourceName.fromString(
            "crn://confluent.cloud/organization=123/kafka=lkc-b2c3a1/topic=clacks")));
    Assert.assertEquals(
        "crn:///kafka=lkc-b2c3a1/topic=*",
        matcher.match(ConfluentResourceName.fromString(
            "crn:///kafka=lkc-b2c3a1/topic=clicks")));

    Assert.assertNull(matcher.match(ConfluentResourceName.fromString(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/group=clicks")));
    Assert.assertNull(matcher.match(ConfluentResourceName.fromString(
        "crn://confluent.cloud/organization=123/kafka=lkc-d4e5f6")));
    Assert.assertNull(matcher.match(ConfluentResourceName.fromString(
        "crn://confluent.cloud/topic=clicks")));
    Assert.assertNull(matcher.match(ConfluentResourceName.fromString(
        "crn://confluent.cloud/organization=123/topic=clicks")));
    Assert.assertNull(matcher.match(ConfluentResourceName.fromString(
        "crn:///kafka=lkc-a1b2c3/topic=clocks")));
  }

  @Test
  public void testCachedMatch() throws CrnSyntaxException {
    List<String> patterns = Arrays.asList(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks2",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clocks",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks*",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=cli*",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*",
        "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3/topic=clicks",
        "crn://confluent.cloud/organization=123/kafka=*/topic=clicks",
        "crn://confluent.cloud/organization=123/kafka=*/topic=*",
        "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3");

    CachedCrnStringPatternMatcher.Builder<String> builder = CachedCrnStringPatternMatcher.builder();
    builder.capacity(100);
    patterns
        .forEach(p -> {
            builder.setPattern(p, p);
        });
    CachedCrnStringPatternMatcher<String> matcher = builder.build();

    Assert.assertEquals("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks",
        matcher.match("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks"));
    Assert.assertEquals("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks2",
        matcher.match("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks2"));
    Assert.assertEquals("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks*",
        matcher.match("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks3"));
    Assert.assertEquals("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=cli*",
        matcher.match("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clips"));
    Assert.assertEquals("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*",
        matcher.match("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=claps"));
    Assert.assertEquals("crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3/topic=clicks",
        matcher.match("crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3/topic=clicks"));
    Assert.assertEquals("crn://confluent.cloud/organization=123/kafka=*/topic=clicks",
        matcher.match("crn://confluent.cloud/organization=123/kafka=lkc-d4e5f6/topic=clicks"));
    Assert.assertEquals("crn://confluent.cloud/organization=123/kafka=*/topic=*",
        matcher.match("crn://confluent.cloud/organization=123/kafka=lkc-d4e5f6/topic=claps"));
    Assert.assertEquals("crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3",
        matcher.match("crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3"));

    Assert.assertNull(matcher.match(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/group=clicks"));
    Assert.assertNull(matcher.match(
        "crn://confluent.cloud/organization=123/kafka=lkc-d4e5f6"));
  }

  @Test
  public void testLRU() throws CrnSyntaxException {
    CachedCrnStringPatternMatcher<String> matcher = CachedCrnStringPatternMatcher.<String>builder()
        .capacity(3)
        .setPattern("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*",
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*")
        .build();

    for (int i = 0; i < 10; i++) {
      Assert.assertEquals("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*",
          matcher.match("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks" + i));
      Assert.assertEquals(Math.min(i + 1, 3), matcher.size());
    }
  }

  @Test
  public void testNullCached() throws CrnSyntaxException {
    CachedCrnStringPatternMatcher<String> matcher = CachedCrnStringPatternMatcher.<String>builder()
        .capacity(3)
        .build();
    Assert.assertNull(matcher.match("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*"));
    Assert.assertNotNull(matcher.matchEntry("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*"));
    Assert.assertFalse(
        matcher.matchEntry("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*").isPresent());
  }

  private String crnNumbered(int kafkas, int n, boolean pattern) {
    String k = pattern && (n % kafkas == 0) ? "*" : Integer.toString(n % kafkas);
    String t = pattern && (n / kafkas == 0) ? "*" : Integer.toString(n / kafkas);
    return String.format("crn://confluent.cloud/kafka=%s/topic=clicks%s", k, t);
  }

  /*
   * The volume tests below can be instructive while developing, but should not be run by
   * Jenkins because they would be flaky.
   */
  private long volume(int capacity, int patterns, int trials) throws CrnSyntaxException {

    CachedCrnStringPatternMatcher.Builder<Integer> builder =
        CachedCrnStringPatternMatcher.<Integer>builder().capacity(capacity);

    for (int i = 0; i < patterns; i++) {
      String crn = crnNumbered(10, i, true);
      builder.setPattern(crn, i);
    }

    CachedCrnStringPatternMatcher<Integer> matcher = builder.build();

    Random random = new Random();
    long start = System.currentTimeMillis();
    for (int j = 0; j < trials; j++) {
      int l = Math.min(patterns - 1, Math.max(0,
          (int) (((2 + random.nextGaussian()) / 4) * patterns)));
      Assert.assertEquals(l, (int) matcher.match(crnNumbered(10, l, false)));
    }
    long end = System.currentTimeMillis();
    return end - start;
  }

  @Ignore("Performance baseline dependent on testing hardware")
  @Test
  public void testVolumeCacheEviction() throws CrnSyntaxException {
    long elapsed = volume(500, 1000, 1000000);
    System.out.println("elapsed: " + elapsed);
    Assert.assertTrue(elapsed < 10000);
  }

  @Ignore("Performance baseline dependent on testing hardware")
  @Test
  public void testVolumeEnoughCache() throws CrnSyntaxException {
    long elapsed = volume(2000, 1000, 1000000);
    System.out.println("elapsed: " + elapsed);
    Assert.assertTrue(elapsed < 2000);
  }
}
