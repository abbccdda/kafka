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

    CrnPatternMatcher<String> matcher = new CrnPatternMatcher<>();
    patterns.stream()
        .flatMap(s -> {
          try {
            return Stream.of(ConfluentResourceName.fromString(s));
          } catch (CrnSyntaxException e) {
            return Stream.empty();
          }
        })
        .forEach(p -> matcher.setPattern(p, p.toString()));

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

    Assert.assertNull(matcher.match(ConfluentResourceName.fromString(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/group=clicks")));
    Assert.assertNull(matcher.match(ConfluentResourceName.fromString(
        "crn://confluent.cloud/organization=123/kafka=lkc-d4e5f6")));
    Assert.assertNull(matcher.match(ConfluentResourceName.fromString(
        "crn://confluent.cloud/topic=clicks")));
    Assert.assertNull(matcher.match(ConfluentResourceName.fromString(
        "crn://confluent.cloud/organization=123/topic=clicks")));
  }

  @Test
  public void testCachedMatch() {
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

    CachedCrnStringPatternMatcher<String> matcher = new CachedCrnStringPatternMatcher<>(100);
    patterns
        .forEach(p -> {
          try {
            matcher.setPattern(p, p);
          } catch (CrnSyntaxException e) {
            e.printStackTrace();
          }
        });

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
  public void testMatchAfterCacheInvalidated() throws CrnSyntaxException {
    CachedCrnStringPatternMatcher<String> matcher =
        new CachedCrnStringPatternMatcher<>(100);
    matcher.setPattern("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*");
    Assert.assertEquals("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*",
        matcher.match("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks"));
    Assert.assertEquals(1, matcher.size());
    matcher.setPattern("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks");
    Assert.assertEquals(0, matcher.size());
    Assert.assertEquals("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks",
        matcher.match("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks"));
    Assert.assertEquals(1, matcher.size());
  }

  @Test
  public void testLRU() throws CrnSyntaxException {
    CachedCrnStringPatternMatcher<String> matcher =
        new CachedCrnStringPatternMatcher<>(3);
    matcher.setPattern("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*",
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*");

    for (int i = 0; i < 10; i++) {
      Assert.assertEquals("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*",
          matcher.match("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks" + i));
      Assert.assertEquals(Math.min(i + 1, 3), matcher.size());
    }
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
    CachedCrnStringPatternMatcher<Integer> matcher =
        new CachedCrnStringPatternMatcher<>(capacity);

    for (int i = 0; i < patterns; i++) {
      String crn = crnNumbered(10, i, true);
      matcher.setPattern(crnNumbered(10, i, true), i);
    }

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
