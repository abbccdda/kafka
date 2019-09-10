/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.crn;

import io.confluent.crn.ConfluentResourceName.Element;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

public class ConfluentResourceNameTest {

  @Test
  public void testBuildCrnKafka() throws CrnSyntaxException {
    ConfluentResourceName crn = ConfluentResourceName.newBuilder()
        .setAuthority("confluent.cloud")
        .addElement("kafka", "lkc-a1b2c3")
        .build();

    Assert.assertNotNull(crn);
    Assert.assertEquals("confluent.cloud", crn.authority());
    Assert.assertEquals(1, crn.elements().size());
    Element element = crn.elements().get(0);
    Assert.assertEquals("kafka", element.resourceType());
    Assert.assertEquals("lkc-a1b2c3", element.encodedResourceName());
  }

  @Test
  public void testBuildCrnKafkaTopic() throws CrnSyntaxException {
    ConfluentResourceName crn = ConfluentResourceName.newBuilder()
        .setAuthority("confluent.cloud")
        .addElement("kafka", "lkc-a1b2c3")
        .addElement("topic", "clicks")
        .build();

    Assert.assertNotNull(crn);
    Assert.assertEquals("confluent.cloud", crn.authority());
    Assert.assertEquals(2, crn.elements().size());
    Element cluster = crn.elements().get(0);
    Assert.assertEquals("kafka", cluster.resourceType());
    Assert.assertEquals("lkc-a1b2c3", cluster.encodedResourceName());
    Element topic = crn.elements().get(1);
    Assert.assertEquals("topic", topic.resourceType());
    Assert.assertEquals("clicks", topic.encodedResourceName());
  }

  @Test
  public void testBuildCrnKafkaTopicPrefix() throws CrnSyntaxException {
    ConfluentResourceName crn = ConfluentResourceName.newBuilder()
        .setAuthority("confluent.cloud")
        .addElement("kafka", "lkc-a1b2c3")
        .addElementWithWildcard("topic", "clicks-")
        .build();

    Assert.assertNotNull(crn);
    Assert.assertEquals("confluent.cloud", crn.authority());
    Assert.assertEquals(2, crn.elements().size());
    Element cluster = crn.elements().get(0);
    Assert.assertEquals("kafka", cluster.resourceType());
    Assert.assertEquals("lkc-a1b2c3", cluster.encodedResourceName());
    Element topic = crn.elements().get(1);
    Assert.assertEquals("topic", topic.resourceType());
    Assert.assertEquals("clicks-*", topic.encodedResourceName());
  }


  @Test
  public void testBuildCrnKafkaTopicAny() throws CrnSyntaxException {
    ConfluentResourceName crn = ConfluentResourceName.newBuilder()
        .setAuthority("confluent.cloud")
        .addElement("kafka", "lkc-a1b2c3")
        .addElementWithWildcard("topic", "")
        .build();

    Assert.assertNotNull(crn);
    Assert.assertEquals("confluent.cloud", crn.authority());
    Assert.assertEquals(2, crn.elements().size());
    Element cluster = crn.elements().get(0);
    Assert.assertEquals("kafka", cluster.resourceType());
    Assert.assertEquals("lkc-a1b2c3", cluster.encodedResourceName());
    Element topic = crn.elements().get(1);
    Assert.assertEquals("topic", topic.resourceType());
    Assert.assertEquals("*", topic.encodedResourceName());
  }

  @Test
  public void testBuildCrnGroupWithStar() throws CrnSyntaxException {
    ConfluentResourceName crn = ConfluentResourceName.newBuilder()
        .setAuthority("confluent.cloud")
        .addElement("kafka", "lkc-a1b2c3")
        .addElement("group", "abc*def")
        .build();

    Assert.assertNotNull(crn);
    Assert.assertEquals("confluent.cloud", crn.authority());
    Assert.assertEquals(2, crn.elements().size());
    Element cluster = crn.elements().get(0);
    Assert.assertEquals("kafka", cluster.resourceType());
    Assert.assertEquals("lkc-a1b2c3", cluster.encodedResourceName());
    Element topic = crn.elements().get(1);
    Assert.assertEquals("group", topic.resourceType());
    Assert.assertEquals("abc%2Adef", topic.encodedResourceName());
  }


  @Test
  public void testBuildCrnGroupWithStarPrefix() throws CrnSyntaxException {
    ConfluentResourceName crn = ConfluentResourceName.newBuilder()
        .setAuthority("confluent.cloud")
        .addElement("kafka", "lkc-a1b2c3")
        .addElementWithWildcard("group", "abc*def")
        .build();

    Assert.assertNotNull(crn);
    Assert.assertEquals("confluent.cloud", crn.authority());
    Assert.assertEquals(2, crn.elements().size());
    Element cluster = crn.elements().get(0);
    Assert.assertEquals("kafka", cluster.resourceType());
    Assert.assertEquals("lkc-a1b2c3", cluster.encodedResourceName());
    Element topic = crn.elements().get(1);
    Assert.assertEquals("group", topic.resourceType());
    Assert.assertEquals("abc%2Adef*", topic.encodedResourceName());
  }

  @Test
  public void testBuildCrnNoAuthority() throws CrnSyntaxException {
    ConfluentResourceName.Builder builder = ConfluentResourceName.newBuilder()
        .addElement("kafka", "lkc-a1b2c3")
        .addElement("topic", "clicks");

    Assert.assertThrows(CrnSyntaxException.class, builder::build);
  }

  @Test
  public void testBuildCrnNoElements() {
    ConfluentResourceName.Builder builder = ConfluentResourceName.newBuilder()
        .setAuthority("confluent.cloud");

    Assert.assertThrows(CrnSyntaxException.class, builder::build);
  }

  @Test
  public void testToString() throws CrnSyntaxException {
    ConfluentResourceName crn = ConfluentResourceName.newBuilder()
        .setAuthority("confluent.cloud")
        .addElement("kafka", "lkc-a1b2c3")
        .addElement("topic", "clicks")
        .build();

    Assert.assertNotNull(crn);
    Assert.assertEquals("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks", crn.toString());
  }


  @Test
  public void testFromString() throws CrnSyntaxException {
    ConfluentResourceName crn = ConfluentResourceName.fromString(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks");

    Assert.assertNotNull(crn);
    Assert.assertEquals("confluent.cloud", crn.authority());
    Assert.assertEquals(2, crn.elements().size());
    Element cluster = crn.elements().get(0);
    Assert.assertEquals("kafka", cluster.resourceType());
    Assert.assertEquals("lkc-a1b2c3", cluster.encodedResourceName());
    Element topic = crn.elements().get(1);
    Assert.assertEquals("topic", topic.resourceType());
    Assert.assertEquals("clicks", topic.encodedResourceName());
  }

  @Test
  public void testFromStringNoAuthority() {
    Assert.assertThrows(CrnSyntaxException.class, () -> {
      ConfluentResourceName.fromString(
          "crn:/kafka=lkc-a1b2c3/topic=clicks");
    });
  }

  @Test
  public void testFromStringNoElements() {
    Assert.assertThrows(CrnSyntaxException.class, () -> {
      ConfluentResourceName.fromString(
          "crn://confluent.cloud/");
    });
  }

  @Test
  public void testFromStringMalformed() {
    Assert.assertThrows(CrnSyntaxException.class, () -> {
      ConfluentResourceName.fromString(
          "crn://confluent.cloud/kafka:lkc-a1b2c3");
    });
    Assert.assertThrows(CrnSyntaxException.class, () -> {
      ConfluentResourceName.fromString(
          "crn://confluent.cloud/kafka/topic=clicks");
    });
    Assert.assertThrows(CrnSyntaxException.class, () -> {
      ConfluentResourceName.fromString(
          "crn://confluent.cloud/kafka=lkc-a1b2c3/topic");
    });
  }

  @Test
  public void testLastResourceElementKafka() throws CrnSyntaxException {
    ConfluentResourceName crn = ConfluentResourceName.newBuilder()
        .setAuthority("confluent.cloud")
        .addElement("kafka", "lkc-a1b2c3")
        .build();

    Assert.assertEquals("kafka", crn.lastResourceElement().resourceType());
    Assert.assertEquals("lkc-a1b2c3", crn.lastResourceElement().encodedResourceName());
  }

  @Test
  public void testLastResourceElementKafkaTopic() throws CrnSyntaxException {
    ConfluentResourceName crn = ConfluentResourceName.newBuilder()
        .setAuthority("confluent.cloud")
        .addElement("kafka", "lkc-a1b2c3")
        .addElement("topic", "clicks")
        .build();

    Assert.assertEquals("topic", crn.lastResourceElement().resourceType());
    Assert.assertEquals("clicks", crn.lastResourceElement().encodedResourceName());
  }

  @Test
  public void testBadElements() {
    ConfluentResourceName.Builder builder = ConfluentResourceName.newBuilder()
        .setAuthority("confluent.cloud");
    Assert.assertThrows(CrnSyntaxException.class, () -> {
      builder.addElement("kaf=ka", "lkc-a1b2c3");
    });
    Assert.assertThrows(CrnSyntaxException.class, () -> {
      builder.addElement("kaf/ka", "lkc-a1b2c3");
    });
  }

  @Test
  public void testBadScheme() {
    Assert.assertThrows(CrnSyntaxException.class, () -> {
      ConfluentResourceName.fromString("http://confluent.cloud/kafka=a1b2c3");
    });
  }

  @Test
  public void testNotURI() {
    Assert.assertThrows(CrnSyntaxException.class, () -> {
      ConfluentResourceName.fromString("twas brillig and the slithy toves");
    });
  }

  @Test
  public void testEquals() throws CrnSyntaxException {
    Assert.assertEquals(ConfluentResourceName.fromString(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks"),
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks")
    );
    Assert.assertNotEquals(ConfluentResourceName.fromString(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks"),
        ConfluentResourceName.fromString(
            "crn://mds.example.com/kafka=lkc-a1b2c3/topic=clicks")
    );
    Assert.assertNotEquals(ConfluentResourceName.fromString(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks"),
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clocks")
    );
    Assert.assertNotEquals(ConfluentResourceName.fromString(
        "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks*"),
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks")
    );
    Assert.assertNotEquals(ConfluentResourceName.fromString(
        "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3/topic=clicks"),
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks")
    );
    Assert.assertNotEquals(ConfluentResourceName.fromString(
        "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3"),
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3/topic=clicks")
    );
  }

  @Test
  public void testComparePrefixes() throws CrnSyntaxException {
    Assert.assertEquals(1,
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks")
            .compareTo(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks*")
            ));
    Assert.assertEquals(-1,
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks*")
            .compareTo(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks")
            ));
    Assert.assertEquals(0,
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks*")
            .compareTo(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks*")
            ));
  }

  @Test
  public void testSort() throws CrnSyntaxException {
    List<String> unsorted = Arrays.asList(
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
        "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3/topic=*");

    List<String> sorted = unsorted.stream()
        .flatMap(s -> {
          try {
            return Stream.of(ConfluentResourceName.fromString(s));
          } catch (CrnSyntaxException e) {
            return Stream.empty();
          }
        }).sorted()
        .map(ConfluentResourceName::toString)
        .collect(Collectors.toList()); //Collectors.joining("\",\n\""));

    Assert.assertEquals(
        Arrays.asList(
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*",
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=cli*",
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks*",
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks",
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks2",
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clocks",
            "crn://confluent.cloud/organization=123/kafka=*/topic=*",
            "crn://confluent.cloud/organization=123/kafka=*/topic=clicks",
            "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3",
            "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3/topic=*",
            "crn://confluent.cloud/organization=123/kafka=lkc-a1b2c3/topic=clicks"),
        sorted);
  }
}
