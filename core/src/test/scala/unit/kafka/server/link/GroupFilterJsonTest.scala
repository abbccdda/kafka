package unit.kafka.server.link

import kafka.server.link.GroupFilterJson
import org.junit.Assert.{assertEquals, assertThrows, assertTrue}
import org.junit.Test

class GroupFilterJsonTest {
  @Test
  def testEmptyStringParsesToEmptyGroupFiltersJson(): Unit = {
    assertTrue(GroupFilterJson.parse("").isEmpty)
  }

  @Test
  def testValidStringParsesToGroupFiltersJson(): Unit = {
    val groupFiltersJson = "{\n  \"groupFilters\": [ { \"name\": \"*\", \"patternType\": \"LITERAL\", \"filterType\": \"INCLUDE\" } ]}"
    val groupJson = GroupFilterJson.parse(groupFiltersJson)
    assertTrue(groupJson.isDefined)
    val groupFilter = groupJson.get.groupFilters.head
    assertEquals("*", groupFilter.name)
    assertEquals("LITERAL", groupFilter.patternType)
    assertEquals("INCLUDE", groupFilter.filterType)
  }

  @Test
  def testNullJsonArgument(): Unit = {
    val thrown = assertThrows(classOf[IllegalArgumentException], () => GroupFilterJson.parse("{}"))
    assertEquals("groupFilters cannot be the JSON null", thrown.getMessage)
  }

  @Test
  def testInvalidFilterType(): Unit = {
    val groupFiltersJson = "{\n  \"groupFilters\": [ { \"name\": \"*\", \"patternType\": \"LITERAL\", \"filterType\": \"FOO\" } ]}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => GroupFilterJson.parse(groupFiltersJson))
    assertEquals("Unknown filterType: FOO", thrown.getMessage)
  }

  @Test
  def testMissingPatternType(): Unit = {
    val groupFiltersJson = "{\n  \"groupFilters\": [ { \"name\": \"*\", \"filterType\": \"INCLUDE\" } ]}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => GroupFilterJson.parse(groupFiltersJson))
    assertEquals("patternType field may not be null.", thrown.getMessage)

  }

  @Test
  def testInvalidPatternType(): Unit = {
    val groupFiltersJson = "{\n  \"groupFilters\": [ { \"name\": \"*\", \"patternType\": \"FOO\", \"filterType\": \"INCLUDE\" } ]}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => GroupFilterJson.parse(groupFiltersJson))
    assertEquals("Unknown patternType: FOO", thrown.getMessage)
  }

  @Test
  def testEmptyPatternType(): Unit = {
    val groupFiltersJson = "{\n  \"groupFilters\": [ { \"name\": \"*\", \"patternType\": \"\", \"filterType\": \"INCLUDE\" } ]}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => GroupFilterJson.parse(groupFiltersJson))
    assertEquals("patternType field may not be empty.", thrown.getMessage)
  }

}
