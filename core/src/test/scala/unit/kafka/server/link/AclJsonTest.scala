package unit.kafka.server.link

import kafka.server.link.AclJson
import org.junit.Assert.{assertEquals, assertNull, assertThrows, assertTrue}
import org.junit.Test

class AclJsonTest {
  @Test
  def testEmptyStringParsesToEmptyAclFiltersJson(): Unit = {
    assertTrue(AclJson.parse("").isEmpty)
  }

  @Test
  def testValidStringParsesToAclFiltersJson(): Unit = {
    val aclFiltersJson = "{\"aclFilters\": [{" + "\"resourceFilter\": {" + "\"resourceType\": \"any\"," + " \"patternType\": \"any\"" + "}, " + "\"accessFilter\": {" + "\"operation\": \"any\"," + " \"permissionType\": \"any\"" + "}" + "}]" + "}"
    val aclJson = AclJson.parse(aclFiltersJson)
    assertTrue(aclJson.isDefined)
    val aclFilter = aclJson.get.aclFilters.head
    assertEquals("any", aclFilter.resourceFilter.resourceType)
    assertEquals("any", aclFilter.resourceFilter.patternType)
    assertEquals("any", aclFilter.accessFilter.operation)
    assertEquals("any", aclFilter.accessFilter.permissionType)
    assertNull(aclFilter.resourceFilter.name)
    assertNull(aclFilter.accessFilter.host)
    assertNull(aclFilter.accessFilter.principal)
  }

  @Test
  def testNullJsonArgument(): Unit = {
    val thrown = assertThrows(classOf[IllegalArgumentException], () => AclJson.parse("{}"))
    assertEquals("acl.filters cannot be the JSON null", thrown.getMessage)
  }

  @Test
  def testMissingResourceType(): Unit = {
    val aclFiltersJson = "{\"aclFilters\": [{" + "\"resourceFilter\": {" + " \"patternType\": \"any\"" + "}, " + "\"accessFilter\": {" + "\"operation\": \"any\"," + " \"permissionType\": \"any\"" + "}" + "}]" + "}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => AclJson.parse(aclFiltersJson))
    assertEquals("resourceType field may not be null.", thrown.getMessage)
  }

  @Test
  def testMissingResourceTypeValue(): Unit = {
    val aclFiltersJson = "{\"aclFilters\": [{" + "\"resourceFilter\": {" + "\"resourceType\": \"\"," + " \"patternType\": \"any\"" + "}, " + "\"accessFilter\": {" + "\"operation\": \"any\"," + " \"permissionType\": \"any\"" + "}" + "}]" + "}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => AclJson.parse(aclFiltersJson))
    assertEquals("resourceType field may not be empty.", thrown.getMessage)
  }

  @Test
  def testInvalidResourceType(): Unit = {
    val aclFiltersJson = "{\"aclFilters\": [{" + "\"resourceFilter\": {" + "\"resourceType\": \"foo\"," + " \"patternType\": \"any\"" + "}, " + "\"accessFilter\": {" + "\"operation\": \"any\"," + " \"permissionType\": \"any\"" + "}" + "}]" + "}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => AclJson.parse(aclFiltersJson))
    assertEquals("Unknown resourceType: foo", thrown.getMessage)
  }

  @Test
  def testMissingPatternType(): Unit = {
    val aclFiltersJson = "{\"aclFilters\": [{" + "\"resourceFilter\": {" + "\"resourceType\": \"any\"" + "}, " + "\"accessFilter\": {" + "\"operation\": \"any\"," + " \"permissionType\": \"any\"" + "}" + "}]" + "}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => AclJson.parse(aclFiltersJson))
    assertEquals("patternType field may not be null.", thrown.getMessage)
  }

  @Test
  def testMissingPatternTypeValue(): Unit = {
    val aclFiltersJson = "{\"aclFilters\": [{" + "\"resourceFilter\": {" + "\"resourceType\": \"any\"," + " \"patternType\": \"\"" + "}, " + "\"accessFilter\": {" + "\"operation\": \"any\"," + " \"permissionType\": \"any\"" + "}" + "}]" + "}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => AclJson.parse(aclFiltersJson))
    assertEquals("patternType field may not be empty.", thrown.getMessage)
  }

  @Test
  def testInvalidPatternType(): Unit = {
    val aclFiltersJson = "{\"aclFilters\": [{" + "\"resourceFilter\": {" + "\"resourceType\": \"any\"," + " \"patternType\": \"foo\"" + "}, " + "\"accessFilter\": {" + "\"operation\": \"any\"," + " \"permissionType\": \"any\"" + "}" + "}]" + "}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => AclJson.parse(aclFiltersJson))
    assertEquals("Unknown patternType: foo", thrown.getMessage)
  }

  @Test
  def testMissingOperation(): Unit = {
    val aclFiltersJson = "{\"aclFilters\": [{" + "\"resourceFilter\": {" + "\"resourceType\": \"any\"," + " \"patternType\": \"any\"" + "}, " + "\"accessFilter\": {" + " \"permissionType\": \"any\"" + "}" + "}]" + "}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => AclJson.parse(aclFiltersJson))
    assertEquals("operation field may not be null.", thrown.getMessage)
  }

  @Test
  def testMissingOperationValue(): Unit = {
    val aclFiltersJson = "{\"aclFilters\": [{" + "\"resourceFilter\": {" + "\"resourceType\": \"any\"," + " \"patternType\": \"any\"" + "}, " + "\"accessFilter\": {" + "\"operation\": \"\"," + " \"permissionType\": \"any\"" + "}" + "}]" + "}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => AclJson.parse(aclFiltersJson))
    assertEquals("operation field may not be empty.", thrown.getMessage)
  }

  @Test
  def testInvalidOperation(): Unit = {
    val aclFiltersJson = "{\"aclFilters\": [{" + "\"resourceFilter\": {" + "\"resourceType\": \"any\"," + " \"patternType\": \"any\"" + "}, " + "\"accessFilter\": {" + "\"operation\": \"foo\"," + " \"permissionType\": \"any\"" + "}" + "}]" + "}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => AclJson.parse(aclFiltersJson))
    assertEquals("Unknown operation: foo", thrown.getMessage)
  }

  @Test
  def testMissingPermissionType(): Unit = {
    val aclFiltersJson = "{\"aclFilters\": [{" + "\"resourceFilter\": {" + "\"resourceType\": \"any\"," + " \"patternType\": \"any\"" + "}, " + "\"accessFilter\": {" + "\"operation\": \"any\"" + "}" + "}]" + "}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => AclJson.parse(aclFiltersJson))
    assertEquals("permissionType field may not be null.", thrown.getMessage)
  }

  @Test
  def testMissingPermissionTypeValue(): Unit = {
    val aclFiltersJson = "{\"aclFilters\": [{" + "\"resourceFilter\": {" + "\"resourceType\": \"any\"," + " \"patternType\": \"any\"" + "}, " + "\"accessFilter\": {" + "\"operation\": \"any\"," + " \"permissionType\": \"\"" + "}" + "}]" + "}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => AclJson.parse(aclFiltersJson))
    assertEquals("permissionType field may not be empty.", thrown.getMessage)
  }

  @Test
  def testInvalidPermissionType(): Unit = {
    val aclFiltersJson = "{\"aclFilters\": [{" + "\"resourceFilter\": {" + "\"resourceType\": \"any\"," + " \"patternType\": \"any\"" + "}, " + "\"accessFilter\": {" + "\"operation\": \"any\"," + " \"permissionType\": \"foo\"" + "}" + "}]" + "}"
    val thrown = assertThrows(classOf[IllegalArgumentException], () => AclJson.parse(aclFiltersJson))
    assertEquals("Unknown permissionType: foo", thrown.getMessage)
  }
}
