package kafka.server.link

import java.io.IOException

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.fasterxml.jackson.core.{JsonParseException, JsonProcessingException}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonMappingException, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.kafka.common.acl.{AccessControlEntryFilter, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.config.{ConfigDef, ConfigException}
import org.apache.kafka.common.resource.{PatternType, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.utils.SecurityUtils

import scala.collection.mutable

/* Example acl.filters.json:
{
  "aclFilters": [
    {
      "resourceFilter": {
        "resourceType": "topic",
        "patternType": "literal"
      },
      "accessFilter": {
        "principal": "User:Alice",
        "host": "goodHost",
        "operation": "any",
        "permissionType": "allow"
      }
    },
   {
      "resourceFilter": {
        "resourceType": "topic",
        "name": "foo",
        "patternType": "prefixed"
      },
      "accessFilter": {
        "principal": "User:Bob",
        "host": "anotherGoodHost",
        "operation": "describeConfigs",
        "permissionType": "allow"
      }
    },
   {
      "resourceFilter": {
        "resourceType": "topic",
        "patternType": "literal"
      },
      "accessFilter": {
        "principal": "User:Mallory",
        "operation": "all",
        "permissionType": "deny"
      }
    }
  ]
}
 */

object AclJson {
  val VALIDATOR = new AclMigrationJsonValidator
  val JSON_SERDE = new ObjectMapper()
  JSON_SERDE.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
  JSON_SERDE.registerModule(DefaultScalaModule)

  def toAclBindingFilters(json: AclFiltersJson): mutable.ListBuffer[AclBindingFilter] = {
    val aclBindingFilterList: mutable.ListBuffer[AclBindingFilter] = mutable.ListBuffer[AclBindingFilter]()
    for (aclFilter <- json.aclFilters) {
      val resourceType: ResourceType = SecurityUtils.resourceType(aclFilter.resourceFilter.resourceType)
      val pattern: PatternType = SecurityUtils.patternType(aclFilter.resourceFilter.patternType)
      val operation: AclOperation = SecurityUtils.operation(aclFilter.accessFilter.operation)
      val permission: AclPermissionType = SecurityUtils.permissionType(aclFilter.accessFilter.permissionType)

      val name: String = aclFilter.resourceFilter.name
      val principal: String = aclFilter.accessFilter.principal
      val host: String = aclFilter.accessFilter.host

      val resourcePatternFilter: ResourcePatternFilter = new ResourcePatternFilter(resourceType, name, pattern)
      val accessControlEntryFilter: AccessControlEntryFilter = new AccessControlEntryFilter(principal, host, operation, permission)
      val aclBindingFilter: AclBindingFilter = new AclBindingFilter(resourcePatternFilter, accessControlEntryFilter)
      aclBindingFilterList += aclBindingFilter
    }
    aclBindingFilterList
  }

  def parse(value: String): Option[AclFiltersJson] = {
    if (value == null || value.trim.isEmpty) {
      // Since we have configure the empty string "" as the default value for
      // ClusterLinkConfig.AclFiltersJsonProp, we need this to succeed and return the empty value
      // for the acl migration json
      Option.empty
    } else {
      try {
        val JSON_SERDE = new ObjectMapper()
        JSON_SERDE.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
        JSON_SERDE.registerModule(DefaultScalaModule)
        val aclJson = JSON_SERDE.readValue(value, classOf[AclFiltersJson])

        if (aclJson.aclFilters == null) {
          throw new IllegalArgumentException("acl.filters cannot be the JSON null")
        }

        if (aclJson.aclFilters.isEmpty) {
          throw new IllegalArgumentException("aclFilters field cannot be empty")
        }

        for (aclFilter <- aclJson.aclFilters) {
          val resourceType = aclFilter.resourceFilter.resourceType
          if (resourceType == null) {
            throw new IllegalArgumentException("resourceType field may not be null.")
          }
          if (resourceType.isEmpty) {
            throw new IllegalArgumentException("resourceType field may not be empty.")
          }
          if (SecurityUtils.resourceType(resourceType) == ResourceType.UNKNOWN) {
            throw new IllegalArgumentException("Unknown resourceType: " + resourceType)
          }

          val patternType = aclFilter.resourceFilter.patternType
          if (patternType == null) {
            throw new IllegalArgumentException("patternType field may not be null.")
          }
          if (patternType.isEmpty) {
            throw new IllegalArgumentException("patternType field may not be empty.")
          }
          if (SecurityUtils.patternType(patternType) == PatternType.UNKNOWN) {
            throw new IllegalArgumentException("Unknown patternType: " + patternType)
          }

          val permissionType = aclFilter.accessFilter.permissionType
          if (permissionType == null) {
            throw new IllegalArgumentException("permissionType field may not be null.")
          }
          if (permissionType.isEmpty) {
            throw new IllegalArgumentException("permissionType field may not be empty.")
          }
          if (SecurityUtils.permissionType(permissionType) == AclPermissionType.UNKNOWN) {
            throw new IllegalArgumentException("Unknown permissionType: " + permissionType)
          }

          val operation = aclFilter.accessFilter.operation
          if (operation == null) {
            throw new IllegalArgumentException("operation field may not be null.")
          }
          if (operation.isEmpty) {
            throw new IllegalArgumentException("operation field may not be empty.")
          }
          if (SecurityUtils.operation(operation) == AclOperation.UNKNOWN) {
            throw new IllegalArgumentException("Unknown operation: " + operation)
          }

          val principal = aclFilter.accessFilter.principal
          if (principal != null) {
            SecurityUtils.parseKafkaPrincipal(principal)
          }
        }
        Some(aclJson)
      } catch {
        case e @ (_ : IOException | _ : JsonMappingException | _ : JsonParseException)  =>
          throw new IllegalArgumentException("Exception while parsing ACL JSON: " + e)
      }
    }
  }
}

final class AclMigrationJsonValidator extends ConfigDef.Validator {
  override def ensureValid(name: String, value: Any): Unit = {
    if (value != null) {
      try {
        AclJson.parse(value.asInstanceOf[String])
      }
      catch {
        case e: IllegalArgumentException =>
          throw new ConfigException(name, value, e.getMessage)
      }
    }
  }
}

@JsonCreator
case class AclFiltersJson(@JsonProperty ("aclFilters") aclFilters: mutable.ListBuffer[AclFilter]) {

  def toJson: String = {
    try {
      AclJson.JSON_SERDE.writeValueAsString(this)
    }
    catch {
      case e: JsonProcessingException =>
        throw new RuntimeException(e)
    }
  }

  override def toString: String = "AclFiltersJson(" + "aclFilters=" + aclFilters + ")"
}

@JsonCreator
case class AclFilter(@JsonProperty ("resourceFilter") resourceFilter: ResourceFilter,
                     @JsonProperty ("accessFilter") accessFilter: AccessFilter) {

  override def toString: String =
    "AclFilter(" + "resourceFilter=" + resourceFilter + ",accessFilter=" + accessFilter + ")"
}

@JsonCreator
case class ResourceFilter(@JsonProperty ("resourceType") resourceType: String,
                          @JsonProperty ("name") name: String,
                          @JsonProperty ("patternType") patternType: String) {

  override def toString: String =
    "ResourceFilter(" + "resourceType=" + resourceType + ",name=" + name + ",patternType=" +
      patternType + ")"
}

@JsonCreator
case class AccessFilter(@JsonProperty ("principal") principal: String,
                        @JsonProperty ("host") host: String,
                        @JsonProperty ("operation") operation: String,
                        @JsonProperty ("permissionType") permissionType: String) {

  override def toString: String =
    "AccessFilter(" + "principal=" + principal + ",host=" + host + ",operation=" + operation +
      ",permissionType=" + permissionType + ")"
}
