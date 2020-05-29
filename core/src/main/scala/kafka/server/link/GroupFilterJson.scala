package kafka.server.link

import java.io.IOException

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.fasterxml.jackson.core.{JsonParseException, JsonProcessingException}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonMappingException, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.config.{ConfigDef, ConfigException}
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.utils.SecurityUtils

import scala.collection.mutable

/* Example consumer.group.filters.json:
{
"groupFilters": [
  {
     "name": "foo",
     "patternType": "PREFIXED",
     "filterType": "WHITELIST"
  },
  {
     "name": "foobar",
     "patternType": "LITERAL",
     "filterType": "BLACKLIST"
  },
    {
     "name": "*",
     "patternType": "LITERAL"
  }
]
}
*/

object FilterType extends Enumeration {
  type FilterType = Value

  final val WHITELIST_OPT = "WHITELIST"
  final val BLACKLIST_OPT = "BLACKLIST"

  val WHITELIST = Value(WHITELIST_OPT)
  val BLACKLIST = Value(BLACKLIST_OPT)

  def fromString(s: String): Option[Value] = values.find(_.toString == s.toUpperCase)
}


object GroupFilterJson {
  val VALIDATOR = new GroupFilterJsonValidator
  val JSON_SERDE = new ObjectMapper()
  JSON_SERDE.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
  JSON_SERDE.registerModule(DefaultScalaModule)

  def parse(value: String): Option[GroupFiltersJson] = {
    if (value == null || value.trim.isEmpty) {
      // Since we have configure the empty string "" as the default value for
      // ClusterLinkConfig.ConsumerGroupFiltersProp, we need this to succeed and return the empty value
      // for the acl migration json
      Option.empty
    } else {
      try {
        val JSON_SERDE = new ObjectMapper()
        JSON_SERDE.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
        JSON_SERDE.registerModule(DefaultScalaModule)
        val groupFilterJson = JSON_SERDE.readValue(value, classOf[GroupFiltersJson])

        if (groupFilterJson.groupFilters == null) {
          throw new IllegalArgumentException("groupFilters cannot be the JSON null")
        }

        for (groupFilter <- groupFilterJson.groupFilters) {
          val filterType = groupFilter.filterType

          if (FilterType.fromString(filterType).isEmpty) {
            throw new IllegalArgumentException(s"Unknown filterType: $filterType")
          }

          val patternType = groupFilter.patternType
          if (patternType == null) {
            throw new IllegalArgumentException("patternType field may not be null.")
          }
          if (patternType.isEmpty) {
            throw new IllegalArgumentException("patternType field may not be empty.")
          }
          if (SecurityUtils.patternType(patternType) == PatternType.UNKNOWN) {
            throw new IllegalArgumentException(s"Unknown patternType: $patternType")
          }
        }
        Some(groupFilterJson)
      } catch {
        case e @ (_ : IOException | _ : JsonMappingException | _ : JsonParseException)  =>
          throw new IllegalArgumentException("Exception while parsing Group Filter JSON: " + e)
      }
    }
  }

}

final class GroupFilterJsonValidator extends ConfigDef.Validator {
  override def ensureValid(name: String, value: Any): Unit = {
    if (value != null) {
      try {
        GroupFilterJson.parse(value.asInstanceOf[String])
      }
      catch {
        case e: IllegalArgumentException =>
          throw new ConfigException(name, value, e.getMessage)
      }
    }
  }
}

@JsonCreator
case class GroupFiltersJson(@JsonProperty ("groupFilters") groupFilters: mutable.ListBuffer[GroupFilter]) {

  def toJson: String = {
    try {
      GroupFilterJson.JSON_SERDE.writeValueAsString(this)
    }
    catch {
      case e: JsonProcessingException =>
        throw new RuntimeException(e)
    }
  }

  override def toString: String = s"GroupFiltersJson(groupFilters=$groupFilters)"
}

@JsonCreator
case class GroupFilter(@JsonProperty ("name") name : String,
                       @JsonProperty ("patternType") patternType: String,
                       @JsonProperty (value="filterType", required = false, defaultValue = FilterType.WHITELIST_OPT) filterType: String) {

  override def toString: String =
    s"GroupFilter(name=$name,filterType=$filterType,patternType=$patternType)"
}