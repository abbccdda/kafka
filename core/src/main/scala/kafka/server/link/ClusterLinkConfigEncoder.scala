package kafka.server.link

import java.util
import java.util.Properties

import kafka.server.KafkaConfig
import kafka.server.link.ClusterLinkConfigProvider._
import kafka.utils.PasswordEncoder
import org.apache.kafka.common.config.{AbstractConfig, ConfigData, ConfigDef, ConfigException}
import org.apache.kafka.common.config.provider.ConfigProvider
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.InvalidConfigurationException

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ClusterLinkConfigEncoder(brokerConfig: KafkaConfig) {

  private val encoders: Seq[Encoder] = createEncoders(brokerConfig.values)

  def encode(linkProps: Properties): Properties = {
    val props = new Properties
    val sensitiveProps = mutable.Map[String, String]()
    linkProps.asScala.foreach { case (k, v) =>
      val propType = ClusterLinkConfig.typeOf(k)
      if (propType.isEmpty || propType.contains(ConfigDef.Type.PASSWORD)) {
        sensitiveProps += k -> v
        props.put(k, "$" + s"{$ProviderName:$k}")
      } else {
        props.put(k, v)
      }
    }
    if (sensitiveProps.nonEmpty) {
      if (encoders.isEmpty)
        throw new InvalidConfigurationException(s"${KafkaConfig.PasswordEncoderSecretProp} must be configured for cluster linking with sensitive or custom configs")
      encoders.foreach { encoder =>
        encoder.addEncodedProp("", encoder.secret, props)
        sensitiveProps.foreach { case (k, v) =>
          encoder.addEncodedProp(k, new Password(v), props)
        }
      }
    }
    props
  }

  def decode(persistentProps: Properties): Properties = {
    val providerProps = providerProperties(persistentProps, brokerConfig)
    val encodedProps = new Properties
    persistentProps.entrySet.stream
      .filter(e => e.getKey == AbstractConfig.CONFIG_PROVIDERS_CONFIG || !providerProps.containsKey(e.getKey))
      .forEach { e => encodedProps.put(e.getKey, e.getValue) }
    val decodedProps = new Properties
    new DecodingConfig(encodedProps, providerProps).originals.forEach((k, v) => decodedProps.put(k, v))
    decodedProps
  }

  def clusterLinkConfig(persistentProps: Properties): ClusterLinkConfig = {
    val decodedProps = decode(persistentProps)
    new ClusterLinkConfig(decodedProps)
  }

  private def providerProperties(persistentProps: Properties, brokerConfig: KafkaConfig): util.Map[String, String] = {
    if (brokerConfig.passwordEncoderSecret.nonEmpty) {
      val props = new util.HashMap[String, String]()
      props.put(AbstractConfig.CONFIG_PROVIDERS_CONFIG, ProviderName)
      props.put(s"${AbstractConfig.CONFIG_PROVIDERS_CONFIG}.$ProviderName.class", classOf[ClusterLinkConfigProvider].getName)

      ConfigDef.convertToStringMapWithPasswordValues(brokerConfig.values).asScala
        .filter(_._1.startsWith("password.encoder."))
        .foreach { case (k, v) => props.put(s"$ProviderParamPrefix$k", v) }

      persistentProps.asScala.filter(_._1.startsWith(ProviderParamPrefix)).foreach { case (k, v) =>
        props.put(k, v)
      }
      props
    } else {
      util.Collections.emptyMap[String, String]
    }
  }

  private class DecodingConfig(props: util.Map[_, _], configProviderProps: util.Map[String, _])
    extends AbstractConfig(new ConfigDef(), props, configProviderProps, false)
}

private[link] object ClusterLinkConfigProvider {
  val ProviderName = "clusterlink"
  val ProviderParamPrefix = s"${AbstractConfig.CONFIG_PROVIDERS_CONFIG}.$ProviderName.param."
  val EncryptedConfigPrefix = "encrypted."

  private[link] def createEncoders(configs: util.Map[String, _]): Seq[Encoder] = {
    def encoderSecret(configName: String): Option[Password] = {
      Option(configs.get(configName)).map {
        case secret: Password => secret
        case secret: String => new Password(secret)
        case secret => throw new IllegalStateException(s"Unexpected password encoder secret type ${secret.getClass}")
      }
    }
    (encoderSecret(KafkaConfig.PasswordEncoderSecretProp).map { secret =>
      createEncoder(configs, "secret.", secret)
    } ++ encoderSecret(KafkaConfig.PasswordEncoderOldSecretProp).map { secret =>
      createEncoder(configs, "old.secret.", secret)
    }).toSeq
  }

  private def createEncoder(configs: util.Map[String, _], secretPrefix: String, secret: Password): Encoder = {
    val passwordEncoder = new PasswordEncoder(secret,
      Option(configs.get(KafkaConfig.PasswordEncoderKeyFactoryAlgorithmProp)).map(_.toString),
      configs.get(KafkaConfig.PasswordEncoderCipherAlgorithmProp).toString,
      configs.get(KafkaConfig.PasswordEncoderKeyLengthProp).toString.toInt,
      configs.get(KafkaConfig.PasswordEncoderIterationsProp).toString.toInt)
    Encoder(secretPrefix, secret, passwordEncoder)
  }

}

private[link] class ClusterLinkConfigProvider extends ConfigProvider {

  private val configs = new util.HashMap[String, String]()

  override def configure(providerConfigs: util.Map[String, _]): Unit = {
    val encoders = createEncoders(providerConfigs)

    val prefix1 = s"${EncryptedConfigPrefix}secret."
    val prefix2 = s"${EncryptedConfigPrefix}old.secret."
    val encoded1 = providerConfigs.asScala.filter(_._1.startsWith(prefix1))
    val encoded2 = providerConfigs.asScala.filter(_._1.startsWith(prefix2))

    if (encoded1.size > 1 || encoded2.size > 1) {
      val decoded = encoders.exists(_.decode(prefix1, encoded1.toMap, configs)) ||
        encoders.exists(_.decode(prefix2, encoded2.toMap, configs))
      if (!decoded)
        throw new ConfigException("Could not decode configs, secrets don't match")
    }
  }

  override def get(path: String): ConfigData = {
    new ConfigData(configs)
  }

  override def get(path: String, keys: util.Set[String]): ConfigData = {
    val allConfigs = get(path)
    val configs = new util.HashMap[String, String](keys.size)
    allConfigs.data.entrySet.stream.filter(e => keys.contains(e.getKey))
      .forEach(e => configs.put(e.getKey, e.getValue))
    new ConfigData(configs, allConfigs.ttl)
  }

  override def close(): Unit = {
    configs.clear()
  }
}

private[link] case class Encoder(secretPrefix: String, secret: Password, passwordEncoder: PasswordEncoder) {
  def addEncodedProp(key: String, value: Password, props: Properties): Unit = {
    props.put(s"$ProviderParamPrefix$EncryptedConfigPrefix$secretPrefix$key", passwordEncoder.encode(value))
  }

  def decode(secretPrefix: String, encoded: Map[String, _], decoded: util.Map[String, String]): Boolean = {
    val props = encoded.filter(_._1.startsWith(secretPrefix))
    val isMatch = props.get(secretPrefix).exists { encodedSecret =>
      try {
        secret == passwordEncoder.decode(encodedSecret.toString)
      } catch {
        case _: Exception => false
      }
    }
    if (isMatch) {
      props.filterNot(_._1 == secretPrefix).foreach { case (k, v) =>
        decoded.put(k.substring(secretPrefix.length), passwordEncoder.decode(v.toString).value)
      }
    }
    isMatch
  }
}
