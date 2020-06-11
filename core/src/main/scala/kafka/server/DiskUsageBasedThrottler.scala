/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server

import java.nio.file.{FileStore, Files, Paths}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import kafka.utils.Logging
import org.apache.kafka.common.config.internals.ConfluentConfigs
import org.apache.kafka.common.utils.Time

import scala.collection.Seq
import scala.jdk.CollectionConverters._

/**
 * This trait encapsulates the logic of throttling through the Quota Managers based on disk capacity of the broker host
 * The current design is mixed in with the ClientQuotaManager as we would want to reuse the existing periodic throttling
 * check through ClientQuotaManager#updateBrokerQuotaLimit which then invokes the checkAndUpdateQuotaOnDiskUsage method
 */
trait DiskUsageBasedThrottler extends Logging {

  import DiskUsageBasedThrottler.{getListeners, anyListenerIsThrottled}

  protected def diskThrottlingConfig: DiskUsageBasedThrottlingConfig

  @volatile private var dynamicDiskThrottlingConfig: DiskUsageBasedThrottlingConfig = diskThrottlingConfig

  /**
   * This method needs to be implemented by the clientQuotaManager to provide the time object for checking the interval
   * for checking the disk usage
   *
   * @return the Time object to be used for detecting whether disk usage needs to be verified
   */
  protected def time: Time

  private val lastCheckedTime = new AtomicLong(time.milliseconds())

  /**
   * This method tries creating the file-stores on top of the underlying log directories within the broker
   * However, this returns an empty sequence in the case of an error, for example, in the case that the underlying log
   * directories might not have been created at the time of the disk throttler initialization.
   * Hence, we currently log that and return an empty sequence (to indicate uninitialized fileStores)
   * Consequently, in the case of disk throttling feature being enabled,
   * subsequent calls to checkAndUpdateQuotaOnDiskUsage will ensure the file stores get created at that time.
   *
   * @return the sequence of the file-store objects
   */
  protected def getFileStores: Seq[FileStore] = {
    try {
      dynamicDiskThrottlingConfig.logDirs.map(logDir => Files.getFileStore(Paths.get(logDir)))
    } catch {
      case e: Exception =>
        debug(s"Couldn't create file-stores for logDirs: ${dynamicDiskThrottlingConfig.logDirs}, " +
          "however, this is normal at startup with non-existent log directories", e)
        Seq.empty
    }
  }

  @volatile private var fileStores = getFileStores

  def updateDiskThrottlingConfig(newConfig: DiskUsageBasedThrottlingConfig): Unit = {
    if (diskThrottlingEnabledInConfig(dynamicDiskThrottlingConfig) && !diskThrottlingEnabledInConfig(newConfig)) {
      // this means we are turning off the throttling through the dynamic config route
      logger.info(s"Disabling disk based throttling based on new config: $newConfig")
      // we are calling updateListeners here because checkAndUpdateQuotaOnDiskUsage will be a noop call based on the updated config
      updateListeners(None)
    }
    logger.info(s"Updating disk throttling config from: $dynamicDiskThrottlingConfig to $newConfig")
    dynamicDiskThrottlingConfig = newConfig
    checkAndUpdateQuotaOnDiskUsage(time.milliseconds())
  }

  protected[server] def diskThrottlingEnabledInConfig(config: DiskUsageBasedThrottlingConfig): Boolean = {
    config.enableDiskBasedThrottling && config.logDirs.nonEmpty
  }

  /**
   * Method for checking the minimum amongst the total bytes allotted to the different underlying log dirs
   * This method is currently unused, but can be used for deriving the threshold as a function of the total disk bytes
   *
   * @return minimum of the total bytes allotted across the log dirs
   */
  protected[server] def minDiskTotalBytes: Long = {
    fileStores.map(_.getTotalSpace).min
  }

  /**
   * Method for checking the minimum approximate bytes available to the JVM amongst the different log dirs
   * The reason behind considering the min of all the various log dirs is that we should start back-pressuring
   * as soon as any one of the log dirs has dropped below the threshold, and not wait for all of them to drop cumulatively
   *
   * @return minimum of the available bytes across the log dirs
   */
  protected[server] def minDiskUsableBytes: Long = {
    fileStores.map(_.getUsableSpace).min
  }

  /**
   * This method checks the underlying available bytes across all the log dirs, and then either caps or resets the
   * quota limit based on the conditions:
   * If the available disk is less than the threshold, then the quota is capped across all the listeners
   * Otherwise, if the available disk >= freeDiskThresholdBytesRecoveryFactor times of the threshold, only then we would lift the cap,
   * because otherwise we might end up with an oscillating condition
   *
   * @param timeMs the system time in ms to be compared against the throttling interval
   */
  def checkAndUpdateQuotaOnDiskUsage(timeMs: Long): Unit = {
    if (!diskThrottlingEnabledInConfig(dynamicDiskThrottlingConfig)) {
      return
    }
    val lastCheck = lastCheckedTime.get
    if (lastCheck + dynamicDiskThrottlingConfig.diskCheckFrequencyMs > timeMs) {
      logger.debug("Current time: {} still early for next check at: {}",
        timeMs,
        lastCheck + dynamicDiskThrottlingConfig.diskCheckFrequencyMs)
      return
    }
    if (lastCheckedTime.compareAndSet(lastCheck, timeMs)) {
      doCheckAndUpdateThrottles()
    }
  }

  private def doCheckAndUpdateThrottles(): Unit = {
    // we are checking if the underlying fileStores are set, otherwise try to set them before evaluating disk threshold
    if (fileStores.nonEmpty || maybeSetFileStores()) {
      if (minDiskUsableBytes < dynamicDiskThrottlingConfig.freeDiskThresholdBytes) {
        logger.info("Disk with the lowest free space: {}B available < threshold: {}B, will apply throttle!",
          minDiskUsableBytes,
          dynamicDiskThrottlingConfig.freeDiskThresholdBytes)
        updateListeners(Some(dynamicDiskThrottlingConfig.throttledProduceThroughput))
      } else if (anyListenerIsThrottled && minDiskUsableBytes >=
        dynamicDiskThrottlingConfig.freeDiskThresholdBytesRecoveryFactor * dynamicDiskThrottlingConfig.freeDiskThresholdBytes) {
        logger.info("Disk with the lowest free space: {}B available >= {} x threshold: {}B, will remove low disk " +
          "space throttle",
          minDiskUsableBytes,
          dynamicDiskThrottlingConfig.freeDiskThresholdBytesRecoveryFactor,
          dynamicDiskThrottlingConfig.freeDiskThresholdBytes)
        updateListeners(None)
      }
    }
  }

  /**
   * This method updates the file stores wrapped on top of the underlying log directories.
   * The rationale behind this delayed instantiation of the file-stores have been explained in the docs of getFileStores
   *
   * @return true if fileStores have been successfully created
   */
  private def maybeSetFileStores(): Boolean = {
    getFileStores match {
      case createdFileStores if createdFileStores.nonEmpty =>
        fileStores = createdFileStores
        debug(s"Created file-stores for logDirs: ${dynamicDiskThrottlingConfig.logDirs}")
        true
      case _ => false
    }
  }

  // Here we are checking for throttles as part of initialization bypassing the lastCheckedTime check
  protected[server] def initThrottler(): Unit = {
    if (diskThrottlingEnabledInConfig(dynamicDiskThrottlingConfig)) {
      logger.info(s"Initializing low disk space throttle with config: $dynamicDiskThrottlingConfig")
      lastCheckedTime.set(time.milliseconds())
      doCheckAndUpdateThrottles()
    }
  }

  /**
   * This method will be called for updating the listeners while either setting a quota cap or to signal that the
   * disk space has increased and that the quota should be lifted
   *
   * @param capOpt The optional quota value, if defined, signals the application of that quota cap
   */
  private def updateListeners(capOpt: Option[Long]): Unit = {
    getListeners.foreach(listener => {
      if (listener.lastSignalledQuotaOptRef.getAndSet(capOpt) != capOpt) {
        capOpt match {
          case Some(cap) =>
            listener.quotaType match {
              case QuotaType.Produce =>
                listener.handleDiskSpaceLow(cap)
              case QuotaType.FollowerReplication =>
                // we are throttling the follower to 2x the produce quota to avoid URPs in case the leader is also throttled
                listener.handleDiskSpaceLow(2 * cap)
              case _ => // other quota types don't need to handle the disk throttling
            }
          case None =>
            listener.handleDiskSpaceRecovered()
        }
      }
    })
  }

  protected[server] def getCurrentDiskThrottlingConfig: DiskUsageBasedThrottlingConfig = dynamicDiskThrottlingConfig
}

/**
 * This is a marker trait which needs to be mixed in with all those QuotaManagers who are controlling the broker quota
 * that will be writing to the disk.
 * Currently, this is enabled with the produce ClientQuotaManager and the fetch ReplicationQuotaManager, because
 * both of them are writing to the underlying log dirs
 */
trait DiskUsageBasedThrottleListener {
  // exposed for logging purposes
  protected[server] def quotaType: QuotaType

  /**
   * This method would be used to signal that disk space is low to the implementing QuotaManager classes
   * In addition, a broker-wide quota is also passed on for throttling of requests through the respective quota managers
   * The pre-condition checks such as whether backpressure is enabled is left to the implementing classes
   *
   * @param cappedQuotaInBytesPerSec The quota limit that is signalled to the implementing quota managers
   */
  def handleDiskSpaceLow(cappedQuotaInBytesPerSec: Long): Unit

  /**
   * This method would be used to signal to the quota managers that the disk availability has increased to a sustainable
   * level and that they can resume their erstwhile quotas
   */
  def handleDiskSpaceRecovered(): Unit

  /**
   * This val will be used to detect if the quota has been updated (through dynamic config) while throttling is active
   * This has been wrapped in a AtomicRef to make it thread-safe
   */
  protected[server] val lastSignalledQuotaOptRef: AtomicReference[Option[Long]] = new AtomicReference(None)
}

/**
 * This companion object will host the private listener map that will be invoked by the throttling logic
 */
object DiskUsageBasedThrottler {
  private val listeners = new java.util.concurrent.ConcurrentHashMap[DiskUsageBasedThrottleListener, Boolean]()

  protected[server] def registerListener(listener: DiskUsageBasedThrottleListener): Unit = {
    listeners.put(listener, true)
  }

  protected[server] def deRegisterListener(listener: DiskUsageBasedThrottleListener): Unit = {
    listeners.remove(listener)
  }

  // the following is non-private for tests
  protected[server] def getListeners: collection.Set[DiskUsageBasedThrottleListener] = listeners.keySet.asScala

  protected[server] def anyListenerIsThrottled: Boolean = {
    getListeners.exists(_.lastSignalledQuotaOptRef.get.isDefined)
  }

  /**
   * Used to detect whether a quotaManager object is currently being throttled due to low disk
   *
   * @param listener the quotaManager object that needs to be verified for active throttling
   * @return whether the listener is actively being throttled due to low disk
   */
  protected[server] def diskThrottlingActive(listener: DiskUsageBasedThrottleListener): Boolean = {
    listener.lastSignalledQuotaOptRef.get.isDefined && listeners.containsKey(listener)
  }
}

/**
 * This config object encapsulates all the details surrounding the disk based throttling
 * In order to validate the arguments, we have made the constructor private and used the companion object's apply method
 *
 * @param freeDiskThresholdBytes               the minimum usable bytes present amongst the log dirs, below which the producer will be throttled
 * @param throttledProduceThroughput           the throughput in Bytes/s that will be applied across the producers
 *                                             corresponds to "confluent.backpressure.disk.produce.bytes.per.second"
 * @param logDirs                              the list of kafka data log dirs, corresponds to kafkaConfig.logDirs
 * @param enableDiskBasedThrottling            the boolean flag to indicate enabling of disk based throttling,
 *                                             corresponds to confluent.backpressure.disk.enable
 * @param diskCheckFrequencyMs                 the frequency in ms of checking the disk usage
 * @param freeDiskThresholdBytesRecoveryFactor the multiplier of the free disk threshold above which throttling would be turned off
 */
case class DiskUsageBasedThrottlingConfig private(freeDiskThresholdBytes: Long,
                                                  throttledProduceThroughput: Long,
                                                  logDirs: Seq[String],
                                                  enableDiskBasedThrottling: Boolean,
                                                  diskCheckFrequencyMs: Long,
                                                  freeDiskThresholdBytesRecoveryFactor: Double) {

  // we are making the copy constructor protected to ensure sanitized case class props, and only exposing it for tests
  protected[server] def copy(freeDiskThresholdBytes: Long = this.freeDiskThresholdBytes,
                             throttledProduceThroughput: Long = this.throttledProduceThroughput,
                             logDirs: Seq[String] = this.logDirs,
                             enableDiskBasedThrottling: Boolean = this.enableDiskBasedThrottling,
                             diskCheckFrequencyMs: Long = this.diskCheckFrequencyMs,
                             freeDiskThresholdBytesRecoveryFactor: Double = this.freeDiskThresholdBytesRecoveryFactor)
  : DiskUsageBasedThrottlingConfig = {
    new DiskUsageBasedThrottlingConfig(freeDiskThresholdBytes, throttledProduceThroughput, logDirs,
      enableDiskBasedThrottling, diskCheckFrequencyMs, freeDiskThresholdBytesRecoveryFactor)
  }
}

/**
 * The companion object to the disk throttling config, responsible for building the config with certain sanity checks
 * This is necessary to avoid the broker from illegal config values
 */
object DiskUsageBasedThrottlingConfig extends Logging {
  /**
   * The minimum disk threshold below which the supplied value will be overridden with this value: 1GB
   */
  val MinDiskThresholdBytes: Long = 1024 * 1024 * 1024
  /**
   * The minimum quota below which the supplied value will be overridden with this value: 16KBps
   */
  val MinThroughputBytesPerSec: Long = 16 * 1024
  /**
   * The default time interval for checking the disk space, currently defaults to 1 minute
   */
  val DefaultDiskCheckFrequencyMs: Long = TimeUnit.SECONDS.toMillis(60)
  /**
   * The minimum multiplier for the free disk threshold above which the throttling would be deactivated
   */
  val MinFreeDiskRecoveryFactor: Double = 1.0

  def apply(freeDiskThresholdBytes: Long = ConfluentConfigs.BACKPRESSURE_DISK_THRESHOLD_BYTES_DEFAULT,
            throttledProduceThroughput: Long = ConfluentConfigs.BACKPRESSURE_PRODUCE_THROUGHPUT_DEFAULT,
            logDirs: Seq[String] = Seq.empty,
            enableDiskBasedThrottling: Boolean = ConfluentConfigs.BACKPRESSURE_DISK_ENABLE_DEFAULT,
            diskCheckFrequencyMs: Long = DefaultDiskCheckFrequencyMs,
            freeDiskThresholdBytesRecoveryFactor: Double = ConfluentConfigs.BACKPRESSURE_DISK_RECOVERY_FACTOR_DEFAULT)
  : DiskUsageBasedThrottlingConfig = {
    if (logDirs.isEmpty) {
      logger.info("Empty logDirs received! Disk based throttling won't be activated!")
    }
    new DiskUsageBasedThrottlingConfig(
      getSanitisedConfig(ConfluentConfigs.BACKPRESSURE_DISK_THRESHOLD_BYTES_CONFIG, freeDiskThresholdBytes, MinDiskThresholdBytes),
      getSanitisedConfig(ConfluentConfigs.BACKPRESSURE_PRODUCE_THROUGHPUT_CONFIG, throttledProduceThroughput, MinThroughputBytesPerSec),
      logDirs,
      logDirs.nonEmpty && enableDiskBasedThrottling,
      diskCheckFrequencyMs,
      getSanitisedConfig(ConfluentConfigs.BACKPRESSURE_DISK_RECOVERY_FACTOR_CONFIG, freeDiskThresholdBytesRecoveryFactor, MinFreeDiskRecoveryFactor))
  }

  /**
   * This method checks if the supplied value for the config is less than the required minimum value, and if that's the case,
   * then it logs the situation and returns the minimum value instead of the supplied value
   *
   * @param configName    name of the config property
   * @param suppliedValue user-provided value of the config
   * @param minimumValue  required minimum value
   * @return max(suppliedValue, minimumValue)
   */
  private def getSanitisedConfig(configName: String, suppliedValue: Long, minimumValue: Long): Long = {
    if (suppliedValue < minimumValue) {
      logger.warn("Illegal value for {}: {}. Will be set to: {}", configName, suppliedValue, minimumValue)
    }
    Math.max(suppliedValue, minimumValue)
  }

  private def getSanitisedConfig(configName: String, suppliedValue: Double, minimumValue: Double): Double = {
    if (suppliedValue < minimumValue) {
      logger.warn("Illegal value for {}: {}. Will be set to: {}", configName, suppliedValue, minimumValue)
    }
    Math.max(suppliedValue, minimumValue)
  }
}
