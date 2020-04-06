package kafka.utils

import java.util.concurrent.atomic.AtomicLong

abstract class HeartbeatingShutdownableThread(name: String, isInterruptible: Boolean = true)
  extends ShutdownableThread(name, isInterruptible) {
  private val _lastHeartbeatMs = new AtomicLong(System.currentTimeMillis)

  override def heartbeat(): Unit = {
    _lastHeartbeatMs.set(System.currentTimeMillis())
  }

  def lastHeartbeatMs(): Long = _lastHeartbeatMs.get()
}
