package kafka.durability.exceptions

class DurabilityServiceShuttingDownException (val error: String, cause: Throwable) extends RuntimeException(error, cause) {
  def this(error: Throwable) = this(error.getMessage, error)

  def this(msg: String) = this(msg, null)
}
