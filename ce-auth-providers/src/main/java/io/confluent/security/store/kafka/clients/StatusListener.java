// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.clients;

/**
 * Interface to listener for failure status of KafkaStore readers and writers
 */
public interface StatusListener {

  /**
   * Invoked when a record has been processed successfully by the consumer.
   */
  void onReaderSuccess();

  /**
   * Invoked if the consumer throws an exception while polling. Failure is cleared on
   * a successful poll.
   * @return true if retry timeout has elapsed since the first failure. If true, authorizer
   *    will be moved to failed state and all future authorization requests will be denied.
   */
  boolean onReaderFailure();

  /**
   * Invoked when the writer is successfully initialized.
   */
  void onWriterSuccess(int partition);

  /**
   * Invoked if the writer fails due to a critical error and resigns. Failure will be cleared
   * after writer is re-elected and writer of a new generation completes initialization.
   *
   * @param partition Partition for which writer failed
   * @return true if retry timeout has elapsed since the first failure. If true, authorizer
   *    will be moved to failed state and all future authorization requests will be denied.
   */
  boolean onWriterFailure(int partition);

  /**
   * Invoked when a record is successfully produced
   * @param partition Partition of the record
   */
  void onProduceSuccess(int partition);

  /**
   * Invoked when a record produce fails.
   * @param partition Partition of the record
   */
  void onProduceFailure(int partition);

  /**
   * Invoked when an INITIALIZED status record is consumed.
   * @param partition Partition of the record
   */
  void onRemoteSuccess(int partition);

  /**
   * Invoked when FAILED status record is consumed. Failure is cleared when the next INITIALIZED
   * record is processed.
   * @param partition Partition of the record
   *
   * @return true if retry timeout has elapsed since the first failure. If true, authorizer
   *    will be moved to failed state and all future authorization requests will be denied.
   */
  boolean onRemoteFailure(int partition);
}
