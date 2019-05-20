/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}

import scala.collection.mutable
import scala.concurrent.CancellationException

trait UpdatableQueueEntry {
  type Key
  def key: Key
}

/**
  * UpdatableQueue is an unbounded, thread safe blocking queue with FIFO semantics. Push operations
  * where T#Key is already present in the queue overwrite T, and maintain queue ordering.
  * @tparam T UpdatableQueueEntry
  */
final class UpdatableQueue[T <: UpdatableQueueEntry] {
  private val lock: Lock = new ReentrantLock()
  private val cond: Condition = lock.newCondition()
  private val items: mutable.Map[T#Key, T] = mutable.Map()
  private val queue: mutable.Queue[T#Key] = mutable.Queue()
  private var closed: Boolean = false

  /**
    * Close the queue, unblocking all pollers.
    */
  def close(): Unit = {
    lock.lock()
    try {
      closed = true
      cond.signalAll()
    } finally {
      lock.unlock()
    }
  }

  /**
    * Enqueue item, replacing any item with a matching T#Key.
    *
    * Throws CancellationException if the queue is closed.
    */
  def push(item: T): Unit = {
    val key = item.key
    lock.lock()
    try {
      if (closed)
        throw new CancellationException("queue closed")

      if (!items.contains(key))
        queue.enqueue(key)

      items(key) = item
      cond.signal()
    } finally {
      lock.unlock()
    }
  }

  /**
    * Push an item onto the queue if an item with the same T#Key is not already present.
    * @param item
    */
  def pushIfNotPresent(item: T): Unit = {
    val key = item.key
    lock.lock()
    try {
      if (!items.contains(key)) {
        queue.enqueue(key)
        items(key) = item
        cond.signal()
      }
    } finally {
      lock.unlock()
    }
  }

  /**
    * Pop the oldest item in this queue, waiting if necessary.
    */
  def pop(): T = {
    pop(Long.MaxValue, TimeUnit.DAYS).get
  }

  /**
    * Pop the oldest item in this queue, waiting up to the specified timeout.
    * @return Some(item) if the pop operation completed before the specified timeout, otherwise None.
    */
  def pop(timeout: Long, unit: TimeUnit): Option[T] = {
    lock.lock()
    try {
      if (!closed) {
        while (queue.isEmpty) {
          if (closed) {
            throw new CancellationException("queue closed")
          } else {
            if (!cond.await(timeout, unit)) {
              // await timed out, return early
              return None
            }
            // await did not time out,
          }
        }
        val key = queue.dequeue()
        items.remove(key) match {
          case Some(item) => return Some(item)
          case None => throw new IllegalStateException("illegal queue state")
        }
      }
      throw new CancellationException("queue closed")
    } finally {
      lock.unlock()
    }
  }
}
