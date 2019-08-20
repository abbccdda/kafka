/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tasks

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
    * Poll the oldest item in this queue if an item is available. This method is non-blocking and returns immediately
    * regardless of whether an item is present in the queue.
    * @return Optional item removed from the queue if one was present; None otherwise
    */
  def poll(): Option[T] = {
    lock.lock()
    try {
      if (closed)
        throw new CancellationException("queue closed")

      if (queue.isEmpty) {
        None
      } else {
        val key = queue.dequeue()
        items.remove(key) match {
          case Some(item) => Some(item)
          case None => throw new IllegalStateException("Illegal queue state")
        }
      }
    } finally {
      lock.unlock()
    }
  }

  /**
    * Remove and return the oldest item in this queue, blocking until an item is available.
    * @return The oldest item in the queue
    */
  def take(): T = {
    lock.lock()

    try {
      while (!closed && queue.isEmpty)
        cond.await()

      if (closed)
        throw new CancellationException("queue closed")

      poll().get
    } finally {
      lock.unlock()
    }
  }

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
}
