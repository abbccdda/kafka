/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tasks

import java.util.concurrent._

object CompletableFutureUtil {

  def completed[T](result: T): CompletableFuture[T] = {
    val f = new CompletableFuture[T]()
    f.complete(result)
    f
  }

  def failed[T](ex: Throwable): CompletableFuture[T] = {
    val f = new CompletableFuture[T]()
    f.completeExceptionally(ex)
    f
  }
}
