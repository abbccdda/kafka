/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

import java.util.concurrent.{CancellationException, TimeUnit}

import org.apache.kafka.test.IntegrationTest
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalacheck.commands.Commands
import org.scalacheck.{Arbitrary, Gen, Prop}
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner
import org.scalatestplus.scalacheck.Checkers

import scala.collection.immutable.Queue
import scala.util.{Failure, Success, Try}


case class QueueEntry(k: Int, value: Int) extends UpdatableQueueEntry {
  override type Key = Int
  override def key: Key = k
}

object QueueEntry {
  implicit def arbQueueEntry: Arbitrary[QueueEntry] = {
    Arbitrary {
      for {
        k <- Gen.chooseNum(0, 2)
        v <- Gen.chooseNum(0, 3)
      } yield QueueEntry(k, v)
    }
  }
}


object UpdatableQueueSpec extends Commands {
  override type Sut = UpdatableQueue[QueueEntry]
  case class State(queue: Queue[QueueEntry#Key] = Queue(),
                   values: Map[QueueEntry#Key, QueueEntry] = Map(),
                   closed: Boolean = false)

  override def canCreateNewSut(newState: State,
                               initSuts: Traversable[State],
                               runningSuts: Traversable[UpdatableQueue[QueueEntry]]): Boolean = {
    true
  }
  override def initialPreCondition(state: State): Boolean = !state.closed && state.values.isEmpty && state.queue.isEmpty
  override def newSut(state: State): UpdatableQueue[QueueEntry] = new UpdatableQueue[QueueEntry]()
  override def destroySut(sut: UpdatableQueue[QueueEntry]): Unit = sut.close()
  override def genInitialState: Gen[State] = State()

  override def genCommand(state: State): Gen[Command] = {

    def genPush: Gen[Push] = for(entry <- QueueEntry.arbQueueEntry.arbitrary) yield Push(entry)

    Gen.frequency(
      (20, genPush),
      (10, Gen.const(Pop)),
      (1, Gen.const(Close))
    )
  }

  case class Push(queueEntry: QueueEntry) extends Command {
    override type Result = Unit

    override def run(sut: UpdatableQueue[QueueEntry]): Unit = {
      sut.push(queueEntry)
    }

    override def nextState(state: State): State = {
      if (state.closed) {
        // If the state is closed, no push allowed
        state
      } else {
        val key = queueEntry.key
        // always insert the state into the map
        val alreadyExists = state.values.contains(key)
        val newMap = state.values + (key -> queueEntry)
        if (alreadyExists) {
          state.copy(values = newMap)
        } else {
          state.copy(queue = state.queue.enqueue(key), values = newMap)
        }
      }
    }
    override def preCondition(state: State): Boolean = true

    override def postCondition(state: State, result: Try[Unit]): Prop = {
      result match {
        case Success(()) =>
          if (state.closed) Prop.falsified
          else Prop.passed
        case Failure(_: CancellationException) =>
          if (state.closed) Prop.passed
          else Prop.falsified
        case Failure(e: Throwable) =>
          Prop.exception(e)
      }
    }
  }

  case object Close extends UnitCommand {
    override def run(sut: UpdatableQueue[QueueEntry]): Unit = sut.close()
    override def nextState(state: State): State = state.copy(closed = true)
    override def preCondition(state: State): Boolean = true
    override def postCondition(state: State, success: Boolean): Prop = success
  }

  case object Pop extends Command {
    override type Result = Option[QueueEntry]

    override def run(sut: UpdatableQueue[QueueEntry]): Result = {
      sut.pop(0, TimeUnit.MILLISECONDS)
    }
    override def nextState(state: State): State = {
      if (state.queue.isEmpty && state.values.isEmpty || state.closed) {
        state // skip the state if there is nothing to pop
      } else {
        val (key, newQueue) = state.queue.dequeue
        state.copy(queue = newQueue, values = state.values - key)
      }
    }
    override def preCondition(state: State): Boolean = true
    override def postCondition(state: State, result: Try[Option[QueueEntry]]): Prop = {
      result match {
        case Failure(_: CancellationException) =>
          if (state.closed) Prop.passed
          else Prop.falsified
        case Failure(exception) =>
          Prop.exception(exception)
        case Success(Some(value)) =>
          if (state.values.isEmpty || state.queue.isEmpty || state.closed) {
            Prop.falsified // pop should not succeed
          } else {
            val (expectedKey, _) = state.queue.dequeue
            val expectedVal = state.values(expectedKey)
            if (expectedVal == value) Prop.passed
            else Prop.falsified
          }
        case Success(None) =>
          if (state.queue.isEmpty && state.values.isEmpty || state.closed) Prop.passed
          else Prop.falsified
      }
    }
  }
}


@Category(Array(classOf[IntegrationTest]))
@RunWith(classOf[JUnitRunner])
class UpdatableQueuePropertyTest extends FunSuite with Checkers {

  val config: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 10000, workers = 4)

  test("testSingleThreadedUpdatableQueue") {
    check {
      UpdatableQueueSpec.property(threadCount = 1)
    }
  }

  test("testMultiThreadedUpdatableQueue") {
    check {
      UpdatableQueueSpec.property(threadCount = 4)
    }
  }

  test("testAllValuesMaterialize") {
    check {
      entries: Seq[QueueEntry] => {
        val queue = new UpdatableQueue[QueueEntry]()
        for (queueEntry <- entries) {
          queue.push(queueEntry)
        }
        val expectedValues = entries
          .foldLeft(Map[QueueEntry#Key, QueueEntry]()) {
            case (acc: Map[QueueEntry#Key, QueueEntry], queueEntry: QueueEntry) =>
              acc + (queueEntry.key -> queueEntry)
          }
        var results = Map[QueueEntry#Key, QueueEntry]()
        var pollResult: Option[QueueEntry] = None
        do {
          pollResult = queue.pop(0, TimeUnit.MILLISECONDS)
          pollResult match {
            case None =>
            case Some(result) =>
              results += (result.key -> result)
          }
        } while (pollResult.isDefined)
        expectedValues == results
      }
    }
  }
}