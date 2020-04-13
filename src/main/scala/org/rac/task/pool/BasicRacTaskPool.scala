package org.rac.task.pool

import org.rac.task.pool.BasicRacTaskPool.BasicRacTaskPoolWorker

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

class BasicRacTaskPool[I <: TaskInput, O <: TaskOutput](maxRunningTasks: Int = 1,
                                                        maxStandByTasks: Int = 101)(implicit ex: ExecutionContext) {

  private val _operations = BasicOperationQueue[I, O](maxStandByTasks)

  private val _workers = List.range(0, maxRunningTasks).map { i =>
    new BasicRacTaskPoolWorker(s"simple-rac-task-pool-worker-$i", _operations)
  }

  _workers.foreach(_.run())

  def runAsynchronous(taskInput: I)(func: (I, TaskContext) => O): Future[O] =
    BasicRacTaskPool.runAsynchronous(taskInput, _operations)(func)

  def sequence(taskInputs: Seq[I])(func: (I, TaskContext) => O): Future[Seq[O]] =
    BasicRacTaskPool.sequence(taskInputs, this)(func)

}

object BasicRacTaskPool {

  private def runAsynchronous[I <: TaskInput, O <: TaskOutput](taskInput: I, operations: BasicOperationQueue[I, O])
                                                              (func: (I, TaskContext) => O): Future[O] = {
    val task = new Task[I, O] {
      override def input: I = taskInput

      override def f: (I, TaskContext) => O = func
    }

    val operation = (task, Promise[O])
    operations enqueue operation

    operation._2.future
  }

  private def sequence[I <: TaskInput, O <: TaskOutput](taskInputs: Seq[I], taskPool: BasicRacTaskPool[I, O])
                                                       (func: (I, TaskContext) => O)
                                                       (implicit ex: ExecutionContext): Future[Seq[O]] = {
    import scala.collection.mutable

    val futures = mutable.Buffer.empty[Future[O]]

    taskInputs.foreach { taskInput =>
      val future = taskPool.runAsynchronous(taskInput)(func)
      futures append future
    }

    sequence(futures.toSeq)
  }

  private def sequence[I <: TaskInput, O <: TaskOutput](futures: Seq[Future[O]])
                                                       (implicit ex: ExecutionContext): Future[Seq[O]] = {
    import scala.collection.mutable

    @tailrec
    def go(f: Seq[Future[O]], acc: Try[mutable.Buffer[O]]): Try[mutable.Buffer[O]] = f match {
      case Seq() =>
        acc
      case Seq(future, tail@_*) =>
        Try(Await.result(future, Duration.Inf)) match {
          case Success(result) =>
            go(tail, Success(acc.get :+ result))
          case Failure(e) =>
            Failure(e)
        }
    }

    Future {
      go(futures, Success(mutable.Buffer.empty[O])) match {
        case Success(results) =>
          results.toSeq
        case Failure(e) =>
          throw new Exception(e)
      }
    }
  }

  class BasicRacTaskPoolWorker[I <: TaskInput, O <: TaskOutput](val name: String,
                                                                val operations: BasicOperationQueue[I, O])
                                                               (implicit ex: ExecutionContext) extends Worker {

    def run(): Future[Unit] = Future {
      while (true) {
        val (task, promise) = operations.dequeue()
        val result = Try(task.run(BasicTacTaskPoolTaskContext(name)))

        promise complete result
      }
    }

  }

  case class BasicTacTaskPoolTaskContext(workerName: String) extends TaskContext

}
