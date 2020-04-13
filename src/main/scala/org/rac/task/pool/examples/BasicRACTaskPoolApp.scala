package org.rac.task.pool.examples

import java.util.concurrent.atomic.AtomicLong

import org.rac.task.pool.{BasicRacTaskPool, TaskContext, TaskInput, TaskOutput}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object BasicRACTaskPoolApp extends App {
  val counter = new AtomicLong(0)

  val task: (MyTaskInput, TaskContext) => MyTaskOutput = { (input, ctx) =>
    println(s"============>Task input: '${input.get}''")
    println(s"Task worker: '${ctx.workerName}''")

    println(s"${ctx.workerName}: Working...")
    Thread sleep 3000
    println(s"${ctx.workerName}: DONE")

    MyTaskOutput(s"TASK_OUTPUT_${counter.incrementAndGet()}")
  }

  val taskPool = new BasicRacTaskPool[MyTaskInput, MyTaskOutput](3, 12)

  val taskInputs = for {
    i <- 1 to 17
  } yield MyTaskInput(i.toString)

  val futures = taskInputs.map(taskPool.runAsynchronous(_)(task))
  futures.foreach { future =>
    future.foreach(output => println(s"Task output: ${output.get}"))
  }

  Thread sleep 31000

  taskPool.sequence(taskInputs)(task).onComplete {
    case Success(results) =>
      println("Results:")
      println(s"${results.map(_.get) mkString "\n"}")
    case Failure(e) =>
      println(s"Error !!! ${e.getMessage}")
  }

  Thread sleep 31000

}

object BasicRACTaskPoolApp2 extends App {
  val counter = new AtomicLong(0)

  val task: (MyTaskInput, TaskContext) => MyTaskOutput = { (input, ctx) =>
    println(s"============>Task input: '${input.get}''")

    println(s"${ctx.workerName}: Working...")
    Thread sleep 500

    MyTaskOutput(s"TASK_OUTPUT_${counter.incrementAndGet()}")
  }

  val taskPool = new BasicRacTaskPool[MyTaskInput, MyTaskOutput](3, 1000)

  for {
    i <- 1 to (51 * 60 * 12)
  } yield {
    if (i % 51 == 0) Thread sleep 1000

    val taskInput = MyTaskInput(i.toString)
    taskPool.runAsynchronous(taskInput)(task).onComplete {
      case Success(output) =>
        println(s"Task output: ${output.get}")
      case Failure(e) =>
        println(s"Error !!! ${e.getMessage}")
    }
  }

  println("================================ FINISH GENERATING TASKS ================================")

  Thread sleep 1000 * 60 * 150

  println(s"Counter: ${counter.get}")

}

class MyTaskInput(v: String) extends TaskInput {
  override type I = String

  private val _input: String = v

  override def get: String = _input

}

object MyTaskInput {

  def apply(v: String): MyTaskInput =
    new MyTaskInput(v)

}

class MyTaskOutput(v: String) extends TaskOutput {
  override type O = String

  private val _output = v

  override def get: String = _output

}

object MyTaskOutput {

  def apply(v: String): MyTaskOutput =
    new MyTaskOutput(v)

}
