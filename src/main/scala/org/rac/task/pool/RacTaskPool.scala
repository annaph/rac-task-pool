package org.rac.task.pool

import scala.concurrent.{Future, Promise}

trait TaskInput {

  type I

  def get: I

}

trait TaskOutput {

  type O

  def get: O

}

trait Task[I <: TaskInput, O <: TaskOutput] {

  def input: I

  def f: (I, TaskContext) => O

  private[pool] def run(ctx: TaskContext): O = f(input, ctx)

}

trait TaskContext {

  def workerName: String

}

trait Worker {

  def name: String

  private[pool] def run(): Future[Unit]

}

object RacTaskPool {

  type Operation[I <: TaskInput, O <: TaskOutput] = (Task[I, O], Promise[O])

}
