package org.rac.task.pool

import org.rac.task.pool.RacTaskPool.Operation

import scala.collection.mutable

class BasicOperationQueue[I <: TaskInput, O <: TaskOutput](maxSize: Int) {

  private val _queue = mutable.Queue.empty[Operation[I, O]]

  def dequeue(): Operation[I, O] = this.synchronized {
    while (_queue.isEmpty) this.wait()

    val v = _queue.dequeue()
    this.notify()

    v
  }

  def enqueue(operation: Operation[I, O]): Unit = this.synchronized {
    while (_queue.size > maxSize) this.wait()

    _queue += operation
    this.notify()
  }

}

object BasicOperationQueue {

  def apply[I <: TaskInput, O <: TaskOutput](maxSize: Int): BasicOperationQueue[I, O] =
    new BasicOperationQueue(maxSize)

}
