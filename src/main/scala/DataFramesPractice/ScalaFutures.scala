package DataFramesPractice


import scala.concurrent.{Future => ConcurrentTask}           // rename
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import java.util.concurrent._
import scala.concurrent.duration.Duration
import java.util.concurrent.{ExecutorService, Executors}

object FutureAsConcurrentTask {

  def main(args: Array[String]): Unit = {

    import scala.concurrent._

    /*
     implicit val ec = new ExecutionContext {
      val threadPool = Executors.newFixedThreadPool(1000)
      def execute(runnable: Runnable) {threadPool.submit(runnable)}
      def reportFailure(t: Throwable) {throw t}
    }
    */

    def test():String = {
      "kali 1"
    }

    // run some long-running task (task has type Future[Int] in this example)
    val task = ConcurrentTask {
      test()
    }

    // whenever the task completes, execute this code
    task.onComplete {
      case Success(value) => println(s"Got the callback, value = $value")
      case Failure(e) => println(s"D'oh! The task failed: ${e.getMessage}")
      //case _ => ec.shutdownNow()
    }

    Await.result(task, Duration.Inf)


  }

}