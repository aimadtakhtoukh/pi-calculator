package example.pi

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing.RoundRobinPool

import scala.concurrent.duration.Duration

object ClassicPiCalculator extends App {

  calculate(nrOfWorkers = 4, nrOfElements = 10000, nrOfMessages = 10000)

  sealed trait PiMessage

  case object Calculate extends PiMessage

  case class Work(start: Int, nrOfElements: Int) extends PiMessage

  case class Result(value: Double) extends PiMessage

  case class PiApproximation(pi: Double, duration: Duration)

  class Worker extends Actor {

    override def receive: Receive = {
      case Work(start, nrOfElements) =>
        sender ! Result(calculatePiFor(start, nrOfElements))
    }

    def calculatePiFor(start: Int, nrOfElement: Int): Double = {
      var acc = 0.0

      for (i <- start until (start + nrOfElement)) {
        acc += 4.0 * (1 - (i % 2) * 2) / (2 * i + 1)
      }

      acc
    }
  }

  class Master(nrOfWorkers: Int, nrOfMessages: Int, nrOfElements: Int, listener: ActorRef) extends Actor {

    var pi: Double = _
    var nrOfResults: Double = _
    val start: Long = System.currentTimeMillis

    val workerRouter = context.actorOf(Props[Worker].withRouter(RoundRobinPool(nrOfWorkers)), name = "workerRouter")

    override def receive: Receive = {
      case Calculate =>
        for (i <- 0 until nrOfMessages) workerRouter ! Work(i * nrOfElements, nrOfElements)
      case Result(value) =>
        pi += value
        nrOfResults += 1

        if (nrOfResults == nrOfMessages) {
          // Send the result to the listener
          listener ! PiApproximation(pi, duration = Duration(System.currentTimeMillis - start, "millis"))

          // Stop this actor and all its supervised children
          context.stop(self)
        }
    }
  }

  class Listener extends Actor {

    override def receive: Receive = {
      case PiApproximation(pi, duration) =>
        println("\n\tPi approximation: \t%s \n\tCalculation time: \t%s"
          .format(pi, duration))
        context.system.terminate()
    }
  }

  def calculate(nrOfWorkers: Int, nrOfElements: Int, nrOfMessages: Int): Unit = {
    // Create an Akka system
    val system = ActorSystem("PiSystem")

    // Create the result listener, which will print the result and shutdown the system
    val listener = system.actorOf(Props[Listener], name = "listener")

    // Create the master
    val master = system.actorOf(Props(new Master(nrOfWorkers, nrOfElements, nrOfMessages, listener)), name = "master")

    // Start the calculation
    master ! Calculate
  }

}
