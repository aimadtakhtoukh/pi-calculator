package example.pi

import akka.actor.typed.scaladsl.{Behaviors, Routers}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, SupervisorStrategy}

sealed trait PiMessage

final case class Work() extends PiMessage
final case class Step(currentMessage: Int, nbLoops: Int, replyTo: ActorRef[StepValue]) extends PiMessage
final case class StepValue(value: Int, result: Double) extends PiMessage

object Listener {
  def behavior(limit: Int, acc: Double = 0.0, messagesReceived: Int = 0): Behavior[StepValue] =
    Behaviors.receive { (context, message) =>
      if (messagesReceived < limit - 1) {
        behavior(limit, acc = acc + message.result, messagesReceived = messagesReceived + 1)
      } else {
        context.log.info(s"$acc")
        context.system.terminate()
        Behaviors.stopped
      }
    }
}

object Worker {

  def behavior() : Behavior[Step] =
    Behaviors.receive { (_, message) =>
      val result = calculatePiStep(message.currentMessage, message.nbLoops)
      message.replyTo ! StepValue(value = message.currentMessage, result = result)
      Behaviors.same
    }

  private def calculatePiStep(start: Int, iterations: Int) : Double = {
    var acc = 0.0
    for (i <- start until (start + iterations)) {
      val step  = i.toDouble
      acc += 4 * (1 - (step % 2) * 2) / (2 * step + 1)
    }
    acc
  }
}

object Starter {

  def behavior(nbWorkers : Int, nbIterations: Int, nbLoopsByMessage: Int) : Behavior[Work] =
    Behaviors.setup {context =>

      val pool = Routers.pool(nbWorkers) {
        Behaviors.supervise(Worker.behavior()).onFailure[Exception](SupervisorStrategy.restart)
      }.withRoundRobinRouting()

      val workers = context.spawn(pool, "worker-pool")
      val listener = context.spawn(Listener.behavior(nbIterations), "listener")

      Behaviors.receiveMessage { _ =>
        for (i <- 0  until nbIterations) {
          workers ! Step(i * nbLoopsByMessage, nbLoopsByMessage, listener)
        }
        Behaviors.same
      }
    }

}

object PiCalculator extends App {
  private val piMain: ActorSystem[Work] = ActorSystem(Starter.behavior(8, 10000, 10000), "PiCalculator")
  piMain ! Work()
}
