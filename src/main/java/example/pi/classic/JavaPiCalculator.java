package example.pi.classic;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.FI;
import akka.routing.RoundRobinPool;

import java.time.Duration;

public class JavaPiCalculator {

    public interface PiMessage {}
    public record Calculate() implements PiMessage {}
    public record Work(int start, int elementCount) implements PiMessage {}
    public record Result(double value) implements PiMessage {}
    public record PiApproximation(double pi, Duration duration) implements PiMessage {}

    public static class Listener extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(PiApproximation.class, message -> {
                        System.out.printf(
                                "\n\tPi approximation: \t%s \n\tCalculation time: \t%s%n",
                                message.pi,
                                message.duration
                        );
                        getContext().getSystem().terminate();
                    })
                    .build();
        }
    }

    public static class Worker extends AbstractActor {
        private Double calculatePiStep(int start,int iterations) {
            double acc = 0.0;
            for (double step = start ; step < (start + iterations); step++) {
                acc += 4 * (1 - (step % 2) * 2) / (2 * step + 1);
            }
            return acc;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(
                            Work.class,
                            message ->
                                    getSender().tell(
                                            new Result(
                                                    calculatePiStep(
                                                            message.start,
                                                            message.elementCount
                                                    )
                                            ), self()
                                    )
                    )
                    .build();
        }
    }

    public static class Master extends AbstractActor {
        int workerCount;
        int messageCount;
        int elementCount;

        double pi;
        double resultCount;
        long startTime = System.currentTimeMillis();

        ActorRef listener;
        ActorRef workerRouter;

        public Master(int workerCount, int messageCount, int elementCount, ActorRef listener) {
            super();
            this.workerCount = workerCount;
            this.messageCount = messageCount;
            this.elementCount = elementCount;
            this.listener = listener;
            workerRouter = getContext().actorOf(
                    Props.create(Worker.class)
                            .withRouter(new RoundRobinPool(workerCount)), "workerRouter");
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Calculate.class, onCalculate())
                    .match(Result.class, onResult())
                    .build();
        }

        private FI.UnitApply<Calculate> onCalculate() {
            return message -> {
                for (int i = 0; i < messageCount; i++) {
                    workerRouter.tell(new Work(i * elementCount, elementCount), self());
                }
            };
        }

        private FI.UnitApply<Result> onResult() {
            return message -> {
                pi += message.value;
                resultCount++;
                if (resultCount == messageCount) {
                    listener.tell(new PiApproximation(pi, Duration.ofMillis(System.currentTimeMillis() - startTime)), self());
                    getContext().stop(self());
                }
            };
        }
    }

    public static void main(String[] args) {
        var system = ActorSystem.create("PiCalculator");
        var listener = system.actorOf(Props.create(Listener.class), "listener");
        var master = system.actorOf(
                Props.create(
                        Master.class,
                        () -> new Master(4, 10000, 10000, listener)
                ), "master");
        master.tell(new Calculate(), master);
    }

}
