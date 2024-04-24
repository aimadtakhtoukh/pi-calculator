package example.pi.typed;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.*;

public class JavaPiCalculator {

    public interface PiMessage {}
    public record Work() implements PiMessage {}
    public record Step(int currentMessage, int nbLoops, ActorRef<StepValue> replyTo) implements PiMessage {}
    public record StepValue(int value, double result) implements PiMessage {}

    public static class Listener extends AbstractBehavior<StepValue> {
        int limit;
        double acc = 0.0;
        int messageCount = 0;

        public Listener(ActorContext<StepValue> context, int limit) {
            super(context);
            this.limit = limit;
        }

        @Override
        public Receive<StepValue> createReceive() {
            return newReceiveBuilder()
                    .onMessage(StepValue.class,this::onStepValue)
                    .build();
        }

        private Behavior<StepValue> onStepValue(StepValue stepValue) {
            if (messageCount < limit - 1) {
                acc += stepValue.result;
                messageCount++;
                return Behaviors.same();
            } else {
                getContext().getLog().info("{}", acc);
                getContext().getSystem().terminate();
                return Behaviors.stopped();
            }
        }

        public static Behavior<StepValue> behavior(int limit) {
            return Behaviors.setup(context -> new Listener(context, limit));
        }
    }

    public static class Worker extends AbstractBehavior<Step> {
        private Double calculatePiStep(int start,int iterations) {
            double acc = 0.0;
            for (double step = start ; step < (start + iterations); step++) {
                acc += 4 * (1 - (step % 2) * 2) / (2 * step + 1);
            }
            return acc;
        }

        public Worker(ActorContext<Step> context) {
            super(context);
        }

        @Override
        public Receive<Step> createReceive() {
            return newReceiveBuilder()
                    .onMessage(Step.class, this::onStep)
                    .build();
        }

        private Behavior<Step> onStep(Step step) {
            step.replyTo.tell(
                    new StepValue(
                            step.currentMessage,
                            calculatePiStep(step.currentMessage, step.nbLoops)
                    )
            );
            return Behaviors.same();
        }


        public static Behavior<Step> behavior() {
            return Behaviors.setup(Worker::new);
        }
    }

    public static class Starter extends AbstractBehavior<Work> {
        int workersCount;
        int iterations;
        int iterationsPerMessage;

        ActorRef<Step> workers;
        ActorRef<StepValue> listener;

        public Starter(ActorContext<Work> context, int workersCount, int iterations, int iterationsPerMessage) {
            super(context);
            var workerPool =
                    Routers.pool(
                            workersCount,
                            Behaviors.supervise(Worker.behavior())
                                    .onFailure(SupervisorStrategy.restart())
                    ).withRoundRobinRouting();
            workers = getContext().spawn(workerPool, "worker-pool");
            listener = getContext().spawn(Listener.behavior(iterations), "listener");
            this.iterations = iterations;
            this.workersCount = workersCount;
            this.iterationsPerMessage = iterationsPerMessage;
        }

        @Override
        public Receive<Work> createReceive() {
            return newReceiveBuilder()
                    .onMessage(Work.class, this::onWork)
                    .build();
        }

        private Behavior<Work> onWork(Work work) {
            for (int i = 0; i < iterations; i++) {
                workers.tell(new Step(i * iterationsPerMessage, iterationsPerMessage, listener));
            }
            return Behaviors.same();
        }

        public static Behavior<Work> behavior(int workersCount, int iterations, int iterationsPerMessage) {
            return Behaviors.setup(
                    context -> new Starter(context, workersCount, iterations, iterationsPerMessage)
            );
        }
    }

    public static void main(String[] args) {
        ActorSystem<Work> system =
            ActorSystem.create(
                    Starter.behavior(8, 10000, 10000),
                    "PiCalculator"
            );
        system.tell(new Work());
    }

}
