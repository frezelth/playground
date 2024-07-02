package eu.altfive.playground;

import java.util.concurrent.LinkedBlockingQueue;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.springframework.stereotype.Component;

@Component
public class BrokerSimulator {

  private static final int NUMBER_OF_THREADS = 30;
  private LinkedBlockingQueue<Object>[] queues;
  private ConsumerThread[] consumers;
  private CommandGateway commandGateway;

  public BrokerSimulator(CommandGateway commandGateway){
    queues = new LinkedBlockingQueue[NUMBER_OF_THREADS];
    consumers = new ConsumerThread[NUMBER_OF_THREADS];

    for (int i = 0; i < NUMBER_OF_THREADS; i++) {
      queues[i] = new LinkedBlockingQueue<>();
      consumers[i] = new ConsumerThread(queues[i], commandGateway);
      consumers[i].start();
    }
  }

  public void sendCommand(String aggregate, Object command) {
//    int targetThreadId = command.getTargetThreadId();
    int targetThreadId = Math.abs(aggregate.hashCode()) % NUMBER_OF_THREADS;
    if (targetThreadId >= 0 && targetThreadId < queues.length) {
      try {
        queues[targetThreadId].put(command);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else {
      System.out.println("Invalid target thread ID: " + targetThreadId);
    }
  }

}
