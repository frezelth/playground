package eu.altfive.playground;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.axonframework.commandhandling.gateway.CommandGateway;

public class ConsumerThread extends Thread {

  private final BlockingQueue<Object> queue;
  private final CommandGateway commandGateway;

  public ConsumerThread(BlockingQueue<Object> queue, CommandGateway commandGateway) {
    this.queue = queue;
    this.commandGateway = commandGateway;
  }

  @Override
  public void run() {
    try {
      while (true) {
        // Simulate consuming a message
        Object command = queue.poll(500, TimeUnit.MILLISECONDS); // Take a command from the queue
        if (command != null) {
          commandGateway.send(command);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.out.println(Thread.currentThread().getName() + " interrupted.");
    }
  }
}
