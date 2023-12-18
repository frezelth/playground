package eu.altfive.playground;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.axonframework.commandhandling.AsynchronousCommandBus;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandBusSpanFactory;
import org.axonframework.commandhandling.DuplicateCommandHandlerResolver;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.messaging.interceptors.CorrelationDataInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AxonConfig {

  @Bean
  public AsynchronousCommandBus commandBus(
      TransactionManager txManager, org.axonframework.config.Configuration axonConfiguration,
      DuplicateCommandHandlerResolver duplicateCommandHandlerResolver) {
    Executor executor = Executors.newFixedThreadPool(30);
    AsynchronousCommandBus commandBus =
        AsynchronousCommandBus.builder()
            .transactionManager(txManager)
            .duplicateCommandHandlerResolver(duplicateCommandHandlerResolver)
            .spanFactory(axonConfiguration.getComponent(CommandBusSpanFactory.class))
            .messageMonitor(axonConfiguration.messageMonitor(CommandBus.class, "commandBus"))
            .executor(executor)
            .build();
    commandBus.registerHandlerInterceptor(
        new CorrelationDataInterceptor<>(axonConfiguration.correlationDataProviders())
    );
    return commandBus;
  }

}
